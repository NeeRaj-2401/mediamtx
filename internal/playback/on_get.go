package playback

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/bluenviron/mediacommon/v2/pkg/formats/fmp4"
	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/recordstore"
	"github.com/gin-gonic/gin"
)

type writerWrapper struct {
	ctx     *gin.Context
	written bool
}

func (w *writerWrapper) Write(p []byte) (int, error) {
	if !w.written {
		w.written = true
		w.ctx.Header("Accept-Ranges", "none")
		w.ctx.Header("Content-Type", "video/mp4")
	}
	return w.ctx.Writer.Write(p)
}

func parseDuration(raw string) (time.Duration, error) {
	// seconds
	if secs, err := strconv.ParseFloat(raw, 64); err == nil {
		return time.Duration(secs * float64(time.Second)), nil
	}

	// deprecated, golang format
	return time.ParseDuration(raw)
}

func computeToken(pathName string, start time.Time, duration time.Duration) string {
	h := sha256.New()
	h.Write([]byte(pathName))
	h.Write([]byte(start.Format(time.RFC3339)))
	h.Write([]byte(duration.String()))
	return hex.EncodeToString(h.Sum(nil))
}

func seekAndMux(
	recordFormat conf.RecordFormat,
	segments []*recordstore.Segment,
	start time.Time,
	duration time.Duration,
	m muxer,
) error {
	if recordFormat == conf.RecordFormatFMP4 {
		var firstInit *fmp4.Init
		var segmentEnd time.Time

		f, err := os.Open(segments[0].Fpath)
		if err != nil {
			return err
		}
		defer f.Close()

		firstInit, _, err = segmentFMP4ReadHeader(f)
		if err != nil {
			return err
		}

		m.writeInit(firstInit)

		segmentStartOffset := start.Sub(segments[0].Start)

		segmentDuration, err := segmentFMP4SeekAndMuxParts(f, segmentStartOffset, duration, firstInit, m)
		if err != nil {
			return err
		}

		segmentEnd = start.Add(segmentDuration)

		for _, seg := range segments[1:] {
			f, err = os.Open(seg.Fpath)
			if err != nil {
				return err
			}
			defer f.Close()

			var init *fmp4.Init
			init, _, err = segmentFMP4ReadHeader(f)
			if err != nil {
				return err
			}

			if !segmentFMP4CanBeConcatenated(firstInit, segmentEnd, init, seg.Start) {
				break
			}

			segmentStartOffset := seg.Start.Sub(start)

			var segmentDuration time.Duration
			segmentDuration, err = segmentFMP4MuxParts(f, segmentStartOffset, duration, firstInit, m)
			if err != nil {
				return err
			}

			segmentEnd = start.Add(segmentDuration)
		}

		err = m.flush()
		if err != nil {
			return err
		}

		return nil
	}

	return fmt.Errorf("MPEG-TS format is not supported yet")
}

func (s *Server) onGet(ctx *gin.Context) {
	pathName := ctx.Query("path")
	if !s.doAuth(ctx, pathName) {
		return
	}

	start, err := time.Parse(time.RFC3339, ctx.Query("start"))
	if err != nil {
		s.writeError(ctx, http.StatusBadRequest, fmt.Errorf("invalid start: %w", err))
		return
	}
	duration, err := parseDuration(ctx.Query("duration"))
	if err != nil {
		s.writeError(ctx, http.StatusBadRequest, fmt.Errorf("invalid duration: %w", err))
		return
	}

	// Retrieve the recording configuration and segments.
	pathConf, err := s.safeFindPathConf(pathName)
	if err != nil {
		s.writeError(ctx, http.StatusBadRequest, err)
		return
	}

	end := start.Add(duration)
	segments, err := recordstore.FindSegments(pathConf, pathName, &start, &end)
	if err != nil {
		if errors.Is(err, recordstore.ErrNoSegmentsFound) {
			s.writeError(ctx, http.StatusNotFound, err)
		} else {
			s.writeError(ctx, http.StatusBadRequest, err)
		}
		return
	}

	format := ctx.Query("format")
	if format == "hls" {
		s.handleHLS(ctx, pathName, start, duration, pathConf, segments)
		return
	}

	// For fMP4 and MP4, set up the appropriate muxer.
	ww := &writerWrapper{ctx: ctx}
	var m muxer
	switch format {
	case "", "fmp4":
		m = &muxerFMP4{w: ww}

	case "mp4":
		m = &muxerMP4{w: ww}

	default:
		s.writeError(ctx, http.StatusBadRequest, fmt.Errorf("invalid format: %s", format))
		return
	}

	err = seekAndMux(pathConf.RecordFormat, segments, start, duration, m)
	if err != nil {
		// user aborted the download
		var neterr *net.OpError
		if errors.As(err, &neterr) {
			return
		}

		// nothing has been written yet; send back JSON
		if !ww.written {
			if errors.Is(err, recordstore.ErrNoSegmentsFound) {
				s.writeError(ctx, http.StatusNotFound, err)
			} else {
				s.writeError(ctx, http.StatusBadRequest, err)
			}
			return
		}

		// something has already been written: abort and write logs only
		s.Log(logger.Error, err.Error())
		return
	}
}

// handles HLS playback flow.
func (s *Server) handleHLS(ctx *gin.Context, pathName string, start time.Time, duration time.Duration, pathConf *conf.Path, segments []*recordstore.Segment) {
	// Use a local HLS directory in the current working directory to keep the processed hls playlists.
	token := computeToken(pathName, start, duration)
	hlsDir := filepath.Join(".", "mediamtx_hls", token)

	// If "file" parameter is present, serve the HLS segment directly.
	if segFile := ctx.Query("file"); segFile != "" {
		ctx.File(filepath.Join(hlsDir, segFile))
		return
	}

	// Ensure HLS directory exists.
	if err := os.MkdirAll(hlsDir, 0755); err != nil {
		s.writeError(ctx, http.StatusInternalServerError, fmt.Errorf("failed to create HLS directory: %w", err))
		return
	}

	// Create a trimmed MP4 file using the same logic (seekAndMux) as the non-HLS (MP4) flow. (2 min of segment out of 2 hr video)
	trimmedFilePath := filepath.Join(hlsDir, "trimmed.mp4")
	f, err := os.Create(trimmedFilePath)
	if err != nil {
		s.writeError(ctx, http.StatusInternalServerError, fmt.Errorf("failed to create trimmed file: %w", err))
		return
	}

	// Use muxerFMP4 (which writes to an io.Writer) to generate the trimmed clip.
	m := &muxerFMP4{w: f}
	err = seekAndMux(pathConf.RecordFormat, segments, start, duration, m)
	f.Close()
	if err != nil {
		s.writeError(ctx, http.StatusInternalServerError, fmt.Errorf("muxing failed: %w", err))
		return
	}

	// Use ffmpeg to convert the trimmed MP4 clip to HLS.
	hlsPlaylist := filepath.Join(hlsDir, "index.m3u8")
	ffmpegArgs := []string{
		"-y",
		"-i", trimmedFilePath,
		"-c", "copy",
		"-f", "hls",
		"-hls_time", "10",
		"-hls_list_size", "0",
		"-hls_base_url", "",
		hlsPlaylist,
	}
	if err = exec.Command("ffmpeg", ffmpegArgs...).Run(); err != nil {
		s.writeError(ctx, http.StatusInternalServerError, fmt.Errorf("ffmpeg conversion failed: %w", err))
		return
	}

	playlistBytes, err := os.ReadFile(hlsPlaylist)
	if err != nil {
		s.writeError(ctx, http.StatusInternalServerError, fmt.Errorf("failed to read playlist: %w", err))
		return
	}
	playlistContent := string(playlistBytes)
	if !strings.HasPrefix(playlistContent, "#EXTM3U") {
		s.writeError(ctx, http.StatusInternalServerError, fmt.Errorf("invalid playlist generated"))
		return
	}

	// Rewrite playlist URLs to point back to this endpoint with the "file" query parameter.
	baseURL := *ctx.Request.URL
	q := baseURL.Query()
	q.Del("file")
	baseURL.RawQuery = q.Encode()
	baseURLStr := baseURL.String()

	var rewrittenLines []string
	for _, line := range strings.Split(playlistContent, "\n") {
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			rewrittenLines = append(rewrittenLines, line)
		} else {
			segURL := fmt.Sprintf("%s&file=%s", baseURLStr, url.QueryEscape(line))
			rewrittenLines = append(rewrittenLines, segURL)
		}
	}
	ctx.Header("Content-Type", "application/vnd.apple.mpegurl")
	ctx.String(http.StatusOK, strings.Join(rewrittenLines, "\n"))
}

func (s *Server) deleteHLSDir(ctx *gin.Context) {
	pathName := ctx.Query("path")
	startStr := ctx.Query("start")
	durationStr := ctx.Query("duration")

	if pathName == "" || startStr == "" || durationStr == "" {
		s.writeError(ctx, http.StatusBadRequest, fmt.Errorf("missing required query parameter"))
		return
	}

	// Parse start time.
	start, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		s.writeError(ctx, http.StatusBadRequest, fmt.Errorf("invalid start: %w", err))
		return
	}

	// Parse duration.
	duration, err := parseDuration(durationStr)
	if err != nil {
		s.writeError(ctx, http.StatusBadRequest, fmt.Errorf("invalid duration: %w", err))
		return
	}

	// Compute token and HLS directory path.
	token := computeToken(pathName, start, duration)
	hlsDir := filepath.Join(".", "mediamtx_hls", token)

	// Check if the directory exists.
	if _, err := os.Stat(hlsDir); os.IsNotExist(err) {
		s.writeError(ctx, http.StatusNotFound, fmt.Errorf("HLS directory does not exist"))
		return
	}

	// Delete the directory.
	if err := os.RemoveAll(hlsDir); err != nil {
		s.writeError(ctx, http.StatusInternalServerError, fmt.Errorf("failed to delete HLS directory: %w", err))
		return
	}

	ctx.String(http.StatusOK, "HLS directory deleted")
}
