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

	format := ctx.Query("format")
	if format == "hls" {
		s.handleHLS(ctx, pathName, start, duration)
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

// handleHLS handles HLS playback requests in a refactored manner.
func (s *Server) handleHLS(ctx *gin.Context, pathName string, start time.Time, duration time.Duration) {
	// Use a local HLS directory in the current working directory.
	token := computeToken(pathName, start, duration)
	hlsDir := filepath.Join(".", "mediamtx_hls", token)

	// If "file" parameter is present, serve the HLS segment.
	if segFile := ctx.Query("file"); segFile != "" {
		ctx.File(filepath.Join(hlsDir, segFile))
		return
	}

	// Ensure HLS directory exists.
	if err := os.MkdirAll(hlsDir, 0755); err != nil {
		s.writeError(ctx, http.StatusInternalServerError, fmt.Errorf("failed to create HLS directory: %w", err))
		return
	}

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

	// Create a file list for ffmpeg's concat demuxer.
	fileListPath := filepath.Join(hlsDir, "filelist.txt")
	fileList, err := os.Create(fileListPath)
	if err != nil {
		s.writeError(ctx, http.StatusInternalServerError, fmt.Errorf("failed to create file list: %w", err))
		return
	}
	defer fileList.Close()
	for _, seg := range segments {
		absPath, err := filepath.Abs(seg.Fpath)
		if err != nil {
			s.writeError(ctx, http.StatusInternalServerError, fmt.Errorf("failed to get absolute path: %w", err))
			return
		}
		if _, err = fileList.WriteString(fmt.Sprintf("file '%s'\n", absPath)); err != nil {
			s.writeError(ctx, http.StatusInternalServerError, fmt.Errorf("failed to write file list: %w", err))
			return
		}
	}

	// Calculate the start offset based on the first segment's start time.
	offset := 0.0
	if len(segments) > 0 {
		offsetDuration := start.Sub(segments[0].Start)
		if offsetDuration < 0 {
			offsetDuration = 0
		}
		offset = offsetDuration.Seconds()
	}

	// Build and run ffmpeg command to generate the HLS package.
	hlsPlaylist := filepath.Join(hlsDir, "index.m3u8")
	cmdArgs := []string{
		"-y",
		"-f", "concat",
		"-safe", "0",
		"-i", fileListPath,
		"-ss", fmt.Sprintf("%.3f", offset),
		"-t", fmt.Sprintf("%.3f", duration.Seconds()),
		"-c", "copy",
		"-f", "hls",
		"-hls_time", "10",
		"-hls_list_size", "0",
		"-hls_base_url", "",
		hlsPlaylist,
	}
	if err = exec.Command("ffmpeg", cmdArgs...).Run(); err != nil {
		s.writeError(ctx, http.StatusInternalServerError, fmt.Errorf("ffmpeg failed: %w", err))
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
