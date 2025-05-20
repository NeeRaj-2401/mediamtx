package playback

import (
	"context"
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

func computeToken(clientID string, pathName string, start time.Time, duration time.Duration) string {
	h := sha256.New()
	h.Write([]byte(clientID))
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

	startStr := ctx.Request.URL.Query().Get("start")
	start, err := time.Parse(time.RFC3339, startStr)
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

var ffmpegSem = make(chan struct{}, 16) // based on CPU cores

// handles HLS playback flow.
func (s *Server) handleHLS(ctx *gin.Context, pathName string, start time.Time, duration time.Duration, pathConf *conf.Path, segments []*recordstore.Segment) {
	clientIP := ctx.ClientIP()
	token := computeToken(clientIP, pathName, start, duration)
	hlsDir := filepath.Join(".", "mediamtx_hls", token)
	hlsPlaylist := filepath.Join(hlsDir, "index.m3u8")

	if segFile := ctx.Query("file"); segFile != "" {
		ctx.File(filepath.Join(hlsDir, segFile))
		return
	}

	s.Log(logger.Info, fmt.Sprintf("HLS request - path: %s, start: %s, duration: %s", pathName, start.Format(time.RFC3339), duration))
	// Check for existing process and wait if present
	s.activeHLSLock.Lock()
	clientMap, exists := s.activeHLSTokens[clientIP]
	if !exists {
		clientMap = make(map[string]*HLSProcessInfo)
		s.activeHLSTokens[clientIP] = clientMap
	}
	processInfo, exists := clientMap[token]
	if exists {
		s.activeHLSLock.Unlock()
		select {
		case <-processInfo.doneChan:
			// // Existing process completed, check for segment file or serve playlist
			// if segFile := ctx.Query("file"); segFile != "" {
			// 	ctx.File(filepath.Join(hlsDir, segFile))
			// 	return
			// }
			playlistBytes, err := os.ReadFile(hlsPlaylist)
			if err != nil {
				s.writeError(ctx, http.StatusInternalServerError, fmt.Errorf("failed to read playlist: %w", err))
				return
			}
			servePlaylist(ctx, playlistBytes)
			return
		case <-ctx.Request.Context().Done():
			return
		}
	} else {
		// No existing process, create new entry
		processInfo = &HLSProcessInfo{
			doneChan: make(chan struct{}),
		}
		clientMap[token] = processInfo
		s.activeHLSLock.Unlock()
	}

	if err := os.MkdirAll(hlsDir, 0755); err != nil {
		s.writeError(ctx, http.StatusInternalServerError, fmt.Errorf("failed to create HLS directory: %w", err))
		return
	}

	var ffmpegArgs []string
	if pathConf.RecordFormat == conf.RecordFormatMPEGTS {
		listPath := filepath.Join(hlsDir, "list.txt")
		listFile, lerr := os.Create(listPath)
		if lerr != nil {
			s.writeError(ctx, http.StatusInternalServerError, fmt.Errorf("failed to create list file: %w", lerr))
			return
		}
		defer listFile.Close()

		for _, seg := range segments {
			fmt.Fprintf(listFile, "file '%s'\n", seg.Fpath)
		}

		startOffset := start.Sub(segments[0].Start)
		ffmpegArgs = []string{
			"-y",
			"-hwaccel", "auto",
			"-threads", "4",
			"-f", "concat",
			"-safe", "0",
			"-i", listPath,
			"-ss", fmt.Sprintf("%.2f", startOffset.Seconds()),
			"-t", fmt.Sprintf("%.2f", duration.Seconds()),
			"-c", "copy",
			"-f", "hls",
			"-hls_time", "10",
			"-hls_list_size", "0",
			hlsPlaylist,
		}
	} else {
		trimmedFilePath := filepath.Join(hlsDir, "trimmed.mp4")
		f, ferr := os.Create(trimmedFilePath)
		if ferr != nil {
			s.writeError(ctx, http.StatusInternalServerError, fmt.Errorf("failed to create trimmed file: %w", ferr))
			return
		}
		m := &muxerFMP4{w: f}
		if muxErr := seekAndMux(pathConf.RecordFormat, segments, start, duration, m); muxErr != nil {
			f.Close()
			s.writeError(ctx, http.StatusInternalServerError, fmt.Errorf("muxing failed: %w", muxErr))
			return
		}
		f.Close()

		ffmpegArgs = []string{
			"-y",
			"-i", trimmedFilePath,
			"-c", "copy",
			"-f", "hls",
			"-hls_time", "10",
			"-hls_list_size", "0",
			"-hls_base_url", "",
			hlsPlaylist,
		}
	}

	errChan := make(chan error, 1)
	cmdCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	go func() {
		// TODO: remove after test
		ffmpegSem <- struct{}{}        // Acquire a slot
		defer func() { <-ffmpegSem }() // Release the slot

		startTime := time.Now()
		cmd := exec.CommandContext(cmdCtx, "ffmpeg", ffmpegArgs...)
		if err := cmd.Start(); err != nil {
			errChan <- fmt.Errorf("ffmpeg start failed: %w", err)
			return
		}

		// Track PID
		s.activeHLSLock.Lock()
		processInfo.pid = cmd.Process.Pid
		s.activeHLSLock.Unlock()

		err := cmd.Wait()

		// logger
		executionTime := time.Since(startTime)
		s.Log(logger.Info, fmt.Sprintf("ffmpeg completed for token %s, took %s", token, executionTime))

		// Cleanup
		s.activeHLSLock.Lock()
		if cm, exists := s.activeHLSTokens[clientIP]; exists {
			delete(cm, token)
			if len(cm) == 0 {
				delete(s.activeHLSTokens, clientIP)
			}
		}
		s.activeHLSLock.Unlock()
		close(processInfo.doneChan)
		errChan <- err
	}()

	if err := <-errChan; err != nil {
		s.writeError(ctx, http.StatusInternalServerError, fmt.Errorf("ffmpeg processing failed: %w", err))
		return
	}

	playlistBytes, err := os.ReadFile(hlsPlaylist)
	if err != nil {
		s.writeError(ctx, http.StatusInternalServerError, fmt.Errorf("failed to read playlist: %w", err))
		return
	}
	servePlaylist(ctx, playlistBytes)
}

// servePlaylist rewrites the playlist URLs and sends the response.
func servePlaylist(ctx *gin.Context, playlistBytes []byte) {
	playlistContent := string(playlistBytes)
	if !strings.HasPrefix(playlistContent, "#EXTM3U") {
		ctx.String(http.StatusInternalServerError, "invalid playlist generated")
		return
	}

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

func (s *Server) onKillHls(ctx *gin.Context) {
}
