package playback

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/gin-gonic/gin"
)

func (s *Server) cleanupDirectories(root string, deleteAll bool) {
	entries, err := os.ReadDir(root)
	if err != nil {
		s.Log(logger.Error, "Failed to read dir "+root+": "+err.Error())
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			fullPath := filepath.Join(root, entry.Name())

			info, statErr := entry.Info()
			if statErr != nil {
				s.Log(logger.Warn, "Failed to stat "+fullPath+": "+statErr.Error())
				continue
			}

			if deleteAll || time.Since(info.ModTime()) > time.Hour {
				if removeErr := os.RemoveAll(fullPath); removeErr != nil {
					s.Log(logger.Error, fmt.Sprintf("Failed to remove %s: %v", fullPath, removeErr))
				}
				s.Log(logger.Info, "Removed HLS dir: "+fullPath)
			}
		}
	}

	s.Log(logger.Info, "cleanupOldHLSDirectories Successful.")
}

// Initial cealup & Background task to cleanup HLS directories older than 1 hour
func (s *Server) cleanupOldHLSDirectories() {
	s.Log(logger.Info, "Staring hls dir cleaup service.")
	hlsRoot := filepath.Join(".", "mediamtx_hls")

	// initial cleaup
	s.cleanupDirectories(hlsRoot, true)

	// periodic cleanup
	for {
		time.Sleep(1 * time.Hour)
		s.cleanupDirectories(hlsRoot, false)
	}
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
	token := computeToken(ctx.ClientIP(), pathName, start, duration)
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
