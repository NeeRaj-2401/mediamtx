// Package playback contains the playback server.
package playback

import (
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/bluenviron/mediamtx/internal/auth"
	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/protocols/httpp"
	"github.com/bluenviron/mediamtx/internal/restrictnetwork"
	"github.com/gin-gonic/gin"
)

type serverAuthManager interface {
	Authenticate(req *auth.Request) error
}

// Server is the playback server.
type Server struct {
	Address        string
	Encryption     bool
	ServerKey      string
	ServerCert     string
	AllowOrigin    string
	TrustedProxies conf.IPNetworks
	ReadTimeout    conf.Duration
	PathConfs      map[string]*conf.Path
	AuthManager    serverAuthManager
	Parent         logger.Writer

	httpServer *httpp.Server
	mutex      sync.RWMutex

	pidStore   map[string][]int
	pidStoreMu sync.RWMutex
}

func (s *Server) addHLSPid(clientIP string, pid int) {
	s.pidStoreMu.Lock()
	defer s.pidStoreMu.Unlock()
	s.pidStore[clientIP] = append(s.pidStore[clientIP], pid)
}

func (s *Server) getAndClearHLSPids(clientIP string) []int {
	s.pidStoreMu.Lock()
	defer s.pidStoreMu.Unlock()
	pids := s.pidStore[clientIP]
	delete(s.pidStore, clientIP)
	return pids
}

func (s *Server) removeHLSPid(clientIP string, pid int) {
	s.pidStoreMu.Lock()
	defer s.pidStoreMu.Unlock()
	pids, ok := s.pidStore[clientIP]
	if !ok {
		return
	}
	for i, p := range pids {
		if p == pid {
			s.pidStore[clientIP] = append(pids[:i], pids[i+1:]...)
			break
		}
	}
	if len(s.pidStore[clientIP]) == 0 {
		delete(s.pidStore, clientIP)
	}
}

// Initialize initializes Server.
func (s *Server) Initialize() error {
	router := gin.New()
	router.SetTrustedProxies(s.TrustedProxies.ToTrustedProxies()) //nolint:errcheck

	router.Use(s.middlewareOrigin)

	router.GET("/list", s.onList)
	router.GET("/get", s.onGet)
	router.GET("/killHLS", s.onKillHls)
	router.DELETE("/hls", s.deleteHLSDir)

	network, address := restrictnetwork.Restrict("tcp", s.Address)

	s.httpServer = &httpp.Server{
		Network:     network,
		Address:     address,
		ReadTimeout: time.Duration(s.ReadTimeout),
		Encryption:  s.Encryption,
		ServerCert:  s.ServerCert,
		ServerKey:   s.ServerKey,
		Handler:     router,
		Parent:      s,
	}
	s.pidStore = make(map[string][]int)
	err := s.httpServer.Initialize()
	if err != nil {
		return err
	}

	// starting cleaup service
	go s.cleanupOldHLSDirectories()

	s.Log(logger.Info, "listener opened on "+address)

	return nil
}

// Close closes Server.
func (s *Server) Close() {
	s.Log(logger.Info, "listener is closing")
	s.httpServer.Close()
}

// Log implements logger.Writer.
func (s *Server) Log(level logger.Level, format string, args ...interface{}) {
	s.Parent.Log(level, "[playback] "+format, args...)
}

// ReloadPathConfs is called by core.Core.
func (s *Server) ReloadPathConfs(pathConfs map[string]*conf.Path) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.PathConfs = pathConfs
}

func (s *Server) writeError(ctx *gin.Context, status int, err error) {
	// show error in logs
	s.Log(logger.Error, err.Error())

	// add error to response
	ctx.String(status, err.Error())
}

func (s *Server) safeFindPathConf(name string) (*conf.Path, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	pathConf, _, err := conf.FindPathConf(s.PathConfs, name)
	return pathConf, err
}

func (s *Server) middlewareOrigin(ctx *gin.Context) {
	ctx.Header("Access-Control-Allow-Origin", s.AllowOrigin)
	ctx.Header("Access-Control-Allow-Credentials", "true")

	// preflight requests
	if ctx.Request.Method == http.MethodOptions &&
		ctx.Request.Header.Get("Access-Control-Request-Method") != "" {
		ctx.Header("Access-Control-Allow-Methods", "OPTIONS, GET")
		ctx.Header("Access-Control-Allow-Headers", "Authorization")
		ctx.AbortWithStatus(http.StatusNoContent)
		return
	}
}

func (s *Server) doAuth(ctx *gin.Context, pathName string) bool {
	req := &auth.Request{
		Action:      conf.AuthActionPlayback,
		Path:        pathName,
		Query:       ctx.Request.URL.RawQuery,
		Credentials: httpp.Credentials(ctx.Request),
		IP:          net.ParseIP(ctx.ClientIP()),
	}

	err := s.AuthManager.Authenticate(req)
	if err != nil {
		if err.(auth.Error).AskCredentials { //nolint:errorlint
			ctx.Header("WWW-Authenticate", `Basic realm="mediamtx"`)
			ctx.Writer.WriteHeader(http.StatusUnauthorized)
			return false
		}

		s.Log(logger.Info, "connection %v failed to authenticate: %v",
			httpp.RemoteAddr(ctx), err.(*auth.Error).Message) //nolint:errorlint

		// wait some seconds to mitigate brute force attacks
		<-time.After(auth.PauseAfterError)

		ctx.Writer.WriteHeader(http.StatusUnauthorized)
		return false
	}

	return true
}
