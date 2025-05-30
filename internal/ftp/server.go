package ftp

import (
	"crypto/tls"
	"errors"
	"io"
	"net/http"

	"github.com/fclairamb/ftpserverlib"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"

	"fafda/config"
)

const IPResolveURL = "https://ipinfo.io/ip"

var (
	ErrNoTLS                 = errors.New("TLS is not configured")
	ErrBadUserNameOrPassword = errors.New("bad username or password")
)

func Serv(cfg config.FTPServer, fs afero.Fs) error {
	logger := log.With().Str("component", "ftpserver").Logger()

	driver := &Driver{
		Fs:     fs,
		Debug:  true,
		Users:  cfg.Users,
		logger: logger,
		Settings: &ftpserver.Settings{
			ListenAddr:          cfg.Addr,
			DefaultTransferType: ftpserver.TransferTypeBinary,
			IdleTimeout:         86400, // 24 hour
		},
	}

	// Optionally resolve public IP
	driver.Settings.PublicIPResolver = func(context ftpserver.ClientContext) (string, error) {
		resp, err := http.Get(IPResolveURL)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		ip, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", err
		}
		return string(ip), nil
	}

	server := ftpserver.NewFtpServer(driver)
	logger.Info().Str("address", cfg.Addr).Msg("starting server")

	return server.ListenAndServe()
}

type Driver struct {
	Fs       afero.Fs
	Debug    bool
	Settings *ftpserver.Settings
	Users    []config.FTPUser
	logger   zerolog.Logger
}

func (d *Driver) ClientConnected(cc ftpserver.ClientContext) (string, error) {
	d.logger.Info().
		Str("address", cc.RemoteAddr().String()).
		Str("version", cc.GetClientVersion()).
		Uint32("sessionId", cc.ID()).
		Msg("client connected")
	return "Fafda FTP Server", nil
}

func (d *Driver) ClientDisconnected(cc ftpserver.ClientContext) {
	d.logger.Info().
		Str("address", cc.RemoteAddr().String()).
		Str("version", cc.GetClientVersion()).
		Uint32("sessionId", cc.ID()).
		Msg("client disconnected")
}

func (d *Driver) AuthUser(cc ftpserver.ClientContext, user, pass string) (ftpserver.ClientDriver, error) {
	for _, u := range d.Users {
		if u.Username == user && u.Password == pass {
			d.logger.Info().
				Str("address", cc.RemoteAddr().String()).
				Uint32("sessionId", cc.ID()).
				Str("user", user).
				Msg("authentication successful")
			return d.Fs, nil
		}
	}
	d.logger.Warn().
		Str("address", cc.RemoteAddr().String()).
		Uint32("session_id", cc.ID()).
		Str("user", user).
		Err(ErrBadUserNameOrPassword).
		Msg("authentication failed")
	return nil, ErrBadUserNameOrPassword
}

func (d *Driver) GetSettings() (*ftpserver.Settings, error) { return d.Settings, nil }

func (d *Driver) GetTLSConfig() (*tls.Config, error) { return nil, ErrNoTLS }
