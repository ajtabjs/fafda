package ftp

import (
	"crypto/tls"
	"errors"
	"fmt"
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
		Fs:       fs,
		Debug:    true,
		username: cfg.Username,
		password: cfg.Password,
		Settings: &ftpserver.Settings{
			ListenAddr:          cfg.Addr,
			DefaultTransferType: ftpserver.TransferTypeBinary,
			// Stooopid FTP thinks connection is idle, even when file transfer is going on.
			// Default is 900 seconds after which the server will drop the connection
			// Increased it to 24 hours to allow big file transfers
			IdleTimeout: 86400, // 24 hour
		},
		logger: logger,
	}

	if cfg.PortRange != nil {
		portRange := &ftpserver.PortRange{}

		if cfg.PortRange.Start < 1 || cfg.PortRange.Start > 65535 {
			return fmt.Errorf("invalid start port: must be between 1-65535, got %d", cfg.PortRange.Start)
		}
		if cfg.PortRange.End < 1 || cfg.PortRange.End > 65535 {
			return fmt.Errorf("invalid end port: must be between 1-65535, got %d", cfg.PortRange.End)
		}

		if cfg.PortRange.Start >= cfg.PortRange.End {
			return fmt.Errorf("start port (%d) must be less than end port (%d)",
				cfg.PortRange.Start, cfg.PortRange.End)
		}

		portRange.Start = cfg.PortRange.Start
		portRange.End = cfg.PortRange.End

		driver.Settings.PassiveTransferPortRange = portRange
		driver.Settings.PublicIPResolver = func(context ftpserver.ClientContext) (string, error) {
			resp, err := http.Get(IPResolveURL)
			if err != nil {
				return "", err
			}
			ip, err := io.ReadAll(resp.Body)
			if err != nil {
				return "", err
			}
			return string(ip), nil
		}
	}

	server := ftpserver.NewFtpServer(driver)
	logger.Info().Str("address", cfg.Addr).Msg("starting server")

	return server.ListenAndServe()
}

type Driver struct {
	Fs       afero.Fs
	Debug    bool
	Settings *ftpserver.Settings
	username string
	password string
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
	if d.username != "" && d.username != user || d.password != "" && d.password != pass {
		d.logger.Warn().
			Str("address", cc.RemoteAddr().String()).
			Uint32("session_id", cc.ID()).
			Str("user", user).
			Err(ErrBadUserNameOrPassword).
			Msg("authentication failed")
		return nil, ErrBadUserNameOrPassword
	}
	d.logger.Info().
		Str("address", cc.RemoteAddr().String()).
		Uint32("sessionId", cc.ID()).
		Str("user", user).
		Msg("authentication successful")
	return d.Fs, nil
}

func (d *Driver) GetSettings() (*ftpserver.Settings, error) { return d.Settings, nil }

func (d *Driver) GetTLSConfig() (*tls.Config, error) { return nil, ErrNoTLS }
