package http

import (
	"net/http"

	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"

	"fafda/config"
)

func Serv(cfg config.HTTPServer, fs afero.Fs) error {
	httpFs := afero.NewHttpFs(fs)
	fileServer := http.FileServer(httpFs.Dir("/"))
	http.Handle("/", fileServer)
	log.Info().
		Str("component", "httpserver").
		Str("address", cfg.Addr).
		Msg("starting server")
	return http.ListenAndServe(cfg.Addr, nil)
}
