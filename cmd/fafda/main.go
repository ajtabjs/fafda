package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	zl "github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.etcd.io/bbolt"

	"fafda/config"
	"fafda/internal"
	"fafda/internal/bolt"
	"fafda/internal/filesystem"
	"fafda/internal/ftp"
	"fafda/internal/github"
	"fafda/internal/http"
)

const name = "fafda"

var (
	debugMode    = flag.Bool("debug", false, "enable debug logs")
	showVersion  = flag.Bool("version", false, "print version information and exit")
	configFile   = flag.String("config", "", "path to nefarious configuration file")
	listReleases = flag.String("list-releases", "", "comma-separated list of GitHub tokens to fetch releases information")
)

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("%s: %s\n", name, internal.Version())
		os.Exit(0)
	}

	if *listReleases != "" {
		tokens := strings.Split(*listReleases, ",")
		github.ListReleases(tokens)
		os.Exit(0)
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	log.Logger = zl.New(zl.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	zl.SetGlobalLevel(zl.InfoLevel)
	if *debugMode {
		zl.SetGlobalLevel(zl.DebugLevel)
	}

	var err error
	var cfg *config.Config
	if *configFile != "" {
		cfg, err = config.New(*configFile)
	} else {
		cfg, err = config.New()
	}
	if err != nil {
		log.Fatal().Err(err).Msgf("failed to load config")
	}

	dbFile := cfg.DBFile
	if dbFile == "" {
		dbFile = name + ".db"
	}

	db, err := bbolt.Open(dbFile, 0600, nil)
	if err != nil {
		log.Fatal().Err(err).Msgf("failed to open bolt")
	}

	metafs, err := bolt.NewMetaFs(db)
	if err != nil {
		log.Fatal().Err(err).Msgf("failed to open bolt data provider")
	}

	driver, err := github.NewDriver(cfg.GitHub, db)
	if err != nil {
		log.Fatal().Err(err).Msgf("failed to load github driver")
	}

	fs := filesystem.New(driver, metafs)

	if cfg.HTTPServer.Addr != "" {
		go func() {
			if err := http.Serv(cfg.HTTPServer, fs); err != nil {
				log.Fatal().Err(err).Msgf("failed to start http server")
			}
		}()
	}

	if err := ftp.Serv(cfg.FTPServer, fs); err != nil {
		log.Fatal().Err(err).Msgf("failed to start ftp server")
	}
}
