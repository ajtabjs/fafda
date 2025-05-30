package config

import (
	"fmt"

	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
)

type GitHubRelease struct {
	ReadOnly   bool   `koanf:"readOnly"`
	Username   string `koanf:"username"`
	AuthToken  string `koanf:"authToken"`
	ReleaseId  int    `koanf:"releaseId"`
	ReleaseTag string `koanf:"releaseTag"`
	Repository string `koanf:"repository"`
}

type GitHub struct {
	PartSize    int64           `koanf:"partSize"`
	Concurrency int             `koanf:"concurrency"`
	Releases    []GitHubRelease `koanf:"releases"`
}

type FTPUser struct {
	Username string `koanf:"username"`
	Password string `koanf:"password"`
}

type FTPServer struct {
	Addr  string    `koanf:"addr"`
	Users []FTPUser `koanf:"users"`
}

type HTTPServer struct {
	Addr string `koanf:"addr"`
}

type Config struct {
	DBFile     string     `koanf:"dbFile"`
	GitHub     GitHub     `koanf:"github"`
	FTPServer  FTPServer  `koanf:"ftpServer"`
	HTTPServer HTTPServer `koanf:"httpServer"`
}

var k = koanf.New(".")
var defaultConfigPath = "config.yaml"

func New(configFile ...string) (*Config, error) {
	configFilePath := defaultConfigPath
	if len(configFile) > 0 {
		configFilePath = configFile[0]
	}

	if err := k.Load(file.Provider(configFilePath), yaml.Parser()); err != nil {
		return nil, fmt.Errorf("load config from path - path %s - %w", configFilePath, err)
	}

	var cfg Config
	if err := k.Unmarshal("", &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
