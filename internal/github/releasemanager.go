package github

import (
	"fmt"
	"sync"

	"fafda/config"
)

type ReleaseManager struct {
	releases     []config.GitHubRelease
	userTokens   map[string]string
	currentToken int
	currentRel   int
	currentPair  int
	mu           sync.Mutex
}

func NewReleaseManager(cfg config.GitHub) (*ReleaseManager, error) {
	rm := &ReleaseManager{
		userTokens:   map[string]string{},
		releases:     make([]config.GitHubRelease, 0),
		currentToken: -1,
		currentRel:   -1,
		currentPair:  -1,
	}

	for _, release := range cfg.Releases {
		if release.AuthToken == "" {
			return nil, fmt.Errorf("auth token missing for release %d", release.ReleaseId)
		}
		rm.userTokens[release.Username] = release.AuthToken
		if !release.ReadOnly {
			rm.releases = append(rm.releases, release)
		}
	}

	if len(rm.releases) == 0 {
		return nil, fmt.Errorf("no valid writable release found in config")
	}

	return rm, nil
}

func (rm *ReleaseManager) GetNextRelease() config.GitHubRelease {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.currentPair = (rm.currentPair + 1) % len(rm.releases)
	return rm.releases[rm.currentPair]
}

func (rm *ReleaseManager) GetUserToken(user string) string {
	return rm.userTokens[user]
}
