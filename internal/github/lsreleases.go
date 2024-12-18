package github

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type Repository struct {
	Name     string `json:"name"`
	FullName string `json:"full_name"`
	Owner    struct {
		Login string `json:"login"`
	} `json:"owner"`
}

type Release struct {
	Id      int64  `json:"id"`
	TagName string `json:"tag_name"`
}

type ReleaseInfo struct {
	AuthToken  string `json:"authToken"`
	Username   string `json:"username"`
	Repository string `json:"repository"`
	ReleaseId  int64  `json:"releaseId"`
	ReleaseTag string `json:"releaseTag"`
}

func fetchGitHubAPI(token, url string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github.v3+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API request failed with status: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %v", err)
	}

	return body, nil
}

func getRepositories(token string) ([]Repository, error) {
	body, err := fetchGitHubAPI(token, "https://api.github.com/user/repos?per_page=100")
	if err != nil {
		return nil, err
	}

	var repos []Repository
	if err := json.Unmarshal(body, &repos); err != nil {
		return nil, fmt.Errorf("error parsing repositories: %v", err)
	}

	return repos, nil
}

func getReleases(token, repoFullName string) ([]Release, error) {
	url := fmt.Sprintf("https://api.github.com/repos/%s/releases", repoFullName)
	body, err := fetchGitHubAPI(token, url)
	if err != nil {
		return nil, err
	}

	var releases []Release
	if err := json.Unmarshal(body, &releases); err != nil {
		return nil, fmt.Errorf("error parsing releases: %v", err)
	}

	return releases, nil
}

func GetAllReleasesInfo(tokens []string) ([]ReleaseInfo, error) {
	var allReleases []ReleaseInfo

	for _, token := range tokens {
		repos, err := getRepositories(token)
		if err != nil {
			fmt.Printf("Warning: error fetching repositories for token: %v\n", err)
			continue
		}

		for _, repo := range repos {
			releases, err := getReleases(token, repo.FullName)
			if err != nil {
				fmt.Printf("Warning: error fetching releases for %s: %v\n", repo.FullName, err)
				continue
			}

			for _, release := range releases {
				releaseInfo := ReleaseInfo{
					Username:   strings.ToLower(repo.Owner.Login),
					Repository: repo.Name,
					ReleaseId:  release.Id,
					ReleaseTag: release.TagName,
					AuthToken:  token,
				}
				allReleases = append(allReleases, releaseInfo)
			}
		}
	}

	return allReleases, nil
}

func ListReleases(tokens []string) {
	releases, err := GetAllReleasesInfo(tokens)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	output, err := json.MarshalIndent(releases, "", "  ")
	if err != nil {
		fmt.Printf("Error marshaling to JSON: %v\n", err)
		return
	}

	fmt.Println(string(output))
}
