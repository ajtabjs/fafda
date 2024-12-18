package github

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"fafda/config"
	"fafda/internal"
)

const headerRateLimitRetryAfter = "retry-after"
const headerRateLimitReset = "x-ratelimit-remaining"
const headerRateLimitRemaining = "x-ratelimit-reset"

type Client struct {
	partSize  int
	client    *http.Client
	resources *ReleaseManager
}

func NewClient(cfg config.GitHub) (*Client, error) {
	resources, err := NewReleaseManager(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize resource manager: %w", err)
	}

	return &Client{
		client:    &http.Client{},
		resources: resources,
	}, nil
}

func handleRateLimit(resp *http.Response) time.Duration {
	if resp.StatusCode != http.StatusForbidden &&
		resp.StatusCode != http.StatusTooManyRequests {
		return 0
	}

	if retryAfter := resp.Header.Get(headerRateLimitRetryAfter); retryAfter != "" {
		seconds, _ := strconv.ParseInt(retryAfter, 10, 64)
		return time.Duration(seconds) * time.Second
	}

	if resp.Header.Get(headerRateLimitRemaining) == "0" {
		resetTime, _ := strconv.ParseInt(resp.Header.Get(headerRateLimitReset), 10, 64)
		waitTime := resetTime - time.Now().UTC().Unix()
		if waitTime > 0 {
			return time.Duration(waitTime) * time.Second
		}
	}

	// Should never come to this - FUCK YOU GITHUB
	return time.Minute
}

func (c *Client) doRequest(req *http.Request) (*http.Response, error) {
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	if waitTime := handleRateLimit(resp); waitTime > 0 {
		time.Sleep(waitTime)
		return c.doRequest(req)
	}

	return resp, nil
}

func (c *Client) UploadAsset(filename string, size int64, b []byte) (*Asset, error) {
	release := c.resources.GetNextRelease()
	url := fmt.Sprintf(
		"%s/repos/%s/%s/releases/%d/assets",
		uploadURL, release.Username, release.Repository, release.ReleaseId,
	)

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s?name=%s", url, filename), bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set(internal.HeaderAccept, internal.MediaTypeGithubJSON)
	req.Header.Set(internal.HeaderContentType, internal.MediaTypeOctetStream)
	req.Header.Set(internal.HeaderAuthorization, "Bearer "+release.AuthToken)
	req.ContentLength = size

	resp, err := c.doRequest(req)
	if err != nil {
		return nil, fmt.Errorf("upload asset: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("upload asset failed: %s", string(body))
	}

	var asset Asset
	if err := json.NewDecoder(resp.Body).Decode(&asset); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	asset.Name = filename
	asset.Username = release.Username
	asset.Repository = release.Repository
	return &asset, nil
}

func (c *Client) DownloadAsset(asset *Asset, start, end int) (io.ReadCloser, error) {
	token := c.resources.GetUserToken(asset.Username)

	if token == "" {
		return nil, fmt.Errorf("token not found for given asset username:%s", asset.Username)
	}

	req, err := http.NewRequest(http.MethodGet, asset.url(), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set(internal.HeaderAuthorization, "Bearer "+token)
	req.Header.Set(internal.HeaderAccept, internal.MediaTypeOctetStream)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	resp, err := c.doRequest(req)
	if err != nil {
		return nil, fmt.Errorf("download asset: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, fmt.Errorf("download asset failed: %s", string(body))
	}

	return resp.Body, nil
}
