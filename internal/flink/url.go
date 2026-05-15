package flink

import (
	"fmt"
	"net/url"
	"path"
	"strings"
)

type BaseURL struct {
	u *url.URL
}

func NormalizeBaseURL(raw string) (BaseURL, error) {
	if strings.TrimSpace(raw) == "" {
		return BaseURL{}, fmt.Errorf("empty Flink Web UI URL")
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return BaseURL{}, fmt.Errorf("parse Flink Web UI URL: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return BaseURL{}, fmt.Errorf("Flink Web UI URL must include scheme and host")
	}
	u.RawQuery = ""
	u.Fragment = ""
	u.Path = strings.TrimRight(u.Path, "/")
	if u.Path == "" {
		u.Path = ""
	}
	return BaseURL{u: u}, nil
}

func (b BaseURL) String() string {
	if b.u == nil {
		return ""
	}
	return b.u.String()
}

func (b BaseURL) Endpoint(apiPath string) string {
	u := *b.u
	cleanAPI := "/" + strings.TrimLeft(apiPath, "/")
	if u.Path == "" {
		u.Path = cleanAPI
	} else {
		u.Path = path.Join(u.Path, cleanAPI)
	}
	return u.String()
}
