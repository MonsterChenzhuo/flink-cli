package flink

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Client struct {
	base       BaseURL
	httpClient *http.Client
}

func NewClient(rawURL string) (*Client, error) {
	return NewClientWithHTTP(rawURL, &http.Client{Timeout: 30 * time.Second})
}

func NewClientWithHTTP(rawURL string, hc *http.Client) (*Client, error) {
	base, err := NormalizeBaseURL(rawURL)
	if err != nil {
		return nil, err
	}
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{base: base, httpClient: hc}, nil
}

func (c *Client) Collect(ctx context.Context) (Snapshot, error) {
	var overview struct {
		Jobs []JobOverview `json:"jobs"`
	}
	if err := c.getJSON(ctx, "/jobs/overview", &overview); err != nil {
		return Snapshot{}, err
	}
	s := Snapshot{
		UIURL:            c.base.String(),
		Jobs:             make([]JobSnapshot, 0, len(overview.Jobs)),
		JobManagerConfig: map[string]string{},
	}
	var dashboard DashboardConfig
	if err := c.getJSON(ctx, "/config", &dashboard); err != nil {
		s.Warnings = append(s.Warnings, fmt.Sprintf("fetch dashboard config: %v", err))
	} else {
		s.FlinkVersion = dashboard.FlinkVersion
		s.FlinkRevision = dashboard.FlinkRevision
	}
	for _, job := range overview.Jobs {
		js := JobSnapshot{Overview: job}
		if err := c.getJSON(ctx, "/jobs/"+job.JID, &js.Detail); err != nil {
			s.Warnings = append(s.Warnings, fmt.Sprintf("fetch job detail %s: %v", job.JID, err))
		}
		var jobCfg []configEntry
		if err := c.getJSON(ctx, "/jobs/"+job.JID+"/jobmanager/config", &jobCfg); err != nil {
			s.Warnings = append(s.Warnings, fmt.Sprintf("fetch job config %s: %v", job.JID, err))
		} else {
			js.JobManagerConfig = map[string]string{}
			for _, entry := range jobCfg {
				js.JobManagerConfig[entry.Key] = entry.Value
			}
		}
		for i := range js.Detail.Vertices {
			vertex := &js.Detail.Vertices[i]
			var bp BackpressureInfo
			if err := c.getJSON(ctx, "/jobs/"+job.JID+"/vertices/"+vertex.ID+"/backpressure", &bp); err != nil {
				s.Warnings = append(s.Warnings, fmt.Sprintf("fetch backpressure %s/%s: %v", job.JID, vertex.ID, err))
			} else {
				vertex.Backpressure = &bp
			}
		}
		if err := c.getJSON(ctx, "/jobs/"+job.JID+"/exceptions", &js.Exceptions); err != nil {
			s.Warnings = append(s.Warnings, fmt.Sprintf("fetch exceptions %s: %v", job.JID, err))
		}
		if err := c.getJSON(ctx, "/jobs/"+job.JID+"/checkpoints", &js.Checkpoints); err != nil {
			s.Warnings = append(s.Warnings, fmt.Sprintf("fetch checkpoints %s: %v", job.JID, err))
		}
		s.Jobs = append(s.Jobs, js)
	}
	var cfg []configEntry
	if err := c.getJSON(ctx, "/jobmanager/config", &cfg); err != nil {
		s.Warnings = append(s.Warnings, fmt.Sprintf("fetch jobmanager config: %v", err))
	} else {
		for _, entry := range cfg {
			s.JobManagerConfig[entry.Key] = entry.Value
		}
	}
	return s, nil
}

func (c *Client) getJSON(ctx context.Context, apiPath string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base.Endpoint(apiPath), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "flink-cli")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("GET %s returned HTTP %d", apiPath, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode %s: %w", apiPath, err)
	}
	return nil
}
