package flink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

type Client struct {
	base       BaseURL
	httpClient *http.Client
}

type CollectOptions struct {
	JobID       string
	MaxVertices int
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
	return c.CollectWithOptions(ctx, CollectOptions{})
}

func (c *Client) CollectWithOptions(ctx context.Context, opts CollectOptions) (Snapshot, error) {
	var overview struct {
		Jobs []JobOverview `json:"jobs"`
	}
	if err := c.getJSON(ctx, "/jobs/overview", &overview); err != nil {
		return Snapshot{}, err
	}
	s := Snapshot{
		UIURL:            c.base.String(),
		SourceEndpoints:  []string{"/jobs/overview"},
		Jobs:             make([]JobSnapshot, 0, len(overview.Jobs)),
		JobManagerConfig: map[string]string{},
	}
	var dashboard DashboardConfig
	if err := c.getJSON(ctx, "/config", &dashboard); err != nil {
		s.addOptionalWarning("fetch dashboard config", err)
	} else {
		s.SourceEndpoints = append(s.SourceEndpoints, "/config")
		s.FlinkVersion = dashboard.FlinkVersion
		s.FlinkRevision = dashboard.FlinkRevision
	}
	for _, job := range filterJobs(overview.Jobs, opts.JobID) {
		js := JobSnapshot{Overview: job}
		if err := c.getJSON(ctx, "/jobs/"+job.JID, &js.Detail); err != nil {
			s.Warnings = append(s.Warnings, fmt.Sprintf("fetch job detail %s: %v", job.JID, err))
		} else {
			s.SourceEndpoints = append(s.SourceEndpoints, "/jobs/"+job.JID)
		}
		var jobCfg []configEntry
		if err := c.getJSON(ctx, "/jobs/"+job.JID+"/jobmanager/config", &jobCfg); err != nil {
			s.addOptionalWarning(fmt.Sprintf("fetch job config %s", job.JID), err)
		} else {
			s.SourceEndpoints = append(s.SourceEndpoints, "/jobs/"+job.JID+"/jobmanager/config")
			js.JobManagerConfig = map[string]string{}
			for _, entry := range jobCfg {
				js.JobManagerConfig[entry.Key] = entry.Value
			}
		}
		maxVertices := opts.MaxVertices
		if maxVertices < 0 {
			maxVertices = 0
		}
		for i := range js.Detail.Vertices {
			if maxVertices > 0 && i >= maxVertices {
				s.Warnings = append(s.Warnings, fmt.Sprintf("skipped backpressure collection for %d vertices in job %s due to max_vertices=%d", len(js.Detail.Vertices)-i, job.JID, maxVertices))
				break
			}
			vertex := &js.Detail.Vertices[i]
			var bp BackpressureInfo
			if err := c.getJSON(ctx, "/jobs/"+job.JID+"/vertices/"+vertex.ID+"/backpressure", &bp); err != nil {
				s.addOptionalWarning(fmt.Sprintf("fetch backpressure %s/%s", job.JID, vertex.ID), err)
			} else {
				s.SourceEndpoints = append(s.SourceEndpoints, "/jobs/"+job.JID+"/vertices/"+vertex.ID+"/backpressure")
				vertex.Backpressure = &bp
			}
		}
		if err := c.getJSON(ctx, "/jobs/"+job.JID+"/exceptions", &js.Exceptions); err != nil {
			s.addOptionalWarning(fmt.Sprintf("fetch exceptions %s", job.JID), err)
		} else {
			s.SourceEndpoints = append(s.SourceEndpoints, "/jobs/"+job.JID+"/exceptions")
		}
		if err := c.getJSON(ctx, "/jobs/"+job.JID+"/checkpoints", &js.Checkpoints); err != nil {
			s.addOptionalWarning(fmt.Sprintf("fetch checkpoints %s", job.JID), err)
		} else {
			s.SourceEndpoints = append(s.SourceEndpoints, "/jobs/"+job.JID+"/checkpoints")
		}
		s.Jobs = append(s.Jobs, js)
	}
	var cfg []configEntry
	if err := c.getJSON(ctx, "/jobmanager/config", &cfg); err != nil {
		s.addOptionalWarning("fetch jobmanager config", err)
	} else {
		s.SourceEndpoints = append(s.SourceEndpoints, "/jobmanager/config")
		for _, entry := range cfg {
			s.JobManagerConfig[entry.Key] = entry.Value
		}
	}
	return s, nil
}

func filterJobs(jobs []JobOverview, jobID string) []JobOverview {
	if jobID == "" {
		return jobs
	}
	out := make([]JobOverview, 0, 1)
	for _, job := range jobs {
		if job.JID == jobID {
			out = append(out, job)
		}
	}
	return out
}

func (s *Snapshot) addOptionalWarning(prefix string, err error) {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound {
		return
	}
	s.Warnings = append(s.Warnings, fmt.Sprintf("%s: %v", prefix, err))
}

type HTTPError struct {
	Path       string
	StatusCode int
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("GET %s returned HTTP %d", e.Path, e.StatusCode)
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
		return &HTTPError{Path: apiPath, StatusCode: resp.StatusCode}
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode %s: %w", apiPath, err)
	}
	return nil
}
