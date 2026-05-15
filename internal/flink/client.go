package flink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
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

type JobNotFoundError struct {
	JobID        string
	AvailableIDs []string
}

func (e *JobNotFoundError) Error() string {
	return fmt.Sprintf("job %q not found in /jobs/overview", e.JobID)
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

func (c *Client) ListJobs(ctx context.Context) ([]JobOverview, error) {
	var overview struct {
		Jobs []JobOverview `json:"jobs"`
	}
	if err := c.getJSON(ctx, "/jobs/overview", &overview); err != nil {
		return nil, err
	}
	return overview.Jobs, nil
}

func (c *Client) ListTaskManagers(ctx context.Context) ([]TaskManagerOverview, error) {
	var response TaskManagersResponse
	if err := c.getJSON(ctx, "/taskmanagers", &response); err != nil {
		return nil, err
	}
	return response.TaskManagers, nil
}

func (c *Client) GetThreadDump(ctx context.Context, taskManagerID string) (ThreadDump, error) {
	taskManagerID = strings.TrimSpace(taskManagerID)
	if taskManagerID == "" {
		return ThreadDump{}, fmt.Errorf("empty TaskManager id")
	}
	var dump ThreadDump
	if err := c.getJSON(ctx, "/taskmanagers/"+url.PathEscape(taskManagerID)+"/thread-dump", &dump); err != nil {
		return ThreadDump{}, err
	}
	return dump, nil
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
	jobs, err := filterJobs(overview.Jobs, opts.JobID)
	if err != nil {
		return Snapshot{}, err
	}
	for _, job := range jobs {
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
			if shouldCollectDorisMetrics(*vertex) {
				metrics, endpoints, err := c.collectDorisSinkMetrics(ctx, job.JID, *vertex)
				if err != nil {
					s.addOptionalWarning(fmt.Sprintf("fetch Doris sink metrics %s/%s", job.JID, vertex.ID), err)
				} else if metrics != nil {
					s.SourceEndpoints = append(s.SourceEndpoints, endpoints...)
					vertex.DorisMetrics = metrics
				}
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

func filterJobs(jobs []JobOverview, jobID string) ([]JobOverview, error) {
	if jobID == "" {
		return jobs, nil
	}
	out := make([]JobOverview, 0, 1)
	available := make([]string, 0, len(jobs))
	for _, job := range jobs {
		available = append(available, job.JID)
		if job.JID == jobID {
			out = append(out, job)
		}
	}
	if len(out) == 0 {
		return nil, &JobNotFoundError{JobID: jobID, AvailableIDs: available}
	}
	return out, nil
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

type NonJSONResponseError struct {
	Path    string
	Prefix  string
	Message string
}

func (e *NonJSONResponseError) Error() string {
	if e.Prefix == "" {
		return fmt.Sprintf("GET %s returned non-JSON response: %s", e.Path, e.Message)
	}
	return fmt.Sprintf("GET %s returned non-JSON response: %s; prefix=%q", e.Path, e.Message, e.Prefix)
}

func (c *Client) getJSON(ctx context.Context, apiPath string, out any) error {
	return c.getJSONWithQuery(ctx, apiPath, nil, out)
}

func (c *Client) getJSONWithQuery(ctx context.Context, apiPath string, query url.Values, out any) error {
	endpoint := c.base.Endpoint(apiPath)
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
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
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return fmt.Errorf("read %s: %w", apiPath, err)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return nonJSONError(apiPath, body, err)
	}
	return nil
}

type metricID struct {
	ID string `json:"id"`
}

type metricValue struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}

var dorisMetricSuffixes = []string{
	"totalFlushSucceededNumber",
	"totalFlushFailedNumber",
	"totalFlushLoadedRows",
	"totalFlushLoadBytes",
	"totalFlushTimeMs",
	"loadTimeMs_mean",
	"loadTimeMs_max",
	"writeDataTimeMs_mean",
	"writeDataTimeMs_max",
	"beginTxnTimeMs_mean",
	"commitAndPublishTimeMs_mean",
	"putDataTimeMs_mean",
}

func shouldCollectDorisMetrics(vertex Vertex) bool {
	name := strings.ToLower(vertex.Name)
	return strings.Contains(name, "doris") && strings.Contains(name, "writer")
}

func (c *Client) collectDorisSinkMetrics(ctx context.Context, jobID string, vertex Vertex) (*DorisSinkMetrics, []string, error) {
	sampleSubtasks := sampleSubtaskIndexes(vertex.Parallelism)
	samples := make([]DorisSinkMetricsSample, 0, len(sampleSubtasks))
	endpoints := make([]string, 0, len(sampleSubtasks))
	var firstErr error
	for _, subtask := range sampleSubtasks {
		metricsPath := fmt.Sprintf("/jobs/%s/vertices/%s/subtasks/%d/metrics", jobID, vertex.ID, subtask)
		var listed []metricID
		if err := c.getJSON(ctx, metricsPath, &listed); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		endpoints = append(endpoints, metricsPath)
		ids := selectDorisSubtaskMetricIDs(listed)
		if len(ids) == 0 {
			continue
		}
		values := map[string]float64{}
		const chunkSize = 40
		for start := 0; start < len(ids); start += chunkSize {
			end := start + chunkSize
			if end > len(ids) {
				end = len(ids)
			}
			var rows []metricValue
			query := url.Values{"get": []string{strings.Join(ids[start:end], ",")}}
			if err := c.getJSONWithQuery(ctx, metricsPath, query, &rows); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			for _, row := range rows {
				if v, ok := parseMetricFloat(row.Value); ok {
					values[row.ID] = v
				}
			}
		}
		if sample, ok := buildDorisSinkMetricsSample(subtask, ids, values); ok {
			samples = append(samples, sample)
		}
	}
	if len(samples) == 0 {
		return nil, endpoints, firstErr
	}
	return &DorisSinkMetrics{Summary: summarizeDorisSinkMetrics(samples), Samples: samples}, endpoints, nil
}

func sampleSubtaskIndexes(parallelism int) []int {
	if parallelism <= 0 {
		return []int{0}
	}
	candidates := []int{0, 1, 2, parallelism / 4, parallelism / 2, parallelism * 3 / 4, parallelism - 1}
	out := make([]int, 0, len(candidates))
	seen := map[int]bool{}
	for _, idx := range candidates {
		if idx < 0 || idx >= parallelism || seen[idx] {
			continue
		}
		seen[idx] = true
		out = append(out, idx)
	}
	return out
}

func selectDorisSubtaskMetricIDs(listed []metricID) []string {
	ids := make([]string, 0, len(dorisMetricSuffixes))
	for _, row := range listed {
		for _, suffix := range dorisMetricSuffixes {
			if matchesDorisMetricSuffix(row.ID, suffix) {
				ids = append(ids, row.ID)
				break
			}
		}
	}
	return ids
}

func matchesDorisMetricSuffix(id, suffix string) bool {
	return strings.HasSuffix(id, "_"+suffix) || strings.HasSuffix(id, "."+suffix)
}

func buildDorisSinkMetricsSample(subtask int, ids []string, values map[string]float64) (DorisSinkMetricsSample, bool) {
	bySuffix := map[string]float64{}
	for _, id := range ids {
		value, ok := values[id]
		if !ok {
			continue
		}
		for _, suffix := range dorisMetricSuffixes {
			if matchesDorisMetricSuffix(id, suffix) {
				bySuffix[suffix] = value
				break
			}
		}
	}
	if len(bySuffix) == 0 {
		return DorisSinkMetricsSample{}, false
	}
	return DorisSinkMetricsSample{
		Subtask:                    subtask,
		FlushSucceeded:             bySuffix["totalFlushSucceededNumber"],
		FlushFailed:                bySuffix["totalFlushFailedNumber"],
		FlushLoadedRows:            bySuffix["totalFlushLoadedRows"],
		FlushLoadBytes:             bySuffix["totalFlushLoadBytes"],
		FlushTimeMs:                bySuffix["totalFlushTimeMs"],
		LoadTimeMsMean:             bySuffix["loadTimeMs_mean"],
		LoadTimeMsMax:              bySuffix["loadTimeMs_max"],
		WriteDataTimeMsMean:        bySuffix["writeDataTimeMs_mean"],
		WriteDataTimeMsMax:         bySuffix["writeDataTimeMs_max"],
		BeginTxnTimeMsMean:         bySuffix["beginTxnTimeMs_mean"],
		CommitAndPublishTimeMsMean: bySuffix["commitAndPublishTimeMs_mean"],
		PutDataTimeMsMean:          bySuffix["putDataTimeMs_mean"],
	}, true
}

func summarizeDorisSinkMetrics(samples []DorisSinkMetricsSample) DorisSinkMetricsSummary {
	summary := DorisSinkMetricsSummary{SampledSubtasks: make([]int, 0, len(samples))}
	var perFlushRows []float64
	var perFlushBytes []float64
	var loadMeans []float64
	var writeMeans []float64
	var beginMeans []float64
	var commitMeans []float64
	for _, sample := range samples {
		summary.SampledSubtasks = append(summary.SampledSubtasks, sample.Subtask)
		summary.FlushSucceededTotal += sample.FlushSucceeded
		summary.FlushFailedTotal += sample.FlushFailed
		if sample.FlushSucceeded > 0 {
			perFlushRows = append(perFlushRows, sample.FlushLoadedRows/sample.FlushSucceeded)
			perFlushBytes = append(perFlushBytes, sample.FlushLoadBytes/sample.FlushSucceeded)
		}
		if sample.LoadTimeMsMean > 0 {
			loadMeans = append(loadMeans, sample.LoadTimeMsMean)
		}
		if sample.LoadTimeMsMax > summary.LoadTimeMsMax {
			summary.LoadTimeMsMax = sample.LoadTimeMsMax
		}
		if sample.WriteDataTimeMsMean > 0 {
			writeMeans = append(writeMeans, sample.WriteDataTimeMsMean)
		}
		if sample.WriteDataTimeMsMax > summary.WriteDataTimeMsMax {
			summary.WriteDataTimeMsMax = sample.WriteDataTimeMsMax
		}
		if sample.BeginTxnTimeMsMean > 0 {
			beginMeans = append(beginMeans, sample.BeginTxnTimeMsMean)
		}
		if sample.CommitAndPublishTimeMsMean > 0 {
			commitMeans = append(commitMeans, sample.CommitAndPublishTimeMsMean)
		}
	}
	summary.PerFlushRowsMean = round3(mean(perFlushRows))
	summary.PerFlushBytesMean = round3(mean(perFlushBytes))
	summary.LoadTimeMsMean = round3(mean(loadMeans))
	summary.WriteDataTimeMsMean = round3(mean(writeMeans))
	summary.BeginTxnTimeMsMean = round3(mean(beginMeans))
	summary.CommitAndPublishTimeMsMean = round3(mean(commitMeans))
	summary.PerFlushMiBMean = round3(summary.PerFlushBytesMean / 1024 / 1024)
	summary.PerFlushGiBMean = round3(summary.PerFlushBytesMean / 1024 / 1024 / 1024)
	summary.LoadTimeSecMean = round3(summary.LoadTimeMsMean / 1000)
	summary.LoadTimeSecMax = round3(summary.LoadTimeMsMax / 1000)
	summary.WriteDataTimeSecMean = round3(summary.WriteDataTimeMsMean / 1000)
	summary.WriteDataTimeSecMax = round3(summary.WriteDataTimeMsMax / 1000)
	summary.CommitAndPublishTimeSecMean = round3(summary.CommitAndPublishTimeMsMean / 1000)
	if summary.LoadTimeMsMean > 0 {
		summary.WriteDataShareOfLoad = round3(summary.WriteDataTimeMsMean / summary.LoadTimeMsMean)
	}
	if summary.LoadTimeSecMean > 0 {
		summary.LoadMiBPerSecPerSubtask = round3(summary.PerFlushMiBMean / summary.LoadTimeSecMean)
		summary.LoadGiBPerSecPerSubtask = round3(summary.PerFlushGiBMean / summary.LoadTimeSecMean)
	}
	return summary
}

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, value := range values {
		sum += value
	}
	return sum / float64(len(values))
}

func parseMetricFloat(value string) (float64, bool) {
	if value == "" {
		return 0, false
	}
	parsed, err := strconv.ParseFloat(value, 64)
	return parsed, err == nil
}

func nonJSONError(apiPath string, body []byte, err error) error {
	prefix := strings.TrimSpace(string(body))
	if len(prefix) > 120 {
		prefix = prefix[:120] + "...(truncated)"
	}
	msg := err.Error()
	if strings.HasPrefix(prefix, "<") {
		msg = "expected Flink REST JSON but got HTML; the YARN proxy application may be expired, redirected, or serving a login/error page"
	}
	return &NonJSONResponseError{Path: apiPath, Prefix: prefix, Message: msg}
}
