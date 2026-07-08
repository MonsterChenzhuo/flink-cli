package flink

import (
	"context"
	"errors"
	"testing"
)

func TestClassifyAPIErrorHTML(t *testing.T) {
	err := &NonJSONResponseError{Path: "/jobs/overview", Prefix: "<!DOCTYPE html>", Message: "html", IsHTML: true}
	class := ClassifyAPIError(err)
	if class.Code != "NON_JSON_HTML_RESPONSE" {
		t.Fatalf("code = %q, want NON_JSON_HTML_RESPONSE", class.Code)
	}
	if class.Details["likely_cause"] != "yarn_proxy_expired_or_login_page" {
		t.Fatalf("missing likely_cause: %v", class.Details)
	}
}

func TestClassifyAPIErrorNonJSON(t *testing.T) {
	err := &NonJSONResponseError{Path: "/jobs/overview", Prefix: "garbage", Message: "invalid character"}
	class := ClassifyAPIError(err)
	if class.Code != "NON_JSON_RESPONSE" {
		t.Fatalf("code = %q, want NON_JSON_RESPONSE", class.Code)
	}
}

func TestClassifyAPIErrorHTTPStatus(t *testing.T) {
	err := &HTTPError{Path: "/jobs/x", StatusCode: 500}
	class := ClassifyAPIError(err)
	if class.Code != "HTTP_STATUS_ERROR" {
		t.Fatalf("code = %q, want HTTP_STATUS_ERROR", class.Code)
	}
	if class.Details["http_status"] != 500 {
		t.Fatalf("http_status = %v, want 500", class.Details["http_status"])
	}
}

func TestClassifyAPIErrorTLS(t *testing.T) {
	err := errors.New("Get \"https://gw/jobs/overview\": x509: certificate signed by unknown authority")
	class := ClassifyAPIError(err)
	if class.Code != "TLS_CERT_ERROR" {
		t.Fatalf("code = %q, want TLS_CERT_ERROR", class.Code)
	}
	flags, ok := class.Details["retriable_flags"].([]string)
	if !ok || len(flags) == 0 || flags[0] != "--insecure-skip-verify" {
		t.Fatalf("retriable_flags = %v, want [--insecure-skip-verify]", class.Details["retriable_flags"])
	}
}

func TestClassifyAPIErrorTimeout(t *testing.T) {
	class := ClassifyAPIError(context.DeadlineExceeded)
	if class.Code != "FLINK_API_TIMEOUT" {
		t.Fatalf("code = %q, want FLINK_API_TIMEOUT", class.Code)
	}
}

func TestClassifyAPIErrorFallback(t *testing.T) {
	class := ClassifyAPIError(errors.New("connection refused"))
	if class.Code != "FLINK_API_UNREACHABLE" {
		t.Fatalf("code = %q, want FLINK_API_UNREACHABLE", class.Code)
	}
}

func TestLooksLikeHTML(t *testing.T) {
	cases := map[string]bool{
		"<!DOCTYPE html><html>": true,
		"<html><head>":          true,
		"  <div>error</div>":    true,
		`{"jobs":[]}`:           false,
		"":                      false,
		"plain text":            false,
	}
	for body, want := range cases {
		if got := looksLikeHTML([]byte(body)); got != want {
			t.Fatalf("looksLikeHTML(%q) = %v, want %v", body, got, want)
		}
	}
}
