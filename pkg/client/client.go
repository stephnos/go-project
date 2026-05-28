package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	baseURL    string
	httpClient *http.Client
}

type Run struct {
	ID           string            `json:"id"`
	WorkflowName string            `json:"workflow_name"`
	Status       string            `json:"status"`
	Input        json.RawMessage   `json:"input"`
	Output       json.RawMessage   `json:"output,omitempty"`
	Failure      string            `json:"failure,omitempty"`
	CancelReason string            `json:"cancel_reason,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
}

type Task struct {
	ID           string          `json:"id"`
	StepID       string          `json:"step_id"`
	Kind         string          `json:"kind"`
	Phase        string          `json:"phase"`
	Status       string          `json:"status"`
	Attempt      int             `json:"attempt"`
	QueueName    string          `json:"queue_name"`
	ActivityName string          `json:"activity_name,omitempty"`
	AvailableAt  time.Time       `json:"available_at"`
	LastError    string          `json:"last_error,omitempty"`
	Input        json.RawMessage `json:"input"`
}

type Event struct {
	RunID     string          `json:"run_id"`
	Sequence  int64           `json:"sequence"`
	Type      string          `json:"type"`
	StepID    string          `json:"step_id,omitempty"`
	TaskID    string          `json:"task_id,omitempty"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

type RunView struct {
	Run   Run    `json:"run"`
	Tasks []Task `json:"tasks"`
}

type ReplayReport struct {
	Run            Run             `json:"run"`
	SummaryMatches bool            `json:"summary_matches"`
	HistoryLength  int             `json:"history_length"`
	State          json.RawMessage `json:"state"`
	Tasks          []Task          `json:"tasks"`
}

func New(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

func (c *Client) StartRun(ctx context.Context, workflow string, input any, metadata map[string]string) (Run, error) {
	payload := map[string]any{"workflow": workflow, "input": input, "metadata": metadata}
	var response struct {
		Run Run `json:"run"`
	}
	if err := c.do(ctx, http.MethodPost, "/v1/runs", payload, &response); err != nil {
		return Run{}, err
	}
	return response.Run, nil
}

func (c *Client) ListRuns(ctx context.Context, limit int) ([]Run, error) {
	var response struct {
		Runs []Run `json:"runs"`
	}
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/v1/runs?limit=%d", limit), nil, &response); err != nil {
		return nil, err
	}
	return response.Runs, nil
}

func (c *Client) GetRun(ctx context.Context, runID string) (RunView, error) {
	var view RunView
	if err := c.do(ctx, http.MethodGet, "/v1/runs/"+runID, nil, &view); err != nil {
		return RunView{}, err
	}
	return view, nil
}

func (c *Client) History(ctx context.Context, runID string) ([]Event, error) {
	var response struct {
		Events []Event `json:"events"`
	}
	if err := c.do(ctx, http.MethodGet, "/v1/runs/"+runID+"/history", nil, &response); err != nil {
		return nil, err
	}
	return response.Events, nil
}

func (c *Client) Cancel(ctx context.Context, runID string, reason string) error {
	return c.do(ctx, http.MethodPost, "/v1/runs/"+runID+"/cancel", map[string]string{"reason": reason}, nil)
}

func (c *Client) Replay(ctx context.Context, runID string) (ReplayReport, error) {
	var report ReplayReport
	if err := c.do(ctx, http.MethodPost, "/v1/runs/"+runID+"/replay", map[string]string{}, &report); err != nil {
		return ReplayReport{}, err
	}
	return report, nil
}

func (c *Client) Workflows(ctx context.Context) ([]string, error) {
	var response struct {
		Workflows []string `json:"workflows"`
	}
	if err := c.do(ctx, http.MethodGet, "/v1/workflows", nil, &response); err != nil {
		return nil, err
	}
	return response.Workflows, nil
}

func (c *Client) do(ctx context.Context, method string, path string, requestBody any, out any) error {
	var body io.Reader
	if requestBody != nil {
		payload, err := json.Marshal(requestBody)
		if err != nil {
			return err
		}
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed: %s", strings.TrimSpace(string(body)))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
