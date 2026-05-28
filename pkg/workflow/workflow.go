package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
)

type StepKind string

const (
	StepKindActivity StepKind = "activity"
	StepKindTimer    StepKind = "timer"
)

type RetryPolicy struct {
	MaxAttempts    int           `json:"max_attempts"`
	InitialBackoff time.Duration `json:"initial_backoff"`
	MaxBackoff     time.Duration `json:"max_backoff"`
}

func (r RetryPolicy) Normalize() RetryPolicy {
	if r.MaxAttempts <= 0 {
		r.MaxAttempts = 1
	}
	if r.InitialBackoff <= 0 {
		r.InitialBackoff = 250 * time.Millisecond
	}
	if r.MaxBackoff <= 0 || r.MaxBackoff < r.InitialBackoff {
		r.MaxBackoff = 5 * time.Second
	}
	return r
}

type Step struct {
	ID                string            `json:"id"`
	Kind              StepKind          `json:"kind"`
	Activity          string            `json:"activity,omitempty"`
	Queue             string            `json:"queue,omitempty"`
	Delay             time.Duration     `json:"delay,omitempty"`
	Timeout           time.Duration     `json:"timeout,omitempty"`
	Dependencies      []string          `json:"dependencies,omitempty"`
	Retry             RetryPolicy       `json:"retry"`
	Compensation      string            `json:"compensation,omitempty"`
	CompensationQueue string            `json:"compensation_queue,omitempty"`
	Metadata          map[string]string `json:"metadata,omitempty"`
}

type Definition struct {
	Name  string          `json:"name"`
	Steps map[string]Step `json:"steps"`
	order []string
}

func (d Definition) StepIDs() []string {
	return slices.Clone(d.order)
}

func (d Definition) RootSteps() []Step {
	out := make([]Step, 0)
	for _, id := range d.order {
		step := d.Steps[id]
		if len(step.Dependencies) == 0 {
			out = append(out, step)
		}
	}
	return out
}

func (d Definition) Dependents(stepID string) []Step {
	out := make([]Step, 0)
	for _, id := range d.order {
		step := d.Steps[id]
		if slices.Contains(step.Dependencies, stepID) {
			out = append(out, step)
		}
	}
	return out
}

type ActivityInput struct {
	RunID        string                     `json:"run_id"`
	Workflow     string                     `json:"workflow"`
	StepID       string                     `json:"step_id"`
	Attempt      int                        `json:"attempt"`
	Compensation bool                       `json:"compensation"`
	RunInput     json.RawMessage            `json:"run_input"`
	Dependencies map[string]json.RawMessage `json:"dependencies,omitempty"`
	Metadata     map[string]string          `json:"metadata,omitempty"`
}

type ActivityHandler func(context.Context, ActivityInput) (any, error)

type permanentError struct {
	error
}

func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{error: err}
}

func IsNonRetryable(err error) bool {
	var target permanentError
	return errors.As(err, &target)
}

type Builder struct {
	name  string
	steps map[string]Step
	order []string
}

func New(name string) *Builder {
	return &Builder{
		name:  name,
		steps: make(map[string]Step),
		order: make([]string, 0),
	}
}

func (b *Builder) Activity(id, activity string, opts ...StepOption) *Builder {
	step := Step{
		ID:       id,
		Kind:     StepKindActivity,
		Activity: activity,
		Queue:    "default",
		Retry: RetryPolicy{
			MaxAttempts:    3,
			InitialBackoff: 250 * time.Millisecond,
			MaxBackoff:     3 * time.Second,
		},
	}
	for _, opt := range opts {
		opt(&step)
	}
	b.put(step)
	return b
}

func (b *Builder) Timer(id string, delay time.Duration, opts ...StepOption) *Builder {
	step := Step{
		ID:    id,
		Kind:  StepKindTimer,
		Delay: delay,
		Retry: RetryPolicy{
			MaxAttempts:    1,
			InitialBackoff: 250 * time.Millisecond,
			MaxBackoff:     250 * time.Millisecond,
		},
	}
	for _, opt := range opts {
		opt(&step)
	}
	b.put(step)
	return b
}

func (b *Builder) put(step Step) {
	if _, exists := b.steps[step.ID]; !exists {
		b.order = append(b.order, step.ID)
	}
	if step.Queue == "" && step.Kind == StepKindActivity {
		step.Queue = "default"
	}
	step.Retry = step.Retry.Normalize()
	b.steps[step.ID] = step
}

func (b *Builder) Build() (Definition, error) {
	if b.name == "" {
		return Definition{}, errors.New("workflow name is required")
	}
	if len(b.steps) == 0 {
		return Definition{}, errors.New("workflow must contain at least one step")
	}
	if err := validateSteps(b.steps, b.order); err != nil {
		return Definition{}, err
	}
	return Definition{
		Name:  b.name,
		Steps: b.steps,
		order: slices.Clone(b.order),
	}, nil
}

type StepOption func(*Step)

func DependsOn(ids ...string) StepOption {
	return func(step *Step) {
		step.Dependencies = append(step.Dependencies, ids...)
	}
}

func WithQueue(name string) StepOption {
	return func(step *Step) {
		step.Queue = name
	}
}

func WithRetry(policy RetryPolicy) StepOption {
	return func(step *Step) {
		step.Retry = policy
	}
}

func WithTimeout(timeout time.Duration) StepOption {
	return func(step *Step) {
		step.Timeout = timeout
	}
}

func WithCompensation(activity string, queue string) StepOption {
	return func(step *Step) {
		step.Compensation = activity
		step.CompensationQueue = queue
	}
}

func WithMetadata(key, value string) StepOption {
	return func(step *Step) {
		if step.Metadata == nil {
			step.Metadata = make(map[string]string)
		}
		step.Metadata[key] = value
	}
}

func validateSteps(steps map[string]Step, order []string) error {
	for _, id := range order {
		step := steps[id]
		if step.ID == "" {
			return errors.New("step id is required")
		}
		switch step.Kind {
		case StepKindActivity:
			if step.Activity == "" {
				return fmt.Errorf("activity step %q is missing activity name", step.ID)
			}
		case StepKindTimer:
			if step.Delay <= 0 {
				return fmt.Errorf("timer step %q must have a positive delay", step.ID)
			}
		default:
			return fmt.Errorf("step %q has unsupported kind %q", step.ID, step.Kind)
		}
		for _, dep := range step.Dependencies {
			if dep == step.ID {
				return fmt.Errorf("step %q cannot depend on itself", step.ID)
			}
			if _, ok := steps[dep]; !ok {
				return fmt.Errorf("step %q depends on unknown step %q", step.ID, dep)
			}
		}
	}
	color := make(map[string]int)
	var visit func(string) error
	visit = func(id string) error {
		switch color[id] {
		case 1:
			return fmt.Errorf("workflow contains a cycle at step %q", id)
		case 2:
			return nil
		}
		color[id] = 1
		for _, dep := range steps[id].Dependencies {
			if err := visit(dep); err != nil {
				return err
			}
		}
		color[id] = 2
		return nil
	}
	for _, id := range order {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}

type Registry struct {
	definitions map[string]Definition
	handlers    map[string]ActivityHandler
}

func NewRegistry() *Registry {
	return &Registry{
		definitions: make(map[string]Definition),
		handlers:    make(map[string]ActivityHandler),
	}
}

func (r *Registry) Register(def Definition) error {
	if _, exists := r.definitions[def.Name]; exists {
		return fmt.Errorf("workflow %q already registered", def.Name)
	}
	r.definitions[def.Name] = def
	return nil
}

func (r *Registry) MustRegister(def Definition) {
	if err := r.Register(def); err != nil {
		panic(err)
	}
}

func (r *Registry) RegisterActivity(name string, handler ActivityHandler) {
	r.handlers[name] = handler
}

func (r *Registry) MustRegisterActivity(name string, handler ActivityHandler) {
	if _, exists := r.handlers[name]; exists {
		panic(fmt.Sprintf("activity %q already registered", name))
	}
	r.handlers[name] = handler
}

func (r *Registry) Definition(name string) (Definition, bool) {
	def, ok := r.definitions[name]
	return def, ok
}

func (r *Registry) Handler(name string) (ActivityHandler, bool) {
	handler, ok := r.handlers[name]
	return handler, ok
}

func (r *Registry) Workflows() []string {
	names := make([]string, 0, len(r.definitions))
	for name := range r.definitions {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}
