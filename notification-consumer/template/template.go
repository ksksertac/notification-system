package template

import (
	"bytes"
	"encoding/json"
	"fmt"
	"text/template"
)

type Engine interface {
	Render(tmpl string, metadata []byte) (string, error)
}

type goTemplateEngine struct{}

func NewEngine() Engine {
	return &goTemplateEngine{}
}

func (e *goTemplateEngine) Render(tmpl string, metadata []byte) (string, error) {
	if len(metadata) == 0 || string(metadata) == "{}" {
		return tmpl, nil
	}

	t, err := template.New("notification").Parse(tmpl)
	if err != nil {
		return "", fmt.Errorf("parsing template: %w", err)
	}

	var vars map[string]interface{}
	if err := json.Unmarshal(metadata, &vars); err != nil {
		return "", fmt.Errorf("parsing metadata: %w", err)
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, vars); err != nil {
		return "", fmt.Errorf("executing template: %w", err)
	}

	return buf.String(), nil
}
