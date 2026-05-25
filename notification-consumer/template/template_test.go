package template

import (
	"strings"
	"testing"
)

func TestEngine_Render(t *testing.T) {
	engine := NewEngine()

	tests := []struct {
		name     string
		tmpl     string
		metadata string
		want     string
		wantErr  bool
	}{
		{
			name:     "no variables",
			tmpl:     "Hello world",
			metadata: "{}",
			want:     "Hello world",
		},
		{
			name:     "empty metadata",
			tmpl:     "Hello world",
			metadata: "",
			want:     "Hello world",
		},
		{
			name:     "simple substitution",
			tmpl:     "Hello {{.Name}}, your order {{.OrderID}} has shipped",
			metadata: `{"Name": "Ahmet", "OrderID": "12345"}`,
			want:     "Hello Ahmet, your order 12345 has shipped",
		},
		{
			name:     "multiple variables",
			tmpl:     "{{.Greeting}} {{.Name}}! Code: {{.Code}}",
			metadata: `{"Greeting": "Hi", "Name": "Sertac", "Code": "ABC123"}`,
			want:     "Hi Sertac! Code: ABC123",
		},
		{
			name:     "invalid template syntax",
			tmpl:     "Hello {{.Name",
			metadata: `{"Name": "Test"}`,
			wantErr:  true,
		},
		{
			name:     "invalid json metadata",
			tmpl:     "Hello {{.Name}}",
			metadata: `not json`,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := engine.Render(tt.tmpl, []byte(tt.metadata))
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !strings.Contains(got, tt.want) && got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}
