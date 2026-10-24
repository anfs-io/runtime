package harness

import (
	"bytes"
	"encoding/json"
	"strconv"
)

// Usage is what a run cost, as its harness reports it: run.completed's (and run.failed's)
// model, input_tokens, output_tokens and cost_usd (anfs-io/events REGISTRY.md, "Run usage";
// OpenTelemetry's gen_ai.response.model, gen_ai.usage.input_tokens and output_tokens). A field the
// harness does not report is "".
type Usage struct {
	Model        string
	InputTokens  string // every prompt token the model processed, cached or not
	OutputTokens string
	CostUSD      string // the harness's own figure, an estimate
}

// Data puts the fields that are known into an event's data; model falls back to the rule's.
func (u Usage) Data(data map[string]string, ruleModel string) {
	if u.Model == "" {
		u.Model = ruleModel
	}
	for k, v := range map[string]string{"model": u.Model, "input_tokens": u.InputTokens, "output_tokens": u.OutputTokens, "cost_usd": u.CostUSD} {
		if v != "" {
			data[k] = v
		}
	}
}

// Known says whether the harness reported anything.
func (u Usage) Known() bool { return u != Usage{} }

// UsageOf reads a run's output for its usage. claude (stream-json, checked against 2.1.296):
// the init line's model; the result line's total_cost_usd, and its modelUsage (per model,
// cumulative: inputTokens + cacheReadInputTokens + cacheCreationInputTokens, outputTokens) or,
// without it, its usage (input_tokens + cache_read_input_tokens + cache_creation_input_tokens,
// output_tokens). The last result line wins (a retried turn writes another). opencode, pi and
// antigravity: their output as anfsd runs them gives no usage.
func UsageOf(harness string, out []byte) Usage {
	if harness != "claude" {
		return Usage{}
	}
	var u Usage
	for _, l := range bytes.Split(out, []byte("\n")) {
		l = bytes.TrimSpace(l)
		if !bytes.HasPrefix(l, []byte("{")) || (!bytes.Contains(l, []byte(`"result"`)) && !bytes.Contains(l, []byte(`"init"`))) {
			continue
		}
		var m struct {
			Type         string       `json:"type"`
			Subtype      string       `json:"subtype"`
			Model        string       `json:"model"`
			TotalCostUSD *json.Number `json:"total_cost_usd"`
			Usage        *struct {
				Input       int64 `json:"input_tokens"`
				CacheRead   int64 `json:"cache_read_input_tokens"`
				CacheCreate int64 `json:"cache_creation_input_tokens"`
				Output      int64 `json:"output_tokens"`
			} `json:"usage"`
			ModelUsage map[string]struct {
				Input       int64 `json:"inputTokens"`
				CacheRead   int64 `json:"cacheReadInputTokens"`
				CacheCreate int64 `json:"cacheCreationInputTokens"`
				Output      int64 `json:"outputTokens"`
			} `json:"modelUsage"`
		}
		d := json.NewDecoder(bytes.NewReader(l))
		d.UseNumber()
		if d.Decode(&m) != nil {
			continue
		}
		switch {
		case m.Type == "system" && m.Subtype == "init" && m.Model != "":
			u.Model = m.Model
		case m.Type == "result":
			u.InputTokens, u.OutputTokens, u.CostUSD = "", "", ""
			if m.TotalCostUSD != nil {
				if f, err := m.TotalCostUSD.Float64(); err == nil {
					u.CostUSD = strconv.FormatFloat(f, 'f', -1, 64)
				}
			}
			var in, outT int64
			switch {
			case len(m.ModelUsage) > 0:
				for name, mu := range m.ModelUsage {
					in += mu.Input + mu.CacheRead + mu.CacheCreate
					outT += mu.Output
					if u.Model == "" && len(m.ModelUsage) == 1 {
						u.Model = name
					}
				}
			case m.Usage != nil:
				in, outT = m.Usage.Input+m.Usage.CacheRead+m.Usage.CacheCreate, m.Usage.Output
			default:
				continue
			}
			u.InputTokens, u.OutputTokens = strconv.FormatInt(in, 10), strconv.FormatInt(outT, 10)
		}
	}
	return u
}
