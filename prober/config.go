package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
)

type Cohort struct {
	Prefix     string `json:"prefix"`
	Dos        int    `json:"dos"`
	ConnsPerDo int    `json:"connsPerDo"`
}

type Config struct {
	WorkerURL      string   `json:"workerUrl"`
	Site           string   `json:"site"`
	DataDir        string   `json:"dataDir"`
	MetricsAddr    string   `json:"metricsAddr"`
	HealthcheckURL string   `json:"healthcheckUrl"`
	RampUpSeconds  int      `json:"rampUpSeconds"`
	TickTimeoutMs  int      `json:"tickTimeoutMs"`
	ReconnectMinMs int      `json:"reconnectMinMs"`
	ReconnectMaxMs int      `json:"reconnectMaxMs"`
	// ReconnectSpreadMs: 確立済み接続を失った直後の再接続を [0, この値] に
	// 一様分散させる。大量同時切断時の再接続ストームがDOを過負荷リセットに
	// 追い込むのを防ぐ。復旧時刻の測定は最初に戻った接続で決まるため精度への影響はない。
	ReconnectSpreadMs int `json:"reconnectSpreadMs"`
	Cohorts        []Cohort `json:"cohorts"`
}

func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := &Config{
		DataDir:        "./data",
		MetricsAddr:    "127.0.0.1:9209",
		RampUpSeconds:  60,
		TickTimeoutMs:  3500,
		ReconnectMinMs: 100,
		ReconnectMaxMs: 1500,
	}
	if err := json.Unmarshal(b, cfg); err != nil {
		return nil, err
	}
	if cfg.WorkerURL == "" {
		return nil, fmt.Errorf("workerUrl is required")
	}
	if cfg.Site == "" {
		return nil, fmt.Errorf("site is required")
	}
	if len(cfg.Cohorts) == 0 {
		return nil, fmt.Errorf("cohorts is required")
	}
	return cfg, nil
}

// wsURL converts the worker base URL to the websocket endpoint for a DO.
func (c *Config) wsURL(doName string) (string, error) {
	u, err := url.Parse(c.WorkerURL)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https", "":
		u.Scheme = "wss"
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "/ws/" + doName
	return u.String(), nil
}

// doNames expands cohorts into DO names: s-0..s-9, m-0.. etc.
func (c *Config) doNames() []struct{ Name, Cohort string } {
	var out []struct{ Name, Cohort string }
	for _, co := range c.Cohorts {
		for i := 0; i < co.Dos; i++ {
			out = append(out, struct{ Name, Cohort string }{
				Name:   fmt.Sprintf("%s-%d", co.Prefix, i),
				Cohort: co.Prefix,
			})
		}
	}
	return out
}
