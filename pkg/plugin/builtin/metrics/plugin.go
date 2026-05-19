// Package metrics 暴露 Prometheus 指标。
package metrics

import (
	"context"
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/shuffleman/mcte/pkg/plugin"
	"github.com/shuffleman/mcte/pkg/session"
)

// Plugin Prometheus 指标插件。
type Plugin struct {
	listen string

	active prometheus.Gauge
	pass   prometheus.Counter
	tunn   prometheus.Counter
	rxByte prometheus.Counter
	txByte prometheus.Counter

	srv      *http.Server
	stopFlag atomic.Bool
}

// New 构造插件；listen 为空时不会启动 HTTP。
func New(listen string) *Plugin {
	p := &Plugin{
		listen: listen,
		active: prometheus.NewGauge(prometheus.GaugeOpts{Name: "mcte_active_sessions", Help: "Active sessions"}),
		pass:   prometheus.NewCounter(prometheus.CounterOpts{Name: "mcte_passthrough_total", Help: "Total passthrough connections"}),
		tunn:   prometheus.NewCounter(prometheus.CounterOpts{Name: "mcte_tunnel_total", Help: "Total tunnel connections"}),
		rxByte: prometheus.NewCounter(prometheus.CounterOpts{Name: "mcte_rx_bytes_total", Help: "Bytes received"}),
		txByte: prometheus.NewCounter(prometheus.CounterOpts{Name: "mcte_tx_bytes_total", Help: "Bytes sent"}),
	}
	prometheus.MustRegister(p.active, p.pass, p.tunn, p.rxByte, p.txByte)
	return p
}

func (p *Plugin) Name() string { return "metrics" }

func (p *Plugin) Start(ctx context.Context) error {
	if p.listen == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	p.srv = &http.Server{Addr: p.listen, Handler: mux}
	go func() { _ = p.srv.ListenAndServe() }()
	return nil
}

func (p *Plugin) Stop(ctx context.Context) error {
	if p.srv != nil {
		return p.srv.Shutdown(ctx)
	}
	return nil
}

func (p *Plugin) OnSessionEvent(s *session.Session, ev plugin.SessionEvent) {
	switch ev {
	case plugin.SessionOpen:
		p.active.Inc()
	case plugin.SessionClose:
		p.active.Dec()
	}
}
