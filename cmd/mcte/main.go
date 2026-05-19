// MCTE 独立运行入口。
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/shuffleman/mcte/pkg/config"
	"github.com/shuffleman/mcte/pkg/engine"
)

func main() {
	cfgPath := flag.String("config", "config/example.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	var zl *zap.Logger
	if cfg.Log.Format == "json" {
		zl, _ = zap.NewProduction()
	} else {
		zl, _ = zap.NewDevelopment()
	}
	defer zl.Sync()

	eng, err := engine.New(cfg, zl, nil)
	if err != nil {
		zl.Fatal("engine init", zap.Error(err))
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	zl.Info("mcte started")
	if err := eng.Run(ctx); err != nil {
		zl.Fatal("engine run", zap.Error(err))
	}
}
