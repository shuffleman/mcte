package config

import (
	"errors"
	"os"

	"gopkg.in/yaml.v3"
)

// Load 读取并校验 YAML 配置。
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := Defaults()
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	if err := validate(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func validate(c *Config) error {
	if len(c.Users) == 0 {
		return errors.New("config: users must not be empty")
	}
	for i, u := range c.Users {
		if u.UUID == "" {
			return errors.New("config: users[" + itoaSimple(i) + "].uuid is required")
		}
	}
	// 每个启用的监听协议必须有对应的 fallback 目标
	if c.Listen.TCP != "" && len(c.FallbackTCP()) == 0 {
		return errors.New("config: listen.tcp set but no fallback.tcp / fallback.targets configured")
	}
	if c.Listen.UDP != "" && len(c.FallbackUDP()) == 0 {
		return errors.New("config: listen.udp set but no fallback.udp / fallback.targets configured")
	}
	if c.Listen.TCP == "" && c.Listen.UDP == "" {
		return errors.New("config: at least one of listen.tcp / listen.udp must be set")
	}
	return nil
}

func itoaSimple(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [16]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
