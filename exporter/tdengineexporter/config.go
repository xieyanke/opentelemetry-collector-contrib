// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tdengineexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/tdengineexporter"

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "github.com/taosdata/driver-go/v3/taosWS" //nolint:golint,unused
	"go.opentelemetry.io/collector/component"
	"go.uber.org/multierr"
)

// nolint
var (
	driverName      = "taosWS"
	defaultProtocol = "ws"
)

var (
	errConfigNoAddress           = errors.New("address must be specified")
	errConfigInvalidAddress      = errors.New("address must be host:port format")
	errConfigProtocolUnsupported = errors.New("protocol must be \"ws\" or \"http\"")
)

type tdengineConfig struct {
	Username   string     `mapstructure:"username"`
	Password   string     `mapstructure:"password"`
	Protocol   string     `mapstructure:"protocol"`
	Address    string     `mapstructure:"address"`
	ConnParams ConnParams `mapstructure:"conn_params"`
}

type ConnParams struct {
	ReadTimeout        string `mapstructure:"read_timeout"`
	WriteTimeout       string `mapstructure:"write_timeout"`
	ReadBufferSize     uint   `mapstructure:"read_buffer_size"`
	DisableCompression bool   `mapstructure:"disable_compression"`
}

func (params ConnParams) ToString(protocol string) string {
	if protocol == "ws" {
		if params.ReadTimeout != "" && params.WriteTimeout != "" {
			paramsFmt := "?%s=%s&%s=%s"
			return fmt.Sprintf(paramsFmt, "readTimeout", params.ReadTimeout, "writeTimeout", params.WriteTimeout)
		}

		if params.ReadTimeout != "" {
			paramsFmt := "?%s=%s"
			return fmt.Sprintf(paramsFmt, "readTimeout", params.ReadTimeout)
		}

		if params.WriteTimeout != "" {
			paramsFmt := "?%s=%s"
			return fmt.Sprintf(paramsFmt, "writeTimeout", params.WriteTimeout)
		}
	}

	if protocol == "http" {
		if params.ReadBufferSize != 0 && params.DisableCompression {
			paramsFmt := "?%s=%t&%s=%d"
			return fmt.Sprintf(paramsFmt, "disableCompression", true, "readBufferSize", params.ReadBufferSize)
		}

		if params.ReadBufferSize != 0 {
			paramsFmt := "?%s=%t&%s=%d"
			return fmt.Sprintf(paramsFmt, "disableCompression", false, "readBufferSize", params.ReadBufferSize)
		}

		paramsFmt := "?%s=%t"
		return fmt.Sprintf(paramsFmt, "disableCompression", params.DisableCompression)
	}

	return ""
}

type Config struct {
	tdengineConfig        `mapstructure:",squash"`
	LogsSuperTableName    string `mapstructure:"logs_stable_name"`
	MetricsSuperTableName string `mapstructure:"metrics_stable_name"`
	TracesSuperTableName  string `mapstructure:"traces_stable_name"`
	TTLDays               uint   `mapstructure:"ttl_days"`
}

func createDefaultConfig() component.Config {
	return &Config{
		LogsSuperTableName:    "logs.otel",
		TracesSuperTableName:  "traces.otel",
		MetricsSuperTableName: "metrics.otel",
		TTLDays:               0,
		tdengineConfig: tdengineConfig{
			Address:  "localhost:6041",
			Username: "root",
			Protocol: "ws",
		},
	}
}

func (cfg *Config) Validate() (err error) {
	if cfg.Address == "" {
		err = multierr.Append(err, errConfigNoAddress)
	}

	if len(strings.Split(cfg.Address, ":")) != 2 {
		err = multierr.Append(err, errConfigInvalidAddress)
	}

	if cfg.Protocol != "ws" && cfg.Protocol != "http" {
		err = multierr.Append(err, errConfigProtocolUnsupported)
	}

	return err
}

func (cfg *Config) buildDSN(database string) string {
	// [username[:password]@][protocol[(address)]]/[dbname][?param1=value1&...&paramN=valueN]
	dsnFmt := "%s:%s@%s(%s)/%s"

	var dsn string
	if cfg.Password == "" {
		dsnFmt = "%s@%s(%s)/%s"
		dsn = fmt.Sprintf(dsnFmt, cfg.Username, cfg.Protocol, cfg.Address, database)
	} else {
		dsn = fmt.Sprintf(dsnFmt, cfg.Username, cfg.Password, cfg.Protocol, cfg.Address, database)
	}

	dsn += cfg.ConnParams.ToString(cfg.Protocol)
	return dsn
}

// nolint
func (cfg *Config) buildDB(database string) (*sql.DB, error) {

	dsn := cfg.buildDSN(database)

	if cfg.Protocol == "http" {
		driverName = "taosRestful"
	}

	conn, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
