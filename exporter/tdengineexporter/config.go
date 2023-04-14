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

package tdengineexporter

import (
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	_ "github.com/taosdata/driver-go/v3/taosWS"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/multierr"
)

var (
	driverName      = "taosWS" // "taosRestful"
	defaultDatabase = "otel"
)

var (
	errConfigNoEndpoint          = errors.New("endpoint must be specified")
	errConfigInvalidEndpoint     = errors.New("endpoint must be host:port format")
	errConfigProtocolUnsupported = errors.New("protocl must be \"ws\" or \"http\"")
)

type tdengineConfig struct {
	Username   string
	Password   string
	Protocol   string
	Endpoint   string
	Database   string
	ConnParams map[string]string
}

// Encode params into form "?bar=barz&foo=fooz" sorted by key
func (cfg tdengineConfig) EncodeConnParams() string {
	if cfg.ConnParams == nil {
		return ""
	}

	var buf strings.Builder
	keys := make([]string, 0, len(cfg.ConnParams))
	for k := range cfg.ConnParams {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for index, k := range keys {
		vs := cfg.ConnParams[k]
		if index == 0 {
			buf.WriteByte('?')
			buf.WriteString(k)
			buf.WriteByte('=')
			buf.WriteString(vs)
		} else {
			buf.WriteByte('&')
			buf.WriteString(k)
			buf.WriteByte('=')
			buf.WriteString(vs)
		}
	}

	return buf.String()
}

type Config struct {
	tdengineConfig   `mapstructure:",squash"`
	LogsTableName    string `mapstructure:"logs_table_name"`
	MetricsTableName string `mapstructure:"metrics_table_name"`
	TracesTableName  string `mapstructure:"traces_table_name"`
	TTLDays          uint   `mapstructure:"ttl_days"`
}

func createDefaultConfig() component.Config {
	return &Config{
		LogsTableName:    "otel_logs",
		TracesTableName:  "otel_traces",
		MetricsTableName: "otel_metrics",
		TTLDays:          0,
	}
}

func (cfg *Config) Validate() (err error) {
	if cfg.Endpoint == "" {
		err = multierr.Append(err, errConfigNoEndpoint)
	}

	if cfg.Protocol != "ws" && cfg.Protocol != "http" {
		err = multierr.Append(err, errConfigProtocolUnsupported)
	}

	return err
}

func (cfg *Config) buildDSN(database string) (string, error) {
	// [username[:password]@][protocol[(address)]]/[dbname][?param1=value1&...&paramN=valueN]
	dsnFmt := "%s:%s@%s(%s)/%s"

	if cfg.Password == "" {
		dsnFmt = "%s@%s(%s)/%s"
	}

	dsn := fmt.Sprintf(dsnFmt, cfg.Username, cfg.Password, cfg.Protocol, cfg.Endpoint, database)

	if cfg.ConnParams != nil {
		dsn += cfg.EncodeConnParams()
	}
	return dsn, nil
}

func (cfg *Config) buildDB(database string) (*sql.DB, error) {
	if database == "" {
		database = defaultDatabase
	}

	dsn, err := cfg.buildDSN(database)
	if err != nil {
		return nil, err
	}

	conn, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
