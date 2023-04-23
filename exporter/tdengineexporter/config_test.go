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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	tests := []struct {
		id       component.ID
		expected component.Config
	}{
		{
			id:       component.NewIDWithName(typeStr, ""),
			expected: createDefaultConfig(),
		},
		{
			id: component.NewIDWithName(typeStr, "ws_full"),
			expected: &Config{
				tdengineConfig: tdengineConfig{
					Address:    "127.0.0.1:6041",
					Protocol:   "ws",
					Username:   "foo",
					Password:   "bar",
					Database:   "foo",
					ConnParams: ConnParams{ReadTimeout: "30m", WriteTimeout: "10s"},
				},
				LogsTableName:    "otel_logs",
				MetricsTableName: "otel_metrics",
				TracesTableName:  "otel_traces",
				TTLDays:          3,
			},
		},
		{
			id: component.NewIDWithName(typeStr, "rest_full"),
			expected: &Config{
				tdengineConfig: tdengineConfig{
					Address:    "127.0.0.1:6041",
					Protocol:   "http",
					Username:   "foo",
					Password:   "bar",
					Database:   "otel",
					ConnParams: ConnParams{ReadBufferSize: 52428800, DisableCompression: false},
				},
				LogsTableName:    "otel_logs",
				MetricsTableName: "otel_metrics",
				TracesTableName:  "otel_traces",
				TTLDays:          0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.id.Name(), func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, component.UnmarshalConfig(sub, cfg))

			assert.NoError(t, component.ValidateConfig(cfg))
			assert.Equal(t, tt.expected, cfg)
		})
	}
}

func TestBuildDSN(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("test_data", "config.yaml"))
	require.NoError(t, err)

	tests := []struct {
		id       component.ID
		dbname   string
		expected string
	}{
		{id: component.NewIDWithName(typeStr, ""), expected: "root:taosdata@ws(localhost:6041)/otel"},
		{id: component.NewIDWithName(typeStr, "ws_full"), dbname: "foo", expected: "foo:bar@ws(127.0.0.1:6041)/foo?readTimeout=30m&writeTimeout=10s"},
		{id: component.NewIDWithName(typeStr, "ws_with_param_read_timeout"), dbname: "foo", expected: "foo:bar@ws(127.0.0.1:6041)/foo?readTimeout=30m"},
		{id: component.NewIDWithName(typeStr, "ws_with_param_write_timeout"), dbname: "foo", expected: "foo:bar@ws(127.0.0.1:6041)/foo?writeTimeout=10s"},
		{id: component.NewIDWithName(typeStr, "rest_full"), dbname: "otel", expected: "foo:bar@http(127.0.0.1:6041)/otel?disableCompression=false&readBufferSize=52428800"},
		{id: component.NewIDWithName(typeStr, "rest_with_param_buffer_size"), dbname: "otel", expected: "foo:bar@http(127.0.0.1:6041)/otel?disableCompression=false&readBufferSize=524"},
		{id: component.NewIDWithName(typeStr, "rest_with_param_disable_compression"), dbname: "otel", expected: "foo:bar@http(127.0.0.1:6041)/otel?disableCompression=true"},
	}

	for _, tt := range tests {
		t.Run(tt.id.Name(), func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, component.UnmarshalConfig(sub, cfg))
			assert.NoError(t, component.ValidateConfig(cfg))

			require.Equal(t, tt.expected, cfg.(*Config).buildDSN(tt.dbname))
		})
	}
}
