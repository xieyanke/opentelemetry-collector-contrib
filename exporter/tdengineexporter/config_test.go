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
					ConnParams: ConnParams{ReadTimeout: "30m", WriteTimeout: "10s"},
				},
				LogsSuperTableName:    "logs.otel",
				MetricsSuperTableName: "metrics.otel",
				TracesSuperTableName:  "traces.otel",
				TTLDays:               3,
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
					ConnParams: ConnParams{ReadBufferSize: 52428800, DisableCompression: false},
				},
				LogsSuperTableName:    "logs.otel",
				MetricsSuperTableName: "metrics.otel",
				TracesSuperTableName:  "traces.otel",
				TTLDays:               0,
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

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	tests := []struct {
		id       component.ID
		expected string
	}{
		{id: component.NewIDWithName(typeStr, ""), expected: "root@ws(localhost:6041)/"},
		{id: component.NewIDWithName(typeStr, "ws_full"), expected: "foo:bar@ws(127.0.0.1:6041)/?readTimeout=30m&writeTimeout=10s"},
		{id: component.NewIDWithName(typeStr, "ws_with_param_read_timeout"), expected: "root@ws(127.0.0.1:6041)/?readTimeout=30m"},
		{id: component.NewIDWithName(typeStr, "ws_with_param_write_timeout"), expected: "root@ws(127.0.0.1:6041)/?writeTimeout=10s"},
		{id: component.NewIDWithName(typeStr, "rest_full"), expected: "foo:bar@http(127.0.0.1:6041)/?disableCompression=false&readBufferSize=52428800"},
		{id: component.NewIDWithName(typeStr, "rest_with_param_buffer_size"), expected: "root@http(127.0.0.1:6041)/?disableCompression=false&readBufferSize=524"},
		{id: component.NewIDWithName(typeStr, "rest_with_param_disable_compression"), expected: "root@http(127.0.0.1:6041)/?disableCompression=true"},
	}

	for _, tt := range tests {
		t.Run(tt.id.Name(), func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, component.UnmarshalConfig(sub, cfg))
			assert.NoError(t, component.ValidateConfig(cfg))
			require.Equal(t, tt.expected, cfg.(*Config).buildDSN(""))
		})
	}
}
