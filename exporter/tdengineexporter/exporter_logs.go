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
	"context"
	"database/sql"

	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
)

type logsExporter struct {
	client *sql.DB
	logger *zap.Logger
	cfg    *Config
}

// Shutdown implements component.Component
func (e *logsExporter) Shutdown(ctx context.Context) error {
	panic("unimplemented")
}

// Start implements component.Component
func (e *logsExporter) Start(ctx context.Context, host component.Host) error {
	panic("unimplemented")
}

func newlogsExporter(logger *zap.Logger, cfg *Config) (*logsExporter, error) {
	return nil, nil
}

var _ component.Component = &logsExporter{}
