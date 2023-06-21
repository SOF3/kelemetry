// Copyright 2023 The Kelemetry Authors.
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

package multitrace

import (
	"context"
	"fmt"
	"math/rand"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/jaegertracing/jaeger/model"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"k8s.io/utils/clock"

	"github.com/kubewharf/kelemetry/pkg/frontend/tracecache"
	pkghttp "github.com/kubewharf/kelemetry/pkg/http"
	"github.com/kubewharf/kelemetry/pkg/manager"
	"github.com/kubewharf/kelemetry/pkg/metrics"
	"github.com/kubewharf/kelemetry/pkg/util/shutdown"
)

func init() {
	manager.Global.Provide("jaeger-multitrace-endpoint", manager.Ptr(&server{}))
}

type options struct {
	enable bool
}

func (options *options) Setup(fs *pflag.FlagSet) {
	fs.BoolVar(&options.enable, "jaeger-multitrace-enable", false, "enable multitrace endpoint for frontend")
}

func (options *options) EnableFlag() *bool { return &options.enable }

type server struct {
	options    options
	Logger     logrus.FieldLogger
	Clock      clock.Clock
	TraceCache tracecache.Cache
	Server     pkghttp.Server

	RequestMetric *metrics.Metric[*requestMetric]
}

type requestMetric struct {
	Error metrics.LabeledError
}

func (*requestMetric) MetricName() string { return "multitrace_request" }

func (server *server) Options() manager.Options {
	return &server.options
}

func (server *server) Init() error {
	server.Server.Routes().GET("/multitrace/:traceIds", func(ctx *gin.Context) {
		logger := server.Logger.WithField("source", ctx.Request.RemoteAddr)
		defer shutdown.RecoverPanic(logger)
		metric := &requestMetric{}
		defer server.RequestMetric.DeferCount(server.Clock.Now(), metric)

		logger.WithField("query", ctx.Request.URL.RawQuery).Infof("GET %v", ctx.Request.URL.Path)

		if code, err := server.handleGet(ctx, metric); err != nil {
			logger.WithError(err).Error()
			ctx.Status(code)
			_, _ = ctx.Writer.WriteString(err.Error())
			ctx.Abort()
		}
	})

	return nil
}

func (server *server) Start(ctx context.Context) error { return nil }

func (server *server) handleGet(ctx *gin.Context, metric *requestMetric) (code int, err error) {
	queryString := ctx.Param("traceIds")

	cacheIds := []model.TraceID{}
	for _, id := range strings.Split(queryString, "...") {
		parsed, err := model.TraceIDFromString(id)
		if err != nil {
			return 400, fmt.Errorf("invalid trace ID %q: %w", id, err)
		}

		cacheIds = append(cacheIds, parsed)
	}

	if len(cacheIds) == 0 {
		return 400, fmt.Errorf("empty query")
	}

	var mergedEntry *tracecache.EntryValue

	for _, cacheId := range cacheIds {
		value, err := server.TraceCache.Fetch(ctx, cacheId.Low)
		if err != nil {
			metric.Error = metrics.MakeLabeledError("FetchError")
			return 500, fmt.Errorf("failed to load cache: %w", err)
		}

		if value == nil {
			metric.Error = metrics.MakeLabeledError("CacheNotFound")
			return 404, fmt.Errorf("cache ID %016x does not exist", cacheId.Low)
		}

		if mergedEntry == nil {
			mergedEntry = value
		} else {
			mergedEntry.Identifiers = append(mergedEntry.Identifiers, value.Identifiers...)

			// union of time range
			if mergedEntry.StartTime.After(value.StartTime) {
				mergedEntry.StartTime = value.StartTime
			}
			if mergedEntry.EndTime.Before(value.EndTime) {
				mergedEntry.EndTime = value.EndTime
			}
		}

		mergedEntry.RootObject = nil // TODO: support exclusive by refactoring
	}

	newCacheId := model.NewTraceID(
		cacheIds[0].High,
		rand.Uint64(),
	)

	server.TraceCache.Persist(ctx, []tracecache.Entry{
		{
			LowId: newCacheId.Low,
			Value: *mergedEntry,
		},
	})

	switch ctx.ContentType() {
	case "application/json":
		ctx.Redirect(302, fmt.Sprintf("/api/traces/%s", newCacheId.String()))
	case "application/vnd.kelemetry.trace-id":
		ctx.Redirect(200, newCacheId.String())
	case "text/html":
		ctx.Redirect(302, fmt.Sprintf("/trace/%s", newCacheId.String()))
	default:
		ctx.Redirect(302, fmt.Sprintf("/trace/%s", newCacheId.String()))
	}

	return 0, nil
}

func (server *server) Close(ctx context.Context) error { return nil }
