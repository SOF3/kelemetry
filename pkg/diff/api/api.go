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

package api

import (
	"context"
	"fmt"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/clock"

	diffcache "github.com/kubewharf/kelemetry/pkg/diff/cache"
	"github.com/kubewharf/kelemetry/pkg/http"
	"github.com/kubewharf/kelemetry/pkg/k8s"
	"github.com/kubewharf/kelemetry/pkg/k8s/objectcache"
	"github.com/kubewharf/kelemetry/pkg/manager"
	"github.com/kubewharf/kelemetry/pkg/metrics"
	utilobject "github.com/kubewharf/kelemetry/pkg/util/object"
	"github.com/kubewharf/kelemetry/pkg/util/shutdown"
)

func init() {
	manager.Global.Provide("diff-api", manager.Ptr(&api{}))
}

type apiOptions struct {
	enable bool
}

func (options *apiOptions) Setup(fs *pflag.FlagSet) {
	fs.BoolVar(&options.enable, "diff-api-enable", false, "enable diff API")
}

func (options *apiOptions) EnableFlag() *bool { return &options.enable }

type api struct {
	options       apiOptions
	Logger        logrus.FieldLogger
	Clock         clock.Clock
	DiffCache     diffcache.Cache
	ObjectCache   *objectcache.ObjectCache
	Server        http.Server
	Clients       k8s.Clients
	RequestMetric *metrics.Metric[*requestMetric]
	ScanMetric    *metrics.Metric[*scanMetric]
}

type (
	requestMetric struct{}
	scanMetric    struct{}
)

func (*requestMetric) MetricName() string { return "diff_api_request" }
func (*scanMetric) MetricName() string    { return "diff_api_scan" }

func (api *api) Options() manager.Options {
	return &api.options
}

func (api *api) Init() error {
	api.Server.Routes().GET("/diff/:group/:version/:resource/:namespace/:name/:rv", func(ctx *gin.Context) {
		logger := api.Logger.WithField("source", ctx.Request.RemoteAddr)
		defer shutdown.RecoverPanic(logger)
		metric := &requestMetric{}
		defer api.RequestMetric.DeferCount(api.Clock.Now(), metric)

		if err := api.handleGet(ctx); err != nil {
			logger.WithError(err).Error()
		}
	})

	api.Server.Routes().GET("/diff/:group/:version/:resource/:namespace/:name", func(ctx *gin.Context) {
		logger := api.Logger.WithField("source", ctx.Request.RemoteAddr)
		defer shutdown.RecoverPanic(logger)
		metric := &scanMetric{}
		defer api.ScanMetric.DeferCount(api.Clock.Now(), metric)

		if err := api.handleScan(ctx); err != nil {
			logger.WithError(err).Error()
		}
	})

	return nil
}

func (api *api) Start(ctx context.Context) error { return nil }

func (api *api) handleGet(ctx *gin.Context) error {
	group := ctx.Param("group")
	version := ctx.Param("version")
	resource := ctx.Param("resource")
	namespace := ctx.Param("namespace")
	name := ctx.Param("name")
	rv := ctx.Param("rv")

	cluster := api.Clients.TargetCluster().ClusterName()
	if clusterQuery := ctx.Query("cluster"); clusterQuery != "" {
		cluster = clusterQuery
	}

	raw, err := api.ObjectCache.Get(ctx, utilobject.VersionedKey{
		Key: utilobject.Key{
			Cluster:   cluster,
			Group:     group,
			Resource:  resource,
			Namespace: namespace,
			Name:      name,
		},
		Version: version,
	})
	if err != nil {
		return err
	}
	if raw == nil {
		return ctx.AbortWithError(404, fmt.Errorf("object does not exist"))
	}

	object := utilobject.RichFromUnstructured(raw, cluster, schema.GroupVersionResource{
		Group:    group,
		Version:  version,
		Resource: resource,
	})

	patch, err := api.DiffCache.Fetch(ctx, object.Key, rv, &rv)
	if err != nil || patch == nil {
		return ctx.AbortWithError(404, fmt.Errorf("patch not found for rv: %w", err))
	}

	ctx.JSON(200, patch)

	return nil
}

func (api *api) handleScan(ctx *gin.Context) error {
	group := ctx.Param("group")
	version := ctx.Param("version")
	resource := ctx.Param("resource")
	namespace := ctx.Param("namespace")
	name := ctx.Param("name")

	cluster := api.Clients.TargetCluster().ClusterName()
	if clusterQuery := ctx.Query("cluster"); clusterQuery != "" {
		cluster = clusterQuery
	}

	limitString := ctx.Query("100")
	limit := 100
	if parsedLimit, err := strconv.Atoi(limitString); err == nil {
		limit = parsedLimit
	}

	raw, err := api.ObjectCache.Get(ctx, utilobject.VersionedKey{
		Key: utilobject.Key{
			Cluster:   cluster,
			Group:     group,
			Resource:  resource,
			Namespace: namespace,
			Name:      name,
		},
		Version: version,
	})
	if err != nil {
		return err
	}
	if raw == nil {
		return ctx.AbortWithError(404, fmt.Errorf("object does not exist"))
	}

	object := utilobject.RichFromUnstructured(raw, cluster, schema.GroupVersionResource{
		Group:    group,
		Version:  version,
		Resource: resource,
	})
	list, err := api.DiffCache.List(ctx, object.Key, limit)
	if err != nil {
		return err
	}

	ctx.JSON(200, list)

	return nil
}

func (api *api) Close(ctx context.Context) error { return nil }
