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

package discovery

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/clock"

	"github.com/kubewharf/kelemetry/pkg/k8s"
	"github.com/kubewharf/kelemetry/pkg/manager"
	"github.com/kubewharf/kelemetry/pkg/metrics"
	"github.com/kubewharf/kelemetry/pkg/util/shutdown"
)

func init() {
	manager.Global.Provide("discovery", manager.Ptr[DiscoveryCache](&discoveryCache{
		hasCtx: make(chan struct{}),
	}))
}

type discoveryOptions struct {
	resyncInterval time.Duration
}

func (options *discoveryOptions) Setup(fs *pflag.FlagSet) {
	fs.DurationVar(&options.resyncInterval, "discovery-resync-interval", time.Hour, "frequency of refreshing discovery API")
}

func (options *discoveryOptions) EnableFlag() *bool { return nil }

type (
	GvrDetails = map[schema.GroupVersionResource]*metav1.APIResource
	GvrToGvk   = map[schema.GroupVersionResource]schema.GroupVersionKind
	GvkToGvr   = map[schema.GroupVersionKind]schema.GroupVersionResource
)

type DiscoveryCache interface {
	// Gets the discovery cache for a specific cluster.
	// Lazily initializes the cache if it is not present.
	ForCluster(name string) (ClusterDiscoveryCache, error)
}

type ClusterDiscoveryCache interface {
	GetAll() GvrDetails
	LookupKind(gvr schema.GroupVersionResource) (schema.GroupVersionKind, bool)
	LookupResource(gvk schema.GroupVersionKind) (schema.GroupVersionResource, bool)
	RequestAndWaitResync()
	// AddResyncHandler adds a channel that sends when at least one GVR has changed.
	// Should only be called during Init stage.
	AddResyncHandler() <-chan struct{}
}

type discoveryCache struct {
	options discoveryOptions
	Logger  logrus.FieldLogger
	Clock   clock.Clock
	Clients k8s.Clients

	// Discovery loops should run indefinitely but lazily.
	// For now we do not consider the need for removing clusters.
	ctx    context.Context //nolint:containedctx
	hasCtx chan struct{}

	ResyncMetric      *metrics.Metric[*resyncMetric]
	LookupErrorMetric *metrics.Metric[*lookupErrorMetric]

	clusters sync.Map
}

type clusterDiscoveryCache struct {
	initLock sync.Once
	initErr  error

	options           *discoveryOptions
	logger            logrus.FieldLogger
	clock             clock.Clock
	resyncMetric      metrics.TaggedMetric
	lookupErrorMetric *metrics.Metric[*lookupErrorMetric]
	client            k8s.Client

	onResyncCh    []chan<- struct{}
	resyncCv      *sync.Cond
	isDoingResync bool

	dataLock   sync.RWMutex
	gvrDetails GvrDetails
	gvrToGvk   GvrToGvk
	gvkToGvr   GvkToGvr
}

type resyncMetric struct {
	Cluster string
}

func (*resyncMetric) MetricName() string { return "discovery_resync" }

type lookupErrorMetric struct {
	Cluster string
	Type    string
	Query   string
}

func (*lookupErrorMetric) MetricName() string { return "discovery_lookup_error" }

func (dc *discoveryCache) Options() manager.Options { return &dc.options }

func (dc *discoveryCache) Init() error { return nil }

func (dc *discoveryCache) Start(ctx context.Context) error {
	dc.ctx = ctx
	close(dc.hasCtx)
	return nil
}

func (dc *discoveryCache) Close(ctx context.Context) error { return nil }

func (dc *discoveryCache) ForCluster(cluster string) (ClusterDiscoveryCache, error) {
	cacheAny, _ := dc.clusters.LoadOrStore(cluster, &clusterDiscoveryCache{resyncCv: sync.NewCond(&sync.Mutex{})})
	cdc := cacheAny.(*clusterDiscoveryCache)
	// no matter we loaded or stored, someone has to initialize it the first time.
	cdc.initLock.Do(func() {
		cdc.logger = dc.Logger.WithField("cluster", cluster)
		cdc.clock = dc.Clock
		cdc.options = &dc.options
		cdc.resyncMetric = dc.ResyncMetric.With(&resyncMetric{
			Cluster: cluster,
		})
		cdc.lookupErrorMetric = dc.LookupErrorMetric
		cdc.client, cdc.initErr = dc.Clients.Cluster(cluster)
		if cdc.initErr != nil {
			return
		}

		cdc.logger.Info("initialized cluster discovery cache")
		go func() {
			<-dc.hasCtx
			cdc.run(dc.ctx)
		}()
	})
	return cdc, cdc.initErr
}

func (cdc *clusterDiscoveryCache) run(ctx context.Context) {
	defer shutdown.RecoverPanic(cdc.logger)

	for {
		cdc.doResync()

		select {
		case <-ctx.Done():
			return
		case <-cdc.clock.After(cdc.options.resyncInterval):
		}
	}
}

func (cdc *clusterDiscoveryCache) doResync() {
	cdc.resyncCv.L.Lock()
	defer cdc.resyncCv.L.Lock()

	if cdc.isDoingResync {
		// wait for isDoingResync to be set to false by other goroutines
		for cdc.isDoingResync {
			cdc.resyncCv.Wait()
		}

		return
	}

	cdc.isDoingResync = true
	defer func() {
		cdc.isDoingResync = false
		cdc.resyncCv.Broadcast()
	}()

	if err := cdc.tryDoResync(); err != nil {
		cdc.logger.Error(err)
	}
}

func (cdc *clusterDiscoveryCache) tryDoResync() error {
	defer cdc.resyncMetric.DeferCount(cdc.clock.Now())

	// TODO also sync non-target clusters
	lists, err := cdc.client.KubernetesClient().Discovery().ServerPreferredResources()
	if err != nil {
		return fmt.Errorf("query discovery API failed: %w", err)
	}

	gvrDetails := GvrDetails{}
	gvrToGvk := GvrToGvk{}
	gvkToGvr := GvkToGvr{}

	for _, list := range lists {
		listGroupVersion, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			return fmt.Errorf("discovery API result contains invalid groupVersion: %w", err)
		}

		for _, res := range list.APIResources {
			group := res.Group
			if group == "" {
				group = listGroupVersion.Group
			}

			version := res.Version
			if version == "" {
				version = listGroupVersion.Version
			}

			gvr := schema.GroupVersionResource{
				Group:    group,
				Version:  version,
				Resource: res.Name,
			}
			gvk := schema.GroupVersionKind{
				Group:   group,
				Version: version,
				Kind:    res.Kind,
			}

			resCopy := res
			gvrDetails[gvr] = &resCopy
			gvrToGvk[gvr] = gvk
			gvkToGvr[gvk] = gvr
		}
	}

	cdc.dataLock.Lock()
	oldGvrToGvk := cdc.gvrToGvk
	cdc.gvrToGvk = gvrToGvk
	cdc.gvkToGvr = gvkToGvr
	cdc.gvrDetails = gvrDetails
	cdc.dataLock.Unlock()

	if !haveSameKeys(oldGvrToGvk, gvrToGvk) {
		cdc.logger.WithField("resourceCount", len(gvrDetails)).Info("Discovery API response changed, triggering resync")
		for _, onResyncCh := range cdc.onResyncCh {
			select {
			case onResyncCh <- struct{}{}:
			default:
			}
		}
	}

	return nil
}

func haveSameKeys(m1, m2 GvrToGvk) bool {
	if len(m1) != len(m2) {
		return false
	}

	for k := range m1 {
		if _, exists := m2[k]; !exists {
			return false
		}
	}

	return true
}

func (cdc *clusterDiscoveryCache) GetAll() GvrDetails {
	cdc.dataLock.RLock()
	defer cdc.dataLock.RUnlock()

	return cdc.gvrDetails
}

func (cdc *clusterDiscoveryCache) LookupKind(gvr schema.GroupVersionResource) (schema.GroupVersionKind, bool) {
	cdc.dataLock.RLock()
	gvk, exists := cdc.gvrToGvk[gvr]
	cdc.dataLock.RUnlock()

	if !exists {
		cdc.RequestAndWaitResync()

		cdc.dataLock.RLock()
		gvk, exists = cdc.gvrToGvk[gvr]
		cdc.dataLock.RUnlock()

		if !exists {
			cdc.lookupErrorMetric.With(&lookupErrorMetric{
				Cluster: cdc.client.ClusterName(),
				Type:    "LookupKind",
				Query:   gvr.String(),
			})
		}
	}

	return gvk, exists
}

func (cdc *clusterDiscoveryCache) LookupResource(gvk schema.GroupVersionKind) (schema.GroupVersionResource, bool) {
	if gvk.Group == "" && gvk.Version == "" {
		gvk.Version = "v1"
	}

	cdc.dataLock.RLock()
	gvr, exists := cdc.gvkToGvr[gvk]
	cdc.dataLock.RUnlock()

	if !exists {
		cdc.RequestAndWaitResync()

		cdc.dataLock.RLock()
		gvr, exists = cdc.gvkToGvr[gvk]
		cdc.dataLock.RUnlock()

		if !exists {
			cdc.lookupErrorMetric.With(&lookupErrorMetric{
				Cluster: cdc.client.ClusterName(),
				Type:    "LookupResource",
				Query:   gvk.String(),
			})
		}
	}

	return gvr, exists
}

func (cdc *clusterDiscoveryCache) RequestAndWaitResync() {
	cdc.doResync()
}

func (cdc *clusterDiscoveryCache) AddResyncHandler() <-chan struct{} {
	ch := make(chan struct{}, 1)
	cdc.onResyncCh = append(cdc.onResyncCh, ch)
	return ch
}

func SplitGroupVersion(apiVersion string) schema.GroupVersion {
	gv := strings.SplitN(apiVersion, "/", 2)
	return schema.GroupVersion{
		Group:   gv[0],
		Version: gv[1],
	}
}
