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

package transform

import (
	"context"
	"fmt"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"

	"github.com/kubewharf/kelemetry/pkg/frontend/extension"
	tfconfig "github.com/kubewharf/kelemetry/pkg/frontend/tf/config"
	tftree "github.com/kubewharf/kelemetry/pkg/frontend/tf/tree"
	"github.com/kubewharf/kelemetry/pkg/manager"
	utilobject "github.com/kubewharf/kelemetry/pkg/util/object"
)

func init() {
	manager.Global.Provide("jaeger-transform", manager.Ptr(&Transformer{}))
}

type TransformerOptions struct{}

func (options *TransformerOptions) Setup(fs *pflag.FlagSet) {}

func (options *TransformerOptions) EnableFlag() *bool { return nil }

type Transformer struct {
	options          TransformerOptions
	Logger           logrus.FieldLogger
	Configs          tfconfig.Provider
	ExtensionFactory *manager.List[extension.ProviderFactory]
}

func (transformer *Transformer) Options() manager.Options        { return &transformer.options }
func (transformer *Transformer) Init() error                     { return nil }
func (transformer *Transformer) Start(ctx context.Context) error { return nil }
func (transformer *Transformer) Close(ctx context.Context) error { return nil }

func (transformer *Transformer) Transform(
	ctx context.Context,
	trace *model.Trace,
	rootObject *utilobject.Key,
	configId tfconfig.Id,
	extensionProcessor ExtensionProcessor,
	start, end time.Time,
) error {
	if len(trace.Spans) == 0 {
		return fmt.Errorf("cannot transform empty trace")
	}

	config := transformer.Configs.GetById(configId)
	if config == nil {
		config = transformer.Configs.GetById(transformer.Configs.DefaultId())
	}

	tree := tftree.NewSpanTree(trace.Spans)

	newSpans, err := extensionProcessor.ProcessExtensions(ctx, transformer, config.Extensions, trace.Spans, start, end)
	if err != nil {
		return fmt.Errorf("cannot prepare extension trace: %w", err)
	}

	newSpans = append(newSpans, tree.GetSpans()...)
	tree = tftree.NewSpanTree(newSpans)

	for _, step := range config.Steps {
		step.Run(tree)
	}

	trace.Spans = tree.GetSpans()

	return nil
}
