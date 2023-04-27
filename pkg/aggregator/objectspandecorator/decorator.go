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

package objectspandecorator

import (
	"context"

	"github.com/kubewharf/kelemetry/pkg/manager"
	"github.com/kubewharf/kelemetry/pkg/util"
)

func init() {
	manager.Global.Provide("object-span-union-decorator", manager.Ptr[UnionEventDecorator](&unionDecorator{}))
}

type Decorator interface {
	Decorate(ctx context.Context, object util.ObjectRef, traceSource string, tags map[string]string)
}

type UnionEventDecorator interface {
	manager.Component

	AddDecorator(decorator Decorator)

	Decorate(ctx context.Context, object util.ObjectRef, traceSource string, tags map[string]string)
}

type unionDecorator struct {
	manager.BaseComponent
	decorators []Decorator
}

func (union *unionDecorator) AddDecorator(decorator Decorator) {
	union.decorators = append(union.decorators, decorator)
}

func (union *unionDecorator) Decorate(ctx context.Context, object util.ObjectRef, traceSource string, tags map[string]string) {
	for _, decorator := range union.decorators {
		decorator.Decorate(ctx, object, traceSource, tags)
	}
}