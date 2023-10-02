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

package tfstep

import (
	"github.com/jaegertracing/jaeger/model"

	tfconfig "github.com/kubewharf/kelemetry/pkg/frontend/tf/config"
	tftree "github.com/kubewharf/kelemetry/pkg/frontend/tf/tree"
	"github.com/kubewharf/kelemetry/pkg/manager"
	"github.com/kubewharf/kelemetry/pkg/util/zconstants"
)

func init() {
	manager.Global.ProvideListImpl(
		"tf-step/object-tags-visitor",
		manager.Ptr(&tfconfig.VisitorStep[ObjectTagsVisitor]{}),
		&manager.List[tfconfig.RegisteredStep]{},
	)
}

// Copy tags from child spans to the object.
type ObjectTagsVisitor struct {
	ResourceTags []string `json:"resourceTags"`
}

func (ObjectTagsVisitor) Kind() string { return "ObjectTagsVisitor" }

func (visitor ObjectTagsVisitor) Enter(tree *tftree.SpanTree, span *model.Span) tftree.TreeVisitor {
	if tagKv, isPseudo := model.KeyValues(span.Tags).FindByKey(zconstants.PseudoType); !isPseudo ||
		tagKv.VStr != string(zconstants.PseudoTypeObject) {
		return visitor
	}
	if _, hasTag := model.KeyValues(span.Tags).FindByKey("resource"); !hasTag {
		return visitor
	}

	for _, resourceKey := range visitor.ResourceTags {
		_ = visitor.findTagRecursively(tree, span, resourceKey)
	}

	return visitor
}

func (visitor ObjectTagsVisitor) Exit(tree *tftree.SpanTree, span *model.Span) {}

func (visitor ObjectTagsVisitor) findTagRecursively(tree *tftree.SpanTree, span *model.Span, tagKey string) model.KeyValue {
	if kv, hasTag := model.KeyValues(span.Tags).FindByKey(tagKey); hasTag {
		return kv
	}

	for childId := range tree.Children(span.SpanID) {
		childSpan := tree.Span(childId)
		{
			tagKv, isPseudo := model.KeyValues(childSpan.Tags).FindByKey(zconstants.PseudoType)
			if isPseudo && tagKv.VStr == string(zconstants.PseudoTypeObject) {
				// do not copy from another object
				continue
			}
		}

		kv := visitor.findTagRecursively(tree, childSpan, tagKey)
		if len(kv.Key) > 0 {
			span.Tags = append(span.Tags, kv)
			return kv
		}
	}
	return model.KeyValue{}
}
