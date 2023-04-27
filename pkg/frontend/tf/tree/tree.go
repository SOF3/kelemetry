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

package tftree

import (
	"fmt"

	"github.com/jaegertracing/jaeger/model"
	"k8s.io/apimachinery/pkg/util/sets"
)

type SpanTree struct {
	spanMap      map[model.SpanID]*model.Span
	childrenMap  map[model.SpanID]sets.Set[model.SpanID]
	visitorStack map[model.SpanID]*stackEntry
	exited       sets.Set[model.SpanID]
	Root         *model.Span
}

type spanNode struct {
	tree SpanTree
	node *model.Span
}

type stackEntry struct {
	unprocessedChildren []model.SpanID
}

func NewSpanTree(trace *model.Trace) SpanTree {
	tree := SpanTree{
		spanMap:      make(map[model.SpanID]*model.Span, len(trace.Spans)),
		childrenMap:  make(map[model.SpanID]sets.Set[model.SpanID], len(trace.Spans)),
		visitorStack: make(map[model.SpanID]*stackEntry),
		exited:       make(sets.Set[model.SpanID], len(trace.Spans)),
	}

	for _, span := range trace.Spans {
		tree.spanMap[span.SpanID] = span
		if len(span.References) == 0 {
			tree.Root = span
		} else {
			ref := span.References[0]
			parentId := ref.SpanID
			if _, exists := tree.childrenMap[parentId]; !exists {
				tree.childrenMap[parentId] = sets.Set[model.SpanID]{}
			}
			tree.childrenMap[parentId].Insert(span.SpanID)
		}
	}

	return tree
}

func (tree SpanTree) Span(id model.SpanID) *model.Span                { return tree.spanMap[id] }
func (tree SpanTree) Children(id model.SpanID) sets.Set[model.SpanID] { return tree.childrenMap[id] }

func (tree SpanTree) GetSpans() []*model.Span {
	spans := make([]*model.Span, 0, len(tree.spanMap))
	for _, span := range tree.spanMap {
		spans = append(spans, span)
	}
	return spans
}

// Sets the root to a span under the current root, and prunes other spans not under the new root.
func (tree *SpanTree) SetRoot(id model.SpanID) {
	if len(tree.visitorStack) != 0 {
		panic("Cannot call SetRoot")
	}

	newRoot, exists := tree.spanMap[id]
	if !exists {
		panic("SetRoot can only be used on nodes in the tree")
	}

	tree.Root = newRoot

	collector := &spanIdCollector{spanIds: sets.Set[model.SpanID]{}}
	tree.Visit(collector)

	for spanId := range tree.spanMap {
		if _, exists := collector.spanIds[spanId]; !exists {
			delete(tree.spanMap, spanId)
		}
	}

	for spanId := range tree.childrenMap {
		if _, exists := collector.spanIds[spanId]; !exists {
			delete(tree.childrenMap, spanId)
		}
	}
}

type spanIdCollector struct {
	spanIds sets.Set[model.SpanID]
}

func (collector *spanIdCollector) Enter(tree SpanTree, span *model.Span) TreeVisitor {
	collector.spanIds.Insert(span.SpanID)
	return collector
}
func (collector *spanIdCollector) Exit(tree SpanTree, span *model.Span) {}

func (tree SpanTree) Visit(visitor TreeVisitor) {
	spanNode{tree: tree, node: tree.Root}.visit(visitor)
}

func (subtree spanNode) visit(visitor TreeVisitor) {
	subvisitor := visitor.Enter(subtree.tree, subtree.node)
	// enter before visitorStack is populated to allow removal

	if _, stillExists := subtree.tree.spanMap[subtree.node.SpanID]; !stillExists {
		// deleted during enter
		return
	}

	stack := &stackEntry{}
	subtree.tree.visitorStack[subtree.node.SpanID] = stack

	stack.unprocessedChildren = make([]model.SpanID, 0, len(subtree.tree.childrenMap))
	for childId := range subtree.tree.childrenMap[subtree.node.SpanID] {
		stack.unprocessedChildren = append(stack.unprocessedChildren, childId)
	}

	for len(stack.unprocessedChildren) > 0 {
		children := stack.unprocessedChildren
		stack.unprocessedChildren = nil

		for _, childId := range children {
			child := subtree.tree.spanMap[childId]

			if child.SpanID == subtree.node.SpanID {
				panic(fmt.Sprintf("childrenMap is not a tree (spanId %s duplicated)", child.SpanID))
			}

			spanNode{
				tree: subtree.tree,
				node: child,
			}.visit(subvisitor)
		}
	}

	delete(subtree.tree.visitorStack, subtree.node.SpanID)

	visitor.Exit(subtree.tree, subtree.node)
	subtree.tree.exited.Insert(subtree.node.SpanID)

	if len(subtree.tree.visitorStack) == 0 {
		for spanId := range subtree.tree.exited {
			delete(subtree.tree.exited, spanId)
		}
	}
}

// Adds a span as a child of another.
//
// parentId must not be exited yet (and may or may not be entered).
func (tree SpanTree) Add(newSpan *model.Span, parentId model.SpanID) {
	if tree.exited.Has(parentId) {
		panic("cannot add under already-exited span")
	}

	tree.spanMap[newSpan.SpanID] = newSpan
	newSpan.References = []model.SpanRef{{
		TraceID: tree.Root.TraceID,
		SpanID:  parentId,
		RefType: model.ChildOf,
	}}

	if _, exists := tree.childrenMap[parentId]; !exists {
		tree.childrenMap[parentId] = sets.Set[model.SpanID]{}
	}
	tree.childrenMap[parentId].Insert(newSpan.SpanID)

	if stack, hasStack := tree.visitorStack[parentId]; hasStack {
		// parent already entered, not yet exited
		// therefore we have to push to its unprcoessed queue
		stack.unprocessedChildren = append(stack.unprocessedChildren, newSpan.SpanID)
	}
	// else, parent not yet entered, and there are no side effects to clean
}

// Moves a span from one span to another.
//
// movedSpan must not be entered yet.
// newParent must not be exited yet (and may or may not be entered).
func (tree SpanTree) Move(movedSpanId model.SpanID, newParentId model.SpanID) {
	if tree.exited.Has(newParentId) {
		panic("cannot move already-exited span")
	}

	movedSpan := tree.spanMap[movedSpanId]
	if len(movedSpan.References) == 0 {
		panic("cannot move root span")
	}

	ref := &movedSpan.References[0]
	oldParentId := ref.SpanID
	ref.SpanID = newParentId

	if _, exists := tree.childrenMap[newParentId]; !exists {
		tree.childrenMap[newParentId] = sets.Set[model.SpanID]{}
	}
	tree.childrenMap[newParentId].Insert(movedSpanId)

	delete(tree.childrenMap[oldParentId], movedSpanId)

	if stack, hasStack := tree.visitorStack[newParentId]; hasStack {
		// parent already entered, not yet exited
		// therefore we have to push to its unprcoessed queue
		stack.unprocessedChildren = append(stack.unprocessedChildren, movedSpanId)
	}
	// else, parent not yet entered, and there are no side effects to clean
}

// Deletes a span entirely from the tree.
//
// All remaining children also get deleted from the list of spans.
// Can only be called when entering spanId or entering its parents (i.e. deleting descendents).
// TreeVisitor.Exit will not be called if Delete is called during enter.
func (tree SpanTree) Delete(spanId model.SpanID) {
	if _, exists := tree.spanMap[spanId]; !exists {
		panic("cannot delete a deleted span")
	}
	if _, entered := tree.visitorStack[spanId]; entered {
		panic("cannot delete a parent span")
	}
	if tree.exited.Has(spanId) {
		panic("cannot delete an exited span")
	}

	span := tree.spanMap[spanId]
	delete(tree.spanMap, spanId)

	if len(span.References) > 0 {
		parentSpanId := span.References[0].SpanID
		delete(tree.childrenMap[parentSpanId], spanId)
	}

	if childrenMap, hasChildren := tree.childrenMap[spanId]; hasChildren {
		for childId := range childrenMap {
			tree.Delete(childId)
		}

		delete(tree.childrenMap, spanId)
	}
}

type TreeVisitor interface {
	// Called before entering the descendents of the span.
	//
	// This method may call tree.Add, tree.Move and tree.Delete
	// with the ancestors of span, span itself and its descendents.
	Enter(tree SpanTree, span *model.Span) TreeVisitor
	// Called after exiting the descendents of the span.
	Exit(tree SpanTree, span *model.Span)
}
