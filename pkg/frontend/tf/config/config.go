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

package tfconfig

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kubewharf/kelemetry/pkg/frontend/extension"
	"github.com/kubewharf/kelemetry/pkg/manager"
)

func init() {
	manager.Global.Provide("jaeger-transform-config", manager.Ptr[Provider](&mux{
		Mux: manager.NewMux("jaeger-transform-config", false),
	}))
}

type Provider interface {
	Names() []string
	DefaultName() string
	DefaultId() Id
	GetByName(name string) *Config
	GetById(id Id) *Config
}

type Id uint32

func (id *Id) UnmarshalText(text []byte) error {
	i, err := strconv.ParseUint(string(text), 16, 32)
	if err != nil {
		return err
	}

	// #nosec G115 -- ParseUint bitSize is 32
	*id = Id(uint32(i))
	return nil
}

type Config struct {
	// The config ID, used to generate the cache ID.
	Id Id
	// The config name, used in search page display.
	Name string
	// Base config name without modifiers, used to help reconstruct the name.
	BaseName string
	// Names of modifiers, used to help reconstruct the name.
	ModifierNames sets.Set[string]
	// Only links with roles in this set are followed.
	LinkSelector LinkSelector
	// The extension traces for this config.
	Extensions []extension.Provider
	// The steps to transform the tree
	Steps []Step
}

func (config *Config) RecomputeName() {
	modifiers := config.ModifierNames.UnsortedList()
	sort.Strings(modifiers)
	if len(modifiers) > 0 {
		config.Name = fmt.Sprintf("%s [%s]", config.BaseName, strings.Join(modifiers, "+"))
	} else {
		config.Name = config.BaseName
	}
}

func (config *Config) Clone() *Config {
	steps := make([]Step, len(config.Steps))
	copy(steps, config.Steps) // no need to deep clone each step

	extensions := make([]extension.Provider, len(config.Extensions))
	copy(extensions, config.Extensions)

	return &Config{
		Id:            config.Id,
		Name:          config.Name,
		BaseName:      config.BaseName,
		ModifierNames: config.ModifierNames.Clone(),
		LinkSelector:  config.LinkSelector, // modifier changes LinkSelector by wrapping the previous value
		Extensions:    extensions,
		Steps:         steps,
	}
}

type mux struct {
	*manager.Mux
}

func (mux *mux) Names() []string               { return mux.Impl().(Provider).Names() }
func (mux *mux) DefaultName() string           { return mux.Impl().(Provider).DefaultName() }
func (mux *mux) DefaultId() Id                 { return mux.Impl().(Provider).DefaultId() }
func (mux *mux) GetByName(name string) *Config { return mux.Impl().(Provider).GetByName(name) }
func (mux *mux) GetById(id Id) *Config         { return mux.Impl().(Provider).GetById(id) }
