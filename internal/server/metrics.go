// Copyright 2015 Eryx <evorui at gmail dot com>, All rights reserved.
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

package server

import (
	"github.com/hooto/hmetrics"
)

const (
	metricStorage     = "Storage"
	metricStorageSize = "StorageSize"
	metricService     = "Service"
	metricServiceSize = "ServiceSize"
	metricLogSync     = "LogSync"
	metricLogSyncSize = "LogSyncSize"
	metricSystem      = "System"
)

var (
	metricCounter = hmetrics.RegisterCounterMap(
		"counter",
		"The General Counter Metric",
	)

	metricGauge = hmetrics.RegisterGaugeMap(
		"gauge",
		"The General Gauge Metric",
	)

	metricLatency = hmetrics.RegisterHistogramMap(
		"latency",
		"The General Latency Metric",
		hmetrics.NewBuckets(0.0001, 1.5, 36),
	)

	metricHistogram = hmetrics.RegisterHistogramMap(
		"histogram",
		"The General Histogram Metric",
		hmetrics.NewBuckets(0.0001, 1.5, 36),
	)

	metricComplex = hmetrics.RegisterComplexMap(
		"complex",
		"The General Complex Metric",
		hmetrics.NewBuckets(0.0001, 1.5, 36),
	)
)
