package kvgo

import (
	"github.com/hooto/hmetrics"
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
