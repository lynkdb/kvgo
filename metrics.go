package kvgo

import (
	"github.com/hooto/metrics"
)

var (
	metricsCounter = metrics.RegisterCounterMap(
		"counter",
		"The General Counter Metric",
	)

	metricsGauge = metrics.RegisterGaugeMap(
		"counter",
		"The General Gauge Metric",
	)

	metricsHistogram = metrics.RegisterHistogramMap(
		"histogram",
		"The General Histogram Metric",
		metrics.NewBuckets(0.0001, 1.5, 36),
	)

	metricsComplex = metrics.RegisterComplexMap(
		"complex",
		"The General Complex Metric",
		metrics.NewBuckets(0.0001, 1.5, 36),
	)
)
