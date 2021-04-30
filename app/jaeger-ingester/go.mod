module github.com/faceair/VictoriaTraces/app/jaeger-ingester

go 1.16

require (
	github.com/VictoriaMetrics/VictoriaMetrics v1.53.1-cluster
	github.com/VictoriaMetrics/metrics v1.13.1
	github.com/faceair/VictoriaTraces v0.0.0
	github.com/jaegertracing/jaeger v1.21.0
)

replace github.com/faceair/VictoriaTraces v0.0.0 => ../../
