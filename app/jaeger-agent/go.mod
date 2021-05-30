module github.com/faceair/VictoriaTraces/app/jaeger-agent

go 1.16

require (
	github.com/VictoriaMetrics/VictoriaMetrics v1.53.1-cluster
	github.com/VictoriaMetrics/metrics v1.13.1
	github.com/faceair/VictoriaTraces v0.0.0
	github.com/jaegertracing/jaeger v1.22.0
	gopkg.in/yaml.v2 v2.4.0
)

replace (
	github.com/faceair/VictoriaTraces v0.0.0 => ../../
	google.golang.org/grpc v1.35.0 => google.golang.org/grpc v1.29.1
)
