package store

import (
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

func (s *Store) DependencyReader() dependencystore.Reader {
	return s
}

func (s *Store) SpanReader() spanstore.Reader {
	return s
}

func (s *Store) SpanWriter() spanstore.Writer {
	return s
}
