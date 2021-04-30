package grpc

import (
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

type Server struct {
	store *Store
}

func MustStart() *Server {
	return nil
}

func (s *Server) DependencyReader() dependencystore.Reader {
	return s.store
}

func (s *Server) SpanReader() spanstore.Reader {
	return s.store
}

func (s *Server) SpanWriter() spanstore.Writer {
	return s.store
}
