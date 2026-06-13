package backend

import "context"

// ctxKey is unexported so no other package can collide with our context
// value.
type ctxKey struct{}

// WithBackend returns a copy of ctx carrying b, for the proxy director
// and quota observer to read after the resolver middleware runs.
func WithBackend(ctx context.Context, b Backend) context.Context {
	return context.WithValue(ctx, ctxKey{}, b)
}

// FromContext returns the backend stored by WithBackend. ok is false
// when no backend was resolved for the request.
func FromContext(ctx context.Context) (Backend, bool) {
	b, ok := ctx.Value(ctxKey{}).(Backend)
	return b, ok
}
