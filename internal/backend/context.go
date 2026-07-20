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

// reachedPoolKey is the context slot for the "did this request name a valid
// pool?" marker. It is separate from ctxKey so the two never collide.
type reachedPoolKey struct{}

// WithReachedPoolMarker returns a copy of ctx carrying flag, a pointer the
// outer activity.Middleware installs before calling into the resolver.
// Middleware flips *flag true via MarkReachedPool once a request resolves to
// a valid pool, letting activity record only requests eligible to reach an
// upstream — dropping unknown-selector 403s (favicon, browser probes,
// misconfigured clients) that fail closed at the boundary and are always
// errors, hence pure noise in the panel (issue #230). The marker lives in
// this package (not activity) so backend need not import activity.
func WithReachedPoolMarker(ctx context.Context, flag *bool) context.Context {
	return context.WithValue(ctx, reachedPoolKey{}, flag)
}

// MarkReachedPool sets the marker installed by WithReachedPoolMarker, if one
// is present. It is a no-op when absent — the resolver middleware runs
// correctly outside the activity chain (tests, or a different wiring).
func MarkReachedPool(ctx context.Context) {
	if flag, ok := ctx.Value(reachedPoolKey{}).(*bool); ok {
		*flag = true
	}
}
