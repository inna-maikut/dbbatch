package dbbatch

type options struct {
	withoutCancel bool
}

type Option func(*options)

// WithoutCancel protects all DB methods from cancelling during request
// Doesn't work for Stmt and Tx
func WithoutCancel(val bool) Option {
	return func(o *options) {
		o.withoutCancel = val
	}
}
