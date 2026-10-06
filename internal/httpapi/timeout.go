package httpapi

import (
	"context"
	"net/http"
	"time"
)

func contextWithTimeout(r *http.Request, waitMillis int64) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), time.Duration(waitMillis)*time.Millisecond)
}
