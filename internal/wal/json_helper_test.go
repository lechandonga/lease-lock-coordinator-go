package wal

import "encoding/json"

func jsonUnmarshal(p []byte, v any) error { return json.Unmarshal(p, v) }
