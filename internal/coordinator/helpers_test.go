package coordinator

import "runtime"

func runtimeGosched() { runtime.Gosched() }
