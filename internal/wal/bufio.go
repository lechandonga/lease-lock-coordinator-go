package wal

import (
	"bufio"
	"io"
)

var readerPool = make(chan *bufio.Reader, 8)

func bufioReader(r io.Reader) *bufio.Reader {
	select {
	case br := <-readerPool:
		br.Reset(r)
		return br
	default:
		return bufio.NewReaderSize(r, 64*1024)
	}
}

func putBufioReader(br *bufio.Reader) {
	select {
	case readerPool <- br:
	default:
	}
}
