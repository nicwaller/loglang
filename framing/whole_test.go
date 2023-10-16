package framing

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestWhole_Extract(t *testing.T) {
	//	func (p *whole) Extract(_ context.Context, streams <-chan io.Reader, out chan<- io.Reader) error {
	ctx := context.Background()
	streams := make(chan io.Reader)
	go func() {
		streams <- strings.NewReader("Hello\nGoodbye\n")
		close(streams)
	}()

	out := make(chan io.Reader, 1)

	err := Whole().Extract(ctx, streams, out)
	if err != nil {
		t.Error(err)
	}

	onlyReader := <-out
	dat, err := io.ReadAll(onlyReader)
	if err != nil {
		t.Error(err)
	}

	const expected = "Hello\nGoodbye\n"
	if expected != string(dat) {
		t.Errorf(`Expected "%s" but got "%s"`, expected, dat)
	}

}
