package framing

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestLines_Extract(t *testing.T) {
	ctx := context.Background()
	streams := make(chan io.Reader)
	go func() {
		streams <- strings.NewReader("Hello\nGoodbye\n")
		close(streams)
	}()

	out := make(chan io.Reader, 2)

	err := Lines().Extract(ctx, streams, out)
	if err != nil {
		t.Error(err)
	}

	line1 := <-out
	dat1, err := io.ReadAll(line1)
	if err != nil {
		t.Error(err)
	}

	line2 := <-out
	dat2, err := io.ReadAll(line2)
	if err != nil {
		t.Error(err)
	}

	const expected1 = "Hello"
	const expected2 = "Goodbye"
	if expected1 != string(dat1) {
		t.Errorf(`Expected "%s" but got "%s"`, expected1, dat1)
	}
	if expected2 != string(dat2) {
		t.Errorf(`Expected "%s" but got "%s"`, expected2, dat2)
	}
}
