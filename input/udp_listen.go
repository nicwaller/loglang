package input

import (
	"bytes"
	"fmt"
	"github.com/nicwaller/loglang"
	"log/slog"
	"net"
	"time"
)

// test with echo -n test | nc -u -w0 localhost 9999
func UdpListener(name string, eventType string, port int, framer loglang.FramingPlugin, codec loglang.CodecPlugin) loglang.InputPlugin {
	return loglang.InputPlugin{
		Name: name,
		Type: eventType,
		Run: func(send chan loglang.Event) error {
			slog.Debug(fmt.Sprintf("UDP listener starting on %s:%d", name, port),
				"server.port", port, "log.logger", name,
			)
			addr := net.UDPAddr{
				Port: port,
				IP:   net.ParseIP("0.0.0.0"),
			}
			conn, err := net.ListenUDP("udp", &addr) // code does not block here
			if err != nil {
				return err
			}
			defer func(conn *net.UDPConn) {
				_ = conn.Close()
			}(conn)

			for {
				var buf [4096]byte
				// UDP is not stream based, so we read each individual datagram
				rlen, addr, err := conn.ReadFromUDP(buf[:])
				if rlen == 1 {
					// FIXME : wtf is this X
					continue
				}
				if err != nil {
					return err
				}
				slog.Debug(fmt.Sprintf("got UDP datagram of %d bytes", rlen))

				frames := make(chan []byte)
				go func() {
					err := framer.Run(bytes.NewReader(buf[:rlen]), frames)
					if err != nil {
						slog.Error(err.Error())
					}
				}()
				go func() {
					for {
						select {
						case frame := <-frames:
							slog.Debug(fmt.Sprintf("got a frame of %d bytes", len(frame)))
							evt, err := codec.Decode(frame)
							// TODO: populate more ECS-style fields, if option is enabled
							evt.Field("client.address").SetString(addr.String())
							if err != nil {
								slog.Error(fmt.Errorf("lost whole datagram or part of datagram: %w", err).Error())
							} else {
								send <- evt
							}
						case <-time.After(30 * time.Second):
							return
						}

					}
				}()
			}
		},
	}
}
