package input

import (
	"fmt"
	"log/slog"
	"loglang/loglang"
	"net"
)

// test with echo -n test | nc -u -w0 localhost 9999
func UdpListener(name string, eventType string, port int, codec loglang.CodecPlugin) loglang.InputPlugin {
	return loglang.InputPlugin{
		Name: name,
		Type: eventType,
		Run: func(events chan loglang.Event) error {
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

			var buf [4096]byte
			for {
				// UDP is not stream based, so we read each individual datagram
				rlen, _, err := conn.ReadFromUDP(buf[:])
				if err != nil {
					return err
				}
				evt, err := codec.Decode(buf[:rlen])
				if err != nil {
					slog.Error(fmt.Errorf("lost datagram: %w", err).Error())
					continue
				}
				events <- evt
			}
		},
	}
}
