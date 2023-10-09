package codec

import (
	"bytes"
	"fmt"
	"github.com/nicwaller/loglang"
	"os"
	"strings"
	"time"
)

// Syslog V0
// https://datatracker.ietf.org/doc/html/rfc3164
// I hate this format - NW

// Syslog V1
// https://datatracker.ietf.org/doc/html/rfc5424

func SyslogV0() loglang.CodecPlugin {
	return loglang.CodecPlugin{
		Encode: func(evt loglang.Event) ([]byte, error) {
			buf := bytes.NewBufferString("")

			// PART 1 - PRI
			// TODO: maybe also look at [level]? coalesce?
			facility := int8(1) // user-level
			// TODO: read from [log][syslog][facility][code]
			severityStr := loglang.Coalesce(
				evt.Field("log.level").GetString(),
				evt.Field("level").GetString(),
			).(string)
			severityStr = strings.ToLower(severityStr)
			severity, _ := syslogSeverityReverse[severityStr]
			priority := syslogPriority(facility, severity)
			buf.WriteString(fmt.Sprintf("<%d>", priority))

			// PART 2 - HEADER
			buf.WriteString(syslogHeader())

			// PART 3 - MESSAGE
			tag := "" // is there a best field for tag?
			content := evt.Field("message").GetString()
			buf.WriteString(syslogMessage(tag, content))

			// total length of packet must be 1024 or less
			// relay MUST truncate the packet to be 1024 bytes
			leng := min(buf.Len(), 1024)
			return buf.Bytes()[:leng], nil
		},
		Decode: func(bytes []byte) (loglang.Event, error) {
			panic("I hate this format")
			// use this in decoding
			// facilityName := syslogFacility[facility]
			// log.syslog.hostname
			// log.syslog.facility.name
			// log.syslog.priority
			// log.syslog.severity.code
			// log.syslog.severity.name
			// log.syslog.version
			// https://www.elastic.co/guide/en/ecs/master/ecs-log.html
			return loglang.NewEvent(), fmt.Errorf("not yet implemented")
		},
	}
}

// The Priority value is calculated by first multiplying the Facility
// number by 8 and then adding the numerical value of the Severity. For
// example, a kernel message (Facility=0) with a Severity of Emergency
// (Severity=0) would have a Priority value of 0.  Also, a "local use 4"
// message (Facility=20) with a Severity of Notice (Severity=5) would
// have a Priority value of 165.
func syslogPriority(facility int8, severity int8) int8 {
	return facility*8 + severity
}

func syslogHeader() string {
	//Timestamp = Mmm dd hh:mm:ss
	timestamp := time.Now().Format("Jan _2 15:04:05")
	hostname, err := os.Hostname()
	if err != nil {
		// TODO: discover IPv4 or IPv6 address
		hostname = "127.0.0.1"
	}

	// hostname must not contain any embedded spaces
	hostname = strings.ReplaceAll(hostname, " ", "-")

	//A single space character MUST also follow the HOSTNAME field.
	return fmt.Sprintf("%s %s ", timestamp, hostname)
}

// The MSG part has two fields known as the TAG field and the CONTENT
// field.  The value in the TAG field will be the name of the program or
// process that generated the message.  The CONTENT contains the details
// of the message.  This has traditionally been a freeform message that
// gives some detailed information of the event.  The TAG is a string of
// ABNF alphanumeric characters that MUST NOT exceed 32 characters.  Any
// non-alphanumeric character will terminate the TAG field and will be
// assumed to be the starting character of the CONTENT field.  Most
// commonly, the first character of the CONTENT field that signifies the
// conclusion of the TAG field has been seen to be the left square
// bracket character ("["), a colon character (":"), or a space
// character.  This is explained in more detail in Section 5.3.
func syslogMessage(tag string, content string) string {
	if tag == "" {
		return content
	}
	return fmt.Sprintf("%s: %s", tag, content)
}

var syslogFacility = map[int]string{
	0:  "kernel",
	1:  "user",
	2:  "mail",
	3:  "system",
	4:  "security",
	5:  "syslogd",
	6:  "line printer",
	7:  "network news",
	8:  "uucp",
	9:  "clock",
	10: "security",
	11: "ftp",
	12: "ntp",
	13: "audit",
	14: "alert",
	15: "clock",
}

var syslogSeverity = map[int8]string{
	0: "emergency", // system is unusable
	1: "alert",     // action must be taken immediately
	2: "critical",
	3: "error",
	4: "warning",
	5: "notice", // normal but significant
	6: "informational",
	7: "debug",
}

var syslogSeverityReverse = map[string]int8{
	"emergency":     0, // system is unusable
	"fatal":         0, // system is unusable
	"alert":         1, // action must be taken immediately
	"critical":      2,
	"error":         3,
	"warning":       4,
	"warn":          4,
	"notice":        5, // normal but significant
	"info":          6,
	"informational": 6,
	"debug":         7,
	"trace":         7,
}
