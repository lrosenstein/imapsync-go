package client

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/emersion/go-imap"
)

func TestFallbackSyncKey(t *testing.T) {
	t.Parallel()

	t.Run("stable across calls", func(t *testing.T) {
		t.Parallel()
		k1 := fallbackSyncKey("alice@example.com", "Mon, 01 Jan 2024 12:00:00 +0000", "Hello", 1024)
		k2 := fallbackSyncKey("alice@example.com", "Mon, 01 Jan 2024 12:00:00 +0000", "Hello", 1024)
		if k1 != k2 {
			t.Errorf("fallbackSyncKey not stable: %q vs %q", k1, k2)
		}
	})

	t.Run("has fallback prefix", func(t *testing.T) {
		t.Parallel()
		k := fallbackSyncKey("a@b.c", "now", "subject", 42)
		if !strings.HasPrefix(k, fallbackKeyPrefix) {
			t.Errorf("key %q does not start with %q", k, fallbackKeyPrefix)
		}
	})

	t.Run("distinct headers produce distinct keys", func(t *testing.T) {
		t.Parallel()
		k1 := fallbackSyncKey("alice@example.com", "Mon, 01 Jan 2024 12:00:00 +0000", "Hello", 1024)
		k2 := fallbackSyncKey("bob@example.com", "Mon, 01 Jan 2024 12:00:00 +0000", "Hello", 1024)
		if k1 == k2 {
			t.Errorf("different From produced same key: %q", k1)
		}
	})

	t.Run("distinct size produces distinct key", func(t *testing.T) {
		t.Parallel()
		k1 := fallbackSyncKey("alice@example.com", "Mon, 01 Jan 2024 12:00:00 +0000", "Hello", 1024)
		k2 := fallbackSyncKey("alice@example.com", "Mon, 01 Jan 2024 12:00:00 +0000", "Hello", 2048)
		if k1 == k2 {
			t.Errorf("different size produced same key: %q", k1)
		}
	})

	t.Run("null-byte separator prevents cross-field collisions", func(t *testing.T) {
		t.Parallel()
		// "AB" + "" vs "A" + "B" must differ because the null separator
		// is between every field.
		k1 := fallbackSyncKey("AB", "", "", 0)
		k2 := fallbackSyncKey("A", "B", "", 0)
		if k1 == k2 {
			t.Errorf("cross-field collision: %q == %q", k1, k2)
		}
	})
}

func TestParseMessageID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "angle brackets",
			in:   "Message-Id: <abc@example.com>\r\n\r\n",
			want: "abc@example.com",
		},
		{
			name: "no brackets",
			in:   "Message-Id: bare-id@example.com\r\n\r\n",
			want: "bare-id@example.com",
		},
		{
			name: "case-insensitive header",
			in:   "MESSAGE-ID: <id@host>\r\n\r\n",
			want: "id@host",
		},
		{
			name: "lowercase header",
			in:   "message-id: <id@host>\r\n\r\n",
			want: "id@host",
		},
		{
			name: "missing terminator (defensive append)",
			in:   "Message-Id: <id@host>\r\n",
			want: "id@host",
		},
		{
			name: "no Message-Id present",
			in:   "Subject: hi\r\n\r\n",
			want: "",
		},
		{
			name: "empty",
			in:   "",
			want: "",
		},
		{
			name: "garbage",
			in:   "not a valid header\r\n",
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := parseMessageID(strings.NewReader(tt.in))
			if got != tt.want {
				t.Errorf("parseMessageID(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestTrimAngleBrackets(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"<abc>":    "abc",
		"abc":      "abc",
		"<abc":     "<abc",
		"abc>":     "abc>",
		"":         "",
		"<>":       "",
		"<a@b.c>":  "a@b.c",
		"<<nest>>": "<nest>",
	}
	for in, want := range cases {
		if got := trimAngleBrackets(in); got != want {
			t.Errorf("trimAngleBrackets(%q) = %q, want %q", in, got, want)
		}
	}
}

// Test_StreamMessagesByUIDs_batchesAt500 asserts that StreamMessagesByUIDs
// splits a 1001-UID slice into exactly 3 UID FETCH commands (500+500+1).
func Test_StreamMessagesByUIDs_batchesAt500(t *testing.T) {
	t.Parallel()

	srv := newFakeServer(t)
	c := newClientWithFake(t, srv)
	c.mailboxCache = mailboxCache{
		folders:   map[string]struct{}{"INBOX": {}},
		delimiter: "/",
		loaded:    true,
	}

	uids := make([]uint32, 1001)
	for i := range uids {
		uids[i] = uint32(i + 1)
	}

	err := c.StreamMessagesByUIDs(context.Background(), "INBOX", uids, func(_ *imap.Message) error {
		return nil
	})
	if err != nil {
		t.Fatalf("StreamMessagesByUIDs: %v", err)
	}

	if got := srv.callCount("UID FETCH"); got != 3 {
		t.Errorf("UID FETCH count = %d, want 3 (batches of 500+500+1)", got)
	}
}

// Test_FetchMessageMap_usesFallbackKeyWhenMessageIdMissing asserts that
// FetchMessageMap assigns a fallback key to messages lacking Message-Id
// (instead of skipping them), logs the count, and the fallback key starts
// with fallbackKeyPrefix.
func Test_FetchMessageMap_usesFallbackKeyWhenMessageIdMissing(t *testing.T) {
	t.Parallel()

	srv := newFakeServer(t)
	srv.addConnHandler(fetchTwoMessagesHandler(srv))

	c := newClientWithFake(t, srv)
	c.mailboxCache = mailboxCache{
		folders:   map[string]struct{}{"INBOX": {}},
		delimiter: "/",
		loaded:    true,
	}

	var logged []string
	c.SetProgressWriter(&logCapture{fn: func(msg string) { logged = append(logged, msg) }})
	c.verbose = true

	result, totalSize, err := c.FetchMessageMap(context.Background(), "INBOX")
	if err != nil {
		t.Fatalf("FetchMessageMap: %v", err)
	}
	// Both messages are now in the map: one under its real Message-Id, one under
	// the fallback key.
	if len(result) != 2 {
		t.Errorf("returned map has %d entries, want 2 (1 real + 1 fallback)", len(result))
	}
	if _, ok := result["ok@host"]; !ok {
		t.Errorf("expected key ok@host in map, got %v", result)
	}
	var hasFallback bool
	for k := range result {
		if strings.HasPrefix(k, fallbackKeyPrefix) {
			hasFallback = true
		}
	}
	if !hasFallback {
		t.Errorf("expected a fallback: key in map, got keys: %v", result)
	}
	if totalSize == 0 {
		t.Error("totalSize = 0, want > 0 — RFC822.SIZE not aggregated from fake server response")
	}

	var foundLog bool
	for _, msg := range logged {
		if strings.Contains(msg, "1 message(s) without Message-Id matched via fallback key") {
			foundLog = true
			break
		}
	}
	if !foundLog {
		t.Errorf("expected log containing 'matched via fallback key'; got: %v", logged)
	}
}

// Test_FetchMessageIDSet_returnsIDsWithoutUIDs asserts that FetchMessageIDSet
// wraps FetchMessageMap correctly: it returns keys (real Message-Ids and
// fallback keys) without UIDs.
func Test_FetchMessageIDSet_returnsIDsWithoutUIDs(t *testing.T) {
	t.Parallel()

	srv := newFakeServer(t)
	srv.addConnHandler(fetchTwoMessagesHandler(srv))

	c := newClientWithFake(t, srv)
	c.mailboxCache = mailboxCache{
		folders:   map[string]struct{}{"INBOX": {}},
		delimiter: "/",
		loaded:    true,
	}

	result, err := c.FetchMessageIDSet(context.Background(), "INBOX")
	if err != nil {
		t.Fatalf("FetchMessageIDSet: %v", err)
	}
	// Now 2 entries: one real Message-Id plus one fallback key.
	if len(result) != 2 {
		t.Errorf("FetchMessageIDSet returned %d entries, want 2 (1 real + 1 fallback)", len(result))
	}
	if _, ok := result["ok@host"]; !ok {
		t.Errorf("expected key ok@host in set, got %v", result)
	}
}

// logCapture implements ProgressWriter for tests.
type logCapture struct {
	fn func(string)
}

func (l *logCapture) Log(msg string, a ...any) {
	l.fn(fmt.Sprintf(msg, a...))
}

// fetchTwoMessagesHandler returns a per-connection handler that:
//   - responds to the first FETCH (seq-based) with 2 messages: one with a
//     Message-Id and one without;
//   - responds to the subsequent UID FETCH (fallback headers) for UID 2 with
//     From/Date/Subject headers so the fallback key can be computed.
func fetchTwoMessagesHandler(srv *fakeServer) func(net.Conn) {
	return func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		_, _ = fmt.Fprintf(conn, "* OK [CAPABILITY IMAP4rev1] fake ready\r\n")
		sc := bufio.NewScanner(conn)
		for sc.Scan() {
			line := sc.Text()
			if line == "" {
				continue
			}
			parts := strings.SplitN(line, " ", 3)
			if len(parts) < 2 {
				continue
			}
			tag, verb := parts[0], strings.ToUpper(parts[1])
			srv.mu.Lock()
			srv.counts[verb]++
			srv.mu.Unlock()
			arg := ""
			if len(parts) == 3 {
				arg = parts[2]
			}
			switch verb {
			case "LOGIN":
				_, _ = fmt.Fprintf(conn, "%s OK LOGIN completed\r\n", tag)
			case "SELECT", "EXAMINE":
				_, _ = fmt.Fprintf(conn, "* 2 EXISTS\r\n")
				_, _ = fmt.Fprintf(conn, "* 0 RECENT\r\n")
				_, _ = fmt.Fprintf(conn, "%s OK [READ-ONLY] %s completed\r\n", tag, verb)
			case "STATUS":
				mboxName := strings.Trim(strings.SplitN(arg, " ", 2)[0], `"`)
				_, _ = fmt.Fprintf(conn, "* STATUS %s (MESSAGES 2)\r\n", mboxName)
				_, _ = fmt.Fprintf(conn, "%s OK STATUS completed\r\n", tag)
			case "FETCH":
				writeFetchResponses(conn)
				_, _ = fmt.Fprintf(conn, "%s OK FETCH completed\r\n", tag)
			case "UID":
				srv.mu.Lock()
				srv.counts["UID FETCH"]++
				srv.mu.Unlock()
				// The fallback path issues UID FETCH for From/Date/Subject on UID 2.
				writeFallbackFetchResponse(conn)
				_, _ = fmt.Fprintf(conn, "%s OK UID FETCH completed\r\n", tag)
			case "LOGOUT":
				_, _ = fmt.Fprintf(conn, "* BYE Logging out\r\n")
				_, _ = fmt.Fprintf(conn, "%s OK LOGOUT completed\r\n", tag)
				return
			default:
				_, _ = fmt.Fprintf(conn, "%s OK %s completed\r\n", tag, verb)
			}
		}
	}
}

// writeFetchResponses emits two IMAP FETCH responses:
//   - message 1 with Message-Id: <ok@host> and RFC822.SIZE 1024
//   - message 2 with an empty Message-Id header and RFC822.SIZE 512
func writeFetchResponses(conn net.Conn) {
	hdr1 := "Message-Id: <ok@host>\r\n\r\n"
	_, _ = fmt.Fprintf(conn,
		"* 1 FETCH (UID 1 RFC822.SIZE 1024 BODY[HEADER.FIELDS (\"MESSAGE-ID\")] {%d}\r\n%s)\r\n",
		len(hdr1), hdr1,
	)

	hdr2 := "\r\n"
	_, _ = fmt.Fprintf(conn,
		"* 2 FETCH (UID 2 RFC822.SIZE 512 BODY[HEADER.FIELDS (\"MESSAGE-ID\")] {%d}\r\n%s)\r\n",
		len(hdr2), hdr2,
	)
}

// writeFallbackFetchResponse emits the UID FETCH response for the fallback
// header pass: UID 2 with From/Date/Subject.
func writeFallbackFetchResponse(conn net.Conn) {
	hdr := "From: sender@example.com\r\nDate: Mon, 01 Jan 2024 12:00:00 +0000\r\nSubject: No ID\r\n\r\n"
	_, _ = fmt.Fprintf(conn,
		"* 2 FETCH (UID 2 BODY[HEADER.FIELDS (\"FROM\" \"DATE\" \"SUBJECT\")] {%d}\r\n%s)\r\n",
		len(hdr), hdr,
	)
}

// Test_FetchMessageMap_allEmptyHeadersLogsWarning asserts that a message with
// no Message-Id AND no From/Date/Subject triggers the extra ⚠️ warning log.
func Test_FetchMessageMap_allEmptyHeadersLogsWarning(t *testing.T) {
	t.Parallel()

	srv := newFakeServer(t)
	srv.addConnHandler(fetchTwoMessagesAllEmptyFallbackHandler(srv))

	c := newClientWithFake(t, srv)
	c.mailboxCache = mailboxCache{
		folders:   map[string]struct{}{"INBOX": {}},
		delimiter: "/",
		loaded:    true,
	}

	var logged []string
	c.SetProgressWriter(&logCapture{fn: func(msg string) { logged = append(logged, msg) }})

	_, _, err := c.FetchMessageMap(context.Background(), "INBOX")
	if err != nil {
		t.Fatalf("FetchMessageMap: %v", err)
	}

	var foundWarn bool
	for _, msg := range logged {
		if strings.Contains(msg, "fallback key relies on size only") {
			foundWarn = true
			break
		}
	}
	if !foundWarn {
		t.Errorf("expected warning about all-empty fallback headers; got: %v", logged)
	}
}

// fetchTwoMessagesAllEmptyFallbackHandler is like fetchTwoMessagesHandler but
// the UID FETCH fallback response returns completely empty headers for UID 2,
// triggering the all-empty degenerate warning.
func fetchTwoMessagesAllEmptyFallbackHandler(srv *fakeServer) func(net.Conn) {
	return func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		_, _ = fmt.Fprintf(conn, "* OK [CAPABILITY IMAP4rev1] fake ready\r\n")
		sc := bufio.NewScanner(conn)
		for sc.Scan() {
			line := sc.Text()
			if line == "" {
				continue
			}
			parts := strings.SplitN(line, " ", 3)
			if len(parts) < 2 {
				continue
			}
			tag, verb := parts[0], strings.ToUpper(parts[1])
			srv.mu.Lock()
			srv.counts[verb]++
			srv.mu.Unlock()
			arg := ""
			if len(parts) == 3 {
				arg = parts[2]
			}
			switch verb {
			case "LOGIN":
				_, _ = fmt.Fprintf(conn, "%s OK LOGIN completed\r\n", tag)
			case "SELECT", "EXAMINE":
				_, _ = fmt.Fprintf(conn, "* 2 EXISTS\r\n")
				_, _ = fmt.Fprintf(conn, "* 0 RECENT\r\n")
				_, _ = fmt.Fprintf(conn, "%s OK [READ-ONLY] %s completed\r\n", tag, verb)
			case "STATUS":
				mboxName := strings.Trim(strings.SplitN(arg, " ", 2)[0], `"`)
				_, _ = fmt.Fprintf(conn, "* STATUS %s (MESSAGES 2)\r\n", mboxName)
				_, _ = fmt.Fprintf(conn, "%s OK STATUS completed\r\n", tag)
			case "FETCH":
				writeFetchResponses(conn)
				_, _ = fmt.Fprintf(conn, "%s OK FETCH completed\r\n", tag)
			case "UID":
				srv.mu.Lock()
				srv.counts["UID FETCH"]++
				srv.mu.Unlock()
				// Empty headers — triggers the all-empty fallback warning.
				emptyHdr := "\r\n"
				_, _ = fmt.Fprintf(conn,
					"* 2 FETCH (UID 2 BODY[HEADER.FIELDS (\"FROM\" \"DATE\" \"SUBJECT\")] {%d}\r\n%s)\r\n",
					len(emptyHdr), emptyHdr,
				)
				_, _ = fmt.Fprintf(conn, "%s OK UID FETCH completed\r\n", tag)
			case "LOGOUT":
				_, _ = fmt.Fprintf(conn, "* BYE Logging out\r\n")
				_, _ = fmt.Fprintf(conn, "%s OK LOGOUT completed\r\n", tag)
				return
			default:
				_, _ = fmt.Fprintf(conn, "%s OK %s completed\r\n", tag, verb)
			}
			_ = arg
		}
	}
}
