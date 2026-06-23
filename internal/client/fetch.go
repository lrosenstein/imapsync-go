package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net/mail"
	"slices"

	"github.com/emersion/go-imap"
	imapclient "github.com/emersion/go-imap/client"
)

// messageIDHeaderSection is the smallest fetch we can ask for that still
// gives us what we need for diffing: just the Message-Id header plus the
// terminating CRLF, with PEEK so the source's \Seen flag is unchanged.
//
// Every Fetch method below uses this section instead of FetchEnvelope. On
// large folders this is dramatically less data on the wire — Envelope also
// pulls From/To/Cc/Bcc/Subject/Date/References/InReplyTo, which we don't
// need for the planning diff.
var messageIDHeaderSection = &imap.BodySectionName{
	BodyPartName: imap.BodyPartName{
		Specifier: imap.HeaderSpecifier,
		Fields:    []string{"Message-Id"},
	},
	Peek: true,
}

// fallbackHeaderSection fetches only the three headers used to build a
// fallback sync key when Message-Id is absent. Requested via a separate
// UID FETCH targeting only the subset of messages that lacked Message-Id,
// so the common case (messages with Message-Id) incurs zero extra wire cost.
var fallbackHeaderSection = &imap.BodySectionName{
	BodyPartName: imap.BodyPartName{
		Specifier: imap.HeaderSpecifier,
		Fields:    []string{"From", "Date", "Subject"},
	},
	Peek: true,
}

// fallbackKeyPrefix makes fallback keys disjoint from real Message-Id strings,
// which always contain "@" (RFC 5322) and never start with this prefix.
const fallbackKeyPrefix = "fallback:"

// fullBodyPeekSection requests the entire RFC822 body without flipping the
// \Seen flag on the source. This matters: a sync tool must not mutate the
// source mailbox state. The previous implementation used FetchRFC822 which
// is functionally equivalent to BODY[] (RFC 3501 §6.4.5) and *does* mark
// messages as read.
var fullBodyPeekSection = &imap.BodySectionName{Peek: true}

// FetchMessageMap returns sync-key → UID for every message in folder, plus
// the sum of RFC822.SIZE across all messages.
//
// One pass fetches Message-Id + UID + RFC822.SIZE for every message. Messages
// with a Message-Id use that as their key. For messages without a Message-Id a
// second targeted UID FETCH retrieves From/Date/Subject and the sync key
// becomes SHA-256(From||0||Date||0||Subject||0||RFC822.SIZE) prefixed with
// "fallback:". The second fetch only covers the missing-id subset, so the
// common case incurs zero extra wire cost.
//
// If both Message-Id and all fallback headers are absent the fallback key
// contains size only; two such messages in the same folder will collide and
// only one will sync — this degenerate case is logged separately.
func (c *Client) FetchMessageMap(ctx context.Context, folder string) (map[string]uint32, uint64, error) {
	stop := c.withCancel(ctx)
	defer stop()

	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}

	c.log("[%s] Fetching folder %s...", c.prefix, folder)

	var (
		ids                 map[string]uint32
		totalSize           uint64
		missingUIDs         []uint32
		missingSize         map[uint32]uint64
		fallbackCount       int
		fallbackAllEmpty    int
	)
	err := c.safeCall(func(cli *imapclient.Client) error {
		ids = nil
		totalSize = 0
		missingUIDs = missingUIDs[:0]
		missingSize = nil
		mbox, err := c.selectIfNeeded(cli, folder)
		if err != nil {
			return fmt.Errorf("[%s] cannot select folder %s: %w", c.prefix, folder, err)
		}
		// selectIfNeeded skips the round-trip when the folder is already
		// selected; it returns nil mbox in that case. Fall back to a STATUS
		// for the message count, which is cheap.
		var total uint32
		if mbox != nil {
			total = mbox.Messages
		} else {
			st, serr := cli.Status(folder, []imap.StatusItem{imap.StatusMessages})
			if serr != nil {
				return fmt.Errorf("[%s] status %s: %w", c.prefix, folder, serr)
			}
			total = st.Messages
		}
		c.log("[%s] Selected folder %s (%d messages)", c.prefix, folder, total)
		if total == 0 {
			ids = make(map[string]uint32)
			return nil
		}
		c.log("[%s] Fetching %d message IDs from %s...", c.prefix, total, folder)

		ids = make(map[string]uint32, total)
		seqset := new(imap.SeqSet)
		seqset.AddRange(1, total)
		messages := make(chan *imap.Message, messageChanBuffer)
		done := make(chan error, 1)
		// RFC822.SIZE is part of the same FETCH so the size total comes
		// free with the diff scan — no extra round-trip per folder.
		items := []imap.FetchItem{messageIDHeaderSection.FetchItem(), imap.FetchUid, imap.FetchRFC822Size}
		go func() { done <- cli.Fetch(seqset, items, messages) }()

		for msg := range messages {
			if ctx.Err() != nil {
				continue
			}
			totalSize += uint64(msg.Size)
			id := readMessageIDHeader(msg)
			if id == "" {
				// Defer to a second targeted fetch; size is already captured.
				if missingSize == nil {
					missingSize = make(map[uint32]uint64)
				}
				missingUIDs = append(missingUIDs, msg.Uid)
				missingSize[msg.Uid] = uint64(msg.Size)
				continue
			}
			ids[id] = msg.Uid
		}
		if err := <-done; err != nil {
			return fmt.Errorf("[%s] fetch IDs: %w", c.prefix, err)
		}

		if len(missingUIDs) > 0 {
			slices.Sort(missingUIDs)
			var ferr error
			fallbackAllEmpty, ferr = c.fetchFallbackKeys(ctx, cli, missingUIDs, missingSize, ids)
			if ferr != nil {
				return ferr
			}
			fallbackCount = len(missingUIDs)
		}
		return nil
	})

	if err == nil {
		if pw := c.progressWriter(); pw != nil {
			if fallbackCount > 0 {
				pw.Log("[%s] %s: %d message(s) without Message-Id matched via fallback key (From/Date/Subject/size)",
					c.prefix, folder, fallbackCount)
			}
			if fallbackAllEmpty > 0 {
				pw.Log("[%s] ⚠️  %s: %d message(s) had no Message-Id, From, Date, or Subject — fallback key relies on size only and may collide",
					c.prefix, folder, fallbackAllEmpty)
			}
		}
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, 0, ctx.Err()
		}
		return nil, 0, err
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	return ids, totalSize, nil
}

// FetchMessageIDSet returns the set of Message-Ids in folder, dropping the
// UIDs. Convenience wrapper around FetchMessageMap for the destination side
// of a sync where UIDs are irrelevant.
func (c *Client) FetchMessageIDSet(ctx context.Context, folder string) (map[string]struct{}, error) {
	m, _, err := c.FetchMessageMap(ctx, folder)
	if err != nil {
		return nil, err
	}
	out := make(map[string]struct{}, len(m))
	for id := range m {
		out[id] = struct{}{}
	}
	return out, nil
}

// fetchFallbackKeys issues a second UID FETCH for From/Date/Subject on the
// given UIDs (messages whose Message-Id was absent) and stores a derived sync
// key into ids. sizes supplies the RFC822.SIZE already captured on the first
// pass — including it in the hash distinguishes messages with identical visible
// headers but different bodies (e.g. bulk notifications received the same
// second). Batched at uidFetchBatchSize for the same reason as
// StreamMessagesByUIDs. Returns the count of messages where all three header
// fields were also empty (degenerate case: key hashes size only).
func (c *Client) fetchFallbackKeys(ctx context.Context, cli *imapclient.Client, uids []uint32, sizes map[uint32]uint64, ids map[string]uint32) (allEmpty int, err error) {
	for start := 0; start < len(uids); start += uidFetchBatchSize {
		if err := ctx.Err(); err != nil {
			return allEmpty, err
		}
		end := min(start+uidFetchBatchSize, len(uids))
		batch := uids[start:end]

		uidSet := new(imap.SeqSet)
		for _, uid := range batch {
			uidSet.AddNum(uid)
		}
		messages := make(chan *imap.Message, messageChanBuffer)
		batchDone := make(chan error, 1)
		items := []imap.FetchItem{fallbackHeaderSection.FetchItem(), imap.FetchUid}
		go func() { batchDone <- cli.UidFetch(uidSet, items, messages) }()

		for msg := range messages {
			if ctx.Err() != nil {
				continue
			}
			from, date, subject := readFallbackHeaders(msg)
			if from == "" && date == "" && subject == "" {
				allEmpty++
			}
			ids[fallbackSyncKey(from, date, subject, sizes[msg.Uid])] = msg.Uid
		}
		if ferr := <-batchDone; ferr != nil {
			return allEmpty, fmt.Errorf("[%s] fetch fallback headers: %w", c.prefix, ferr)
		}
	}
	return allEmpty, nil
}

// fallbackSyncKey hashes From, Date, Subject and RFC822.SIZE into a stable
// string key prefixed with fallbackKeyPrefix. Null-byte separators prevent
// cross-field collisions (e.g. from="A", subject="BC" vs from="AB", subject="C").
func fallbackSyncKey(from, date, subject string, size uint64) string {
	h := sha256.New()
	h.Write([]byte(from))
	h.Write([]byte{0})
	h.Write([]byte(date))
	h.Write([]byte{0})
	h.Write([]byte(subject))
	h.Write([]byte{0})
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], size)
	h.Write(buf[:])
	return fallbackKeyPrefix + hex.EncodeToString(h.Sum(nil))
}

// readFallbackHeaders extracts the raw From, Date, and Subject header values
// from a message fetched with fallbackHeaderSection. Missing or unparseable
// fields are returned as empty strings.
func readFallbackHeaders(msg *imap.Message) (from, date, subject string) {
	body := msg.GetBody(fallbackHeaderSection)
	if body == nil {
		return "", "", ""
	}
	raw, err := io.ReadAll(body)
	if err != nil || len(raw) == 0 {
		return "", "", ""
	}
	if !bytes.Contains(raw, []byte("\r\n\r\n")) && !bytes.Contains(raw, []byte("\n\n")) {
		raw = append(raw, '\r', '\n', '\r', '\n')
	}
	m, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		return "", "", ""
	}
	return m.Header.Get("From"), m.Header.Get("Date"), m.Header.Get("Subject")
}

// readMessageIDHeader extracts a normalized Message-Id from a fetched message.
// Returns empty string when the section was omitted or unparseable.
func readMessageIDHeader(msg *imap.Message) string {
	body := msg.GetBody(messageIDHeaderSection)
	if body == nil {
		// Some servers reply with a slightly different BodySectionName
		// shape; fall back to scanning all body sections for the header.
		for _, lit := range msg.Body {
			if lit == nil {
				continue
			}
			if id := parseMessageID(lit); id != "" {
				return id
			}
		}
		return ""
	}
	return parseMessageID(body)
}

// parseMessageID reads a literal that should contain just "Message-Id: ..."
// followed by a blank line, and returns the trimmed Message-Id value.
func parseMessageID(lit io.Reader) string {
	raw, err := io.ReadAll(lit)
	if err != nil || len(raw) == 0 {
		return ""
	}
	// net/mail.ReadMessage requires a header/body separator; servers always
	// supply one, but be defensive in case some implementation truncates.
	if !bytes.Contains(raw, []byte("\r\n\r\n")) && !bytes.Contains(raw, []byte("\n\n")) {
		raw = append(raw, '\r', '\n', '\r', '\n')
	}
	m, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		return ""
	}
	id := m.Header.Get("Message-Id")
	return trimAngleBrackets(id)
}

// trimAngleBrackets returns id without surrounding "<" and ">" without
// allocating, unlike strings.Trim which allocates on the trim path.
func trimAngleBrackets(id string) string {
	if len(id) >= 2 && id[0] == '<' && id[len(id)-1] == '>' {
		return id[1 : len(id)-1]
	}
	return id
}

// StreamMessagesByUIDs fetches the bodies for the given UIDs in batches and
// invokes onMessage for each as it arrives. The caller is expected to feed in
// UIDs already filtered against the destination — typically the result of a
// Message-Id diff produced from two FetchMessageMap calls.
//
// Streaming avoids materializing every body into memory at once; large
// mailboxes can produce many GB of cumulative body data.
//
// If onMessage returns an error, the channel from the in-flight batch is
// drained (so the producer goroutine exits cleanly) and the error is
// returned without scheduling further batches.
func (c *Client) StreamMessagesByUIDs(ctx context.Context, folder string, uids []uint32, onMessage func(*imap.Message) error) error {
	stop := c.withCancel(ctx)
	defer stop()

	if err := ctx.Err(); err != nil {
		return err
	}
	if len(uids) == 0 {
		return nil
	}

	// Sorted UIDs help imap.SeqSet collapse runs into ranges instead of
	// emitting "1,2,3,4..." over the wire.
	uids = slices.Clone(uids)
	slices.Sort(uids)

	c.log("[%s] Streaming %d messages from %s", c.prefix, len(uids), folder)

	var cbErr error
	for start := 0; start < len(uids); start += uidFetchBatchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		if cbErr != nil {
			return cbErr
		}

		end := min(start+uidFetchBatchSize, len(uids))
		batch := uids[start:end]

		err := c.safeCall(func(cli *imapclient.Client) error {
			// selectIfNeeded short-circuits when the folder is already
			// selected on this connection. After a reconnect inside this
			// safeCall, the generation flip has cleared selectedFolder so
			// we re-Select here.
			if _, err := c.selectIfNeeded(cli, folder); err != nil {
				return fmt.Errorf("[%s] select folder %s: %w", c.prefix, folder, err)
			}
			uidSet := new(imap.SeqSet)
			for _, uid := range batch {
				uidSet.AddNum(uid)
			}
			messages := make(chan *imap.Message, messageChanBuffer)
			batchDone := make(chan error, 1)
			items := []imap.FetchItem{imap.FetchEnvelope, fullBodyPeekSection.FetchItem()}
			go func() { batchDone <- cli.UidFetch(uidSet, items, messages) }()

			for msg := range messages {
				// Once cancelled or the callback errored, just drain so the
				// producer goroutine can exit and we don't leak it.
				if ctx.Err() != nil || cbErr != nil {
					continue
				}
				if e := onMessage(msg); e != nil {
					cbErr = e
				}
			}
			if err := <-batchDone; err != nil {
				return fmt.Errorf("[%s] body fetch: %w", c.prefix, err)
			}
			return nil
		})
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		if cbErr != nil {
			return cbErr
		}
	}
	return nil
}
