package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"strconv"
)

const (
	msgInsertedFmt = "INSERTED %d\r\n"
	msgBadFmt      = "BAD_FORMAT\r\n"

	msgUnknownCommand = "UNKNOWN_COMMAND\r\n"
	msgExpectedCRLF   = "EXPECTED_CRLF\r\n"
)

type opType int

const (
	opPut opType = iota
	opStats
	opUse
	opQuit
	opUnknown
)

var (
	cmdUse    = "use "
	cmdUseLen = len(cmdUse)
	cmdPut    = "put "
	cmdStats  = "stats"
	cmdQuit   = "quit"

	opNames = map[opType]string{
		opPut:     cmdPut,
		opStats:   cmdStats,
		opUse:     cmdUse,
		opQuit:    cmdQuit,
		opUnknown: "<unknown>",
	}

	opCount = map[opType]uint64{}

	curConnCount = 0

	readyCount = 0

	globalStat = stats{}
)

type stats struct {
	urgentCount    uint
	waitingCount   uint
	buriedCount    uint
	reservedCount  uint
	pauseCount     uint
	totalJobsCount uint64
}

func main() {
	hostPort := ":3333"
	l, err := net.Listen("tcp", hostPort)
	if err != nil {
		fmt.Printf("Failed to listen: %v\n", err)
		os.Exit(-1)
	}

	defer l.Close()
	fmt.Printf("Listening on %v\n", hostPort)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Printf("Failed to accept: %v\n", err)
			continue
		}

		c := makeConn(conn, connStateWantCommand)
		go handleConn(c)
	}
}

type connState int

const (
	connStateWantCommand connState = iota
	connStateSendWord
	connStateSendJob
	connStateClose
)

const (
	lineBufSize = 224
)

type conn struct {
	conn  net.Conn
	state connState

	reader *bufio.Reader

	cmd     []byte
	cmdLen  int
	cmdRead int

	reply string

	inJobRead int
	inJob     *job
}

func makeConn(c net.Conn, initialState connState) *conn {
	curConnCount++
	return &conn{
		conn:   c,
		reader: bufio.NewReader(c),
		state:  initialState,
	}
}

type job struct {
	pri      uint64
	delay    uint64
	ttr      uint64
	bodySize uint64
	body     []byte
}

func makeJob(pri, delay, ttr, bodySize uint64) *job {
	return &job{
		pri:      pri,
		delay:    delay,
		ttr:      ttr,
		bodySize: bodySize,
		body:     make([]byte, bodySize),
	}
}

func handleConn(c *conn) {
	for {
		connData(c)

		if c.state == connStateClose {
			connClose(c)
			return
		}
	}
}

func connData(c *conn) {
	switch c.state {
	case connStateWantCommand:
		r, err := c.reader.ReadBytes('\n')
		if err != nil {
			c.state = connStateClose
			return
		}
		c.cmd = r
		// TODO handle large job
		doCmd(c)
		return

		break
	case connStateSendWord:
		_, err := c.conn.Write([]byte(c.reply))
		if err != nil {
			// TODO log error
			c.state = connStateClose
			return
		}
		resetConn(c)
		break
	case connStateSendJob:
		_, err := c.conn.Write([]byte(c.reply))

		if err != nil {
			// TODO log error
			c.state = connStateClose
			return
		}

		resetConn(c)
		break
	}
}

func resetConn(c *conn) {
	c.state = connStateWantCommand
}

func wantCommand(c *conn) bool {
	return c.state == connStateWantCommand
}

func cmdDataReady(c *conn) bool {
	return wantCommand(c) && c.cmdRead > 0
}

func doCmd(c *conn) {
	msgType := whichCmd(c.cmd)
	fmt.Printf("command %s\n", opNames[msgType])

	switch msgType {
	case opPut:
		fields := bytes.Fields(c.cmd)
		if len(fields) != 5 {
			replyMsg(c, msgBadFmt)
			return
		}

		pri, err := strconv.ParseUint(string(fields[1]), 10, 32)
		if err != nil {
			replyMsg(c, msgBadFmt)
			return
		}

		delay, err := strconv.ParseUint(string(fields[2]), 10, 32)
		if err != nil {
			replyMsg(c, msgBadFmt)
			return
		}

		ttr, err := strconv.ParseUint(string(fields[3]), 10, 32)
		if err != nil {
			replyMsg(c, msgBadFmt)
			return
		}

		bodySize, err := strconv.ParseUint(string(fields[4]), 10, 32)
		if err != nil {
			replyMsg(c, msgBadFmt)
			return
		}

		opCount[msgType]++

		// TODO check max job size

		if ttr < 1000000000 {
			ttr = 1000000000
		}

		c.inJob = makeJob(pri, delay, ttr, bodySize+2)

		nbRead, err := c.reader.Read(c.inJob.body)
		if nbRead != len(c.inJob.body) {
			replyMsg(c, msgBadFmt)
			return
		}
		fmt.Printf("body %s\n", string(c.inJob.body))
		enqueueIncomingJob(c)
		return

		break
	case opStats:
		// TODO verify no trailing garbage
		opCount[msgType]++
		doStats(c, fmtStats)
		break
	case opUse:
		name := c.cmd[cmdUseLen:]
		// TODO verify name
		opCount[msgType]++
		replyLine(c, connStateSendWord, "USING %s\r\n", name)
		break
	case opQuit:
		c.state = connStateClose
		break
	default:
		replyMsg(c, msgUnknownCommand)
		return
	}
}

func whichCmd(cmd []byte) opType {
	if bytes.HasPrefix(cmd, []byte(cmdPut)) {
		return opPut
	}
	if bytes.HasPrefix(cmd, []byte(cmdStats)) {
		return opStats
	}
	if bytes.HasPrefix(cmd, []byte(cmdUse)) {
		return opUse
	}
	if bytes.HasPrefix(cmd, []byte(cmdQuit)) {
		return opQuit
	}
	return opUnknown
}

func enqueueIncomingJob(c *conn) {
	j := c.inJob
	c.inJob = nil
	if !bytes.HasSuffix(j.body, []byte("\r\n")) {
		replyMsg(c, msgExpectedCRLF)
		return
	}
	// TODO log new job
	// XXX do something with job
	_ = j

	globalStat.totalJobsCount++
	// TODO increase tube stats
	id := 1
	replyLine(c, connStateSendWord, msgInsertedFmt, id)
}

func replyLine(c *conn, state connState, f string, data ...interface{}) {
	r := fmt.Sprintf(f, data...)
	reply(c, r, state)
}

func replyMsg(c *conn, msg string) {
	reply(c, msg, connStateSendWord)
}

func reply(c *conn, msg string, state connState) {
	if c == nil {
		return
	}
	c.reply = msg
	c.state = state

	fmt.Printf("reply %s\n", msg)
}

func countCurConns() int {
	return curConnCount
}

func getDelayedJobCount() uint {
	// FIXME
	return 0
}

type fmtFunc func(data ...interface{}) string

var statsFmt = "---\n" +
	"current-jobs-urgent: %d\n" +
	"current-jobs-ready: %d\n" +
	"current-jobs-reserved: %d\n" +
	"current-jobs-delayed: %d\n" +
	"current-jobs-buried: %d\n" +
	"cmd-put: %d\n" +
	"cmd-use: %d\n" +
	"cmd-stats: %d\n" +
	"current-connections: %d\n"

func fmtStats(data ...interface{}) string {
	return fmt.Sprintf(statsFmt,
		globalStat.urgentCount,
		readyCount,
		globalStat.reservedCount,
		getDelayedJobCount(),
		globalStat.buriedCount,
		opCount[opPut],
		opCount[opUse],
		opCount[opStats],
		countCurConns(),
	)
}

func doStats(c *conn, fmtFn fmtFunc, data ...interface{}) {
	res := fmtFn(data)
	replyLine(c, connStateSendJob, "OK %s\r\n", res)
}

func connClose(c *conn) {
	if err := c.conn.Close(); err != nil {
		// TODO log error
	}
	curConnCount = curConnCount - 1
	// TODO clean

}
