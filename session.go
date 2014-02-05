// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// Session is the interface used by users to interact with the database.
//
// It's safe for concurrent use by multiple goroutines and a typical usage
// scenario is to have one global session object to interact with the
// whole Cassandra cluster.
//
// This type extends the Node interface by adding a convinient query builder
// and automatically sets a default consinstency level on all operations
// that do not have a consistency level set.
type Session struct {
	Node     Node
	cons     Consistency
	pageSize int
	prefetch float64
	trace    Tracer
	mu       sync.RWMutex
	cfg      ClusterConfig
}

// NewSession wraps an existing Node.
func NewSession(c *clusterImpl) *Session {
	return &Session{Node: c,
		cons:     Quorum,
		prefetch: 0.25,
		cfg:      c.cfg}
}

// SetConsistency sets the default consistency level for this session. This
// setting can also be changed on a per-query basis and the default value
// is Quorum.
func (s *Session) SetConsistency(cons Consistency) {
	s.mu.Lock()
	s.cons = cons
	s.mu.Unlock()
}

// SetPageSize sets the default page size for this session. A value <= 0 will
// disable paging. This setting can also be changed on a per-query basis.
func (s *Session) SetPageSize(n int) {
	s.mu.Lock()
	s.pageSize = n
	s.mu.Unlock()
}

// SetPrefetch sets the default threshold for pre-fetching new pages. If
// there are only p*pageSize rows remaining, the next page will be requested
// automatically. This value can also be changed on a per-query basis and
// the default value is 0.25.
func (s *Session) SetPrefetch(p float64) {
	s.mu.Lock()
	s.prefetch = p
	s.mu.Unlock()
}

// SetTrace sets the default tracer for this session. This setting can also
// be changed on a per-query basis.
func (s *Session) SetTrace(trace Tracer) {
	s.mu.Lock()
	s.trace = trace
	s.mu.Unlock()
}

// Query generates a new query object for interacting with the database.
// Further details of the query may be tweaked using the resulting query
// value before the query is executed.
func (s *Session) Query(stmt string, values ...interface{}) *Query {
	s.mu.RLock()
	rt := RetryPolicy{
		DataCenter: s.cfg.DefaultRetryPolicy.DataCenter,
		Rack:       s.cfg.DefaultRetryPolicy.Rack,
	}

	qry := &Query{stmt: stmt,
		values:   values,
		cons:     s.cons,
		session:  s,
		pageSize: s.pageSize,
		trace:    s.trace,
		prefetch: s.prefetch,
		qm: &QueryMeta{
			rtPolicy: rt,
			lbPolicy: s.cfg.DefaultLBPolicy,
			prefDC:   s.cfg.PreferredDataCenter,
			prefRack: s.cfg.PreferredRack,
		},
	}
	s.mu.RUnlock()
	return qry
}

// Close closes all connections. The session is unusable after this
// operation.
func (s *Session) Close() {
	s.Node.Close()
}

func (s *Session) executeQuery(qry *Query) *Iter {
	var err error
	var itr *Iter
	q := qry.qm
	rtyNum := q.rtPolicy.Rack + q.rtPolicy.DataCenter
	if rtyNum == 0 {
		rtyNum = 1
	}
	for q.rtPolicy.Count < rtyNum {
		conn := s.Node.Pick(q)

		if conn == nil {
			itr = &Iter{err: ErrUnavailable}
		} else {
			q.lastHID = conn.hostID
			itr = conn.executeQuery(qry)
		}
		//Check if there was even an error
		if itr.err == nil {
			return itr
		}
		//Check if an errFrame was configured. If not we can't retry the query
		if itr.errFrame == nil {
			return itr
		}
		//Determine if we can retry the query based on the error frame.
		switch itr.errFrame.Code {
		case errServer, errWriteTimeout, errReadTimeout, errOverloaded:
			err = itr.errFrame
		default:
			return itr
		}
		q.rtPolicy.Count++
	}
	return &Iter{err: err}
}

func (s *Session) ExecuteBatch(batch *Batch) error {
	var err error
	b := batch.qm

	rtyNum := b.rtPolicy.Rack + b.rtPolicy.DataCenter
	if rtyNum == 0 {
		rtyNum = 1
	}
	for b.rtPolicy.Count < rtyNum {
		conn := s.Node.Pick(b)
		if conn == nil {
			return ErrUnavailable
		}
		b.lastHID = conn.hostID
		err = conn.executeBatch(batch)
		//Return if no error was returned
		if err == nil {
			return nil
		}
		//Check that an error frame was provided.
		if errFrm, ok := err.(errorFrame); ok {
			//Check the list of errors that can be retried.
			switch errFrm.Code {
			case errServer, errWriteTimeout, errReadTimeout, errOverloaded:
				err = errFrm
			default:
				return err
			}
		} else {
			return err
		}
		b.rtPolicy.Count++
	}
	return err
}

type QueryMeta struct {
	lastHID  UUID
	prefDC   string
	prefRack string
	lbPolicy LoadBalancePolicy
	rtPolicy RetryPolicy
}

// Query represents a CQL statement that can be executed.
type Query struct {
	stmt      string
	values    []interface{}
	cons      Consistency
	pageSize  int
	pageState []byte
	prefetch  float64
	trace     Tracer
	session   *Session
	qm        *QueryMeta
}

// Consistency sets the consistency level for this query. If no consistency
// level have been set, the default consistency level of the cluster
// is used.
func (q *Query) Consistency(c Consistency) *Query {
	q.cons = c
	return q
}

// Trace enables tracing of this query. Look at the documentation of the
// Tracer interface to learn more about tracing.
func (q *Query) Trace(trace Tracer) *Query {
	q.trace = trace
	return q
}

// PageSize will tell the iterator to fetch the result in pages of size n.
// This is useful for iterating over large result sets, but setting the
// page size to low might decrease the performance. This feature is only
// available in Cassandra 2 and onwards.
func (q *Query) PageSize(n int) *Query {
	q.pageSize = n
	return q
}

// Prefetch sets the default threshold for pre-fetching new pages. If
// there are only p*pageSize rows remaining, the next page will be requested
// automatically.
func (q *Query) Prefetch(p float64) *Query {
	q.prefetch = p
	return q
}

// RetryPolicy sets the retry policy to use for this query that may differ
// from the default cluster retry policy
func (q *Query) RetryPolicy(rt RetryPolicy) *Query {
	q.qm.rtPolicy = rt
	return q
}

// LoadBalancePolicy sets the load balancing policy to use for this query
// that may differ from the default cluster policy.
func (q *Query) LoadBalancePolicy(lbp LoadBalancePolicy) *Query {
	q.qm.lbPolicy = lbp
	return q
}

// PreferredDataCenter sets the datacenter to prefer when executing the query
func (q *Query) PreferredDataCenter(dc string) *Query {
	q.qm.prefDC = dc
	return q
}

// PreferredRack sets the rack to prefer when executing the query
func (q *Query) PreferredRack(rack string) *Query {
	q.qm.prefRack = rack
	return q
}

// Exec executes the query without returning any rows.
func (q *Query) Exec() error {
	iter := q.session.executeQuery(q)
	return iter.err
}

// Iter executes the query and returns an iterator capable of iterating
// over all results.
func (q *Query) Iter() *Iter {
	return q.session.executeQuery(q)
}

// Scan executes the query, copies the columns of the first selected
// row into the values pointed at by dest and discards the rest. If no rows
// were selected, ErrNotFound is returned.
func (q *Query) Scan(dest ...interface{}) error {
	iter := q.Iter()
	if iter.err != nil {
		return iter.err
	}
	if len(iter.rows) == 0 {
		return ErrNotFound
	}
	iter.Scan(dest...)
	return iter.Close()
}

// ScanCAS executes a lightweight transaction (i.e. an UPDATE or INSERT
// statement containing an IF clause). If the transaction fails because
// the existing values did not match, the previos values will be stored
// in dest.
func (q *Query) ScanCAS(dest ...interface{}) (applied bool, err error) {
	iter := q.Iter()
	if iter.err != nil {
		return false, iter.err
	}
	if len(iter.rows) == 0 {
		return false, ErrNotFound
	}
	if len(iter.Columns()) > 1 {
		dest = append([]interface{}{&applied}, dest...)
		iter.Scan(dest...)
	} else {
		iter.Scan(&applied)
	}
	return applied, iter.Close()
}

// Iter represents an iterator that can be used to iterate over all rows that
// were returned by a query. The iterator might send additional queries to the
// database during the iteration if paging was enabled.
type Iter struct {
	err      error
	pos      int
	rows     [][][]byte
	columns  []ColumnInfo
	next     *nextIter
	errFrame *errorFrame
}

// Columns returns the name and type of the selected columns.
func (iter *Iter) Columns() []ColumnInfo {
	return iter.columns
}

// Scan consumes the next row of the iterator and copies the columns of the
// current row into the values pointed at by dest. Scan might send additional
// queries to the database to retrieve the next set of rows if paging was
// enabled.
//
// Scan returns true if the row was successfully unmarshaled or false if the
// end of the result set was reached or if an error occurred. Close should
// be called afterwards to retrieve any potential errors.
func (iter *Iter) Scan(dest ...interface{}) bool {
	if iter.err != nil {
		return false
	}
	if iter.pos >= len(iter.rows) {
		if iter.next != nil {
			*iter = *iter.next.fetch()
			return iter.Scan(dest...)
		}
		return false
	}
	if iter.next != nil && iter.pos == iter.next.pos {
		go iter.next.fetch()
	}
	if len(dest) != len(iter.columns) {
		iter.err = errors.New("count mismatch")
		return false
	}
	for i := 0; i < len(iter.columns); i++ {
		err := Unmarshal(iter.columns[i].TypeInfo, iter.rows[iter.pos][i], dest[i])
		if err != nil {
			iter.err = err
			return false
		}
	}
	iter.pos++
	return true
}

// Close closes the iterator and returns any errors that happened during
// the query or the iteration.
func (iter *Iter) Close() error {
	return iter.err
}

type nextIter struct {
	qry  Query
	pos  int
	once sync.Once
	next *Iter
}

func (n *nextIter) fetch() *Iter {
	n.once.Do(func() {
		n.next = n.qry.session.executeQuery(&n.qry)
	})
	return n.next
}

type Batch struct {
	Type    BatchType
	Entries []BatchEntry
	Cons    Consistency
	qm      *QueryMeta
}

//NewBatch creates a new batch object without using the defaults from the
//cluster configuration for load balancing and the retry policy
func NewBatch(typ BatchType) *Batch {
	return &Batch{Type: typ,
		qm: &QueryMeta{
			rtPolicy: RetryPolicy{DataCenter: 0, Rack: 0},
			lbPolicy: &RoundRobin{},
		},
	}
}

//NewBatch creates a new batch object using the defaults defined for the cluster.
func (s *Session) NewBatch(typ BatchType) *Batch {
	rt := RetryPolicy{
		DataCenter: s.cfg.DefaultRetryPolicy.DataCenter,
		Rack:       s.cfg.DefaultRetryPolicy.Rack,
	}
	return &Batch{
		Type: typ,
		Cons: s.cfg.Consistency,
		qm: &QueryMeta{
			rtPolicy: rt,
			lbPolicy: s.cfg.DefaultLBPolicy,
			prefDC:   s.cfg.PreferredDataCenter,
			prefRack: s.cfg.PreferredRack,
		},
	}
}

func (b *Batch) Query(stmt string, args ...interface{}) {
	b.Entries = append(b.Entries, BatchEntry{Stmt: stmt, Args: args})
}

// RetryPolicy sets the retry policy to use for this query that may differ
// from the default cluster retry policy
func (b *Batch) RetryPolicy(rt RetryPolicy) *Batch {
	b.qm.rtPolicy = rt
	return b
}

// LoadBalancePolicy sets the load balancing policy to use for this query
// that may differ from the default cluster policy.
func (b *Batch) LoadBalancePolicy(lbp LoadBalancePolicy) *Batch {
	b.qm.lbPolicy = lbp
	return b
}

type BatchType int

const (
	LoggedBatch   BatchType = 0
	UnloggedBatch BatchType = 1
	CounterBatch  BatchType = 2
)

type BatchEntry struct {
	Stmt string
	Args []interface{}
}

type Consistency int

const (
	Any Consistency = 1 + iota
	One
	Two
	Three
	Quorum
	All
	LocalQuorum
	EachQuorum
	Serial
	LocalSerial
)

var consinstencyNames = []string{
	0:           "default",
	Any:         "any",
	One:         "one",
	Two:         "two",
	Three:       "three",
	Quorum:      "quorum",
	All:         "all",
	LocalQuorum: "localquorum",
	EachQuorum:  "eachquorum",
	Serial:      "serial",
	LocalSerial: "localserial",
}

func (c Consistency) String() string {
	return consinstencyNames[c]
}

type ColumnInfo struct {
	Keyspace string
	Table    string
	Name     string
	TypeInfo *TypeInfo
}

// Tracer is the interface implemented by query tracers. Tracers have the
// ability to obtain a detailed event log of all events that happened during
// the execution of a query from Cassandra. Gathering this information might
// be essential for debugging and optimizing queries, but this feature should
// not be used on production systems with very high load.
type Tracer interface {
	Trace(traceId []byte)
}

type traceWriter struct {
	session *Session
	w       io.Writer
	mu      sync.Mutex
}

// NewTraceWriter returns a simple Tracer implementation that outputs
// the event log in a textual format.
func NewTraceWriter(session *Session, w io.Writer) Tracer {
	return traceWriter{session: session, w: w}
}

func (t traceWriter) Trace(traceId []byte) {
	var (
		coordinator string
		duration    int
	)
	t.session.Query(`SELECT coordinator, duration
			FROM system_traces.sessions
			WHERE session_id = ?`, traceId).
		Consistency(One).Scan(&coordinator, &duration)

	iter := t.session.Query(`SELECT event_id, activity, source, source_elapsed
			FROM system_traces.events
			WHERE session_id = ?`, traceId).
		Consistency(One).Iter()
	var (
		timestamp time.Time
		activity  string
		source    string
		elapsed   int
	)
	t.mu.Lock()
	defer t.mu.Unlock()
	fmt.Fprintf(t.w, "Tracing session %016x (coordinator: %s, duration: %v):\n",
		traceId, coordinator, time.Duration(duration)*time.Microsecond)
	for iter.Scan(&timestamp, &activity, &source, &elapsed) {
		fmt.Fprintf(t.w, "%s: %s (source: %s, elapsed: %d)\n",
			timestamp.Format("2006/01/02 15:04:05.999999"), activity, source, elapsed)
	}
	if err := iter.Close(); err != nil {
		fmt.Fprintln(t.w, "Error:", err)
	}
}

type Error struct {
	Code    int
	Message string
}

func (e Error) Error() string {
	return e.Message
}

var (
	ErrNotFound    = errors.New("not found")
	ErrUnavailable = errors.New("unavailable")
	ErrProtocol    = errors.New("protocol error")
	ErrUnsupported = errors.New("feature not supported")
	ErrConnTimeout = errors.New("connection timed out")
)
