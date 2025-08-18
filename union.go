package squirrel

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/lann/builder"
)

// UnionBuilder builds SQL for (SELECT ...) UNION [ALL] (SELECT ...) ... chains.
// It intentionally parenthesizes each subselect so ORDER BY / LIMIT / OFFSET
// apply to the *whole* union across dialects.
//
// API:
//   sq.Union(   sq.Select(...), sq.Select(...), ...).OrderBy(...).Limit(...)
//   sq.UnionAll(sq.Select(...), sq.Select(...), ...).Union(sq.Select(...))
//   // Optional: .Compact() to strip newlines / collapse spaces
//
// When composing with your CTE builder, pass the UnionBuilder as the *final statement*
// via a convenience method on your CTE builder (see snippet below).
//
// Note on placeholders:
// Prefer to set PlaceholderFormat at the top-level builder (e.g., your CTE builder)
// so replacement happens exactly once end-to-end. If you set it on the union itself,
// that’s fine too; just don’t double-replace.

type unionOp string

const (
	unionDistinct unionOp = "UNION"
	unionAll      unionOp = "UNION ALL"
)

// one union segment: [op] (subquery)
// The first segment has op="" (no leading operator).
type unionPart struct {
	op    unionOp
	query Sqlizer
}

// internal state carried by the builder.
type unionData struct {
	PlaceholderFormat PlaceholderFormat

	Parts   []unionPart // ordered list of subqueries composing the union
	OrderBy []string    // whole-union ORDER BY

	LimitSet  bool
	Limit     uint64
	OffsetSet bool
	Offset    uint64

	Suffixes []Sqlizer // trailing expressions (e.g., hints, comments)

	// If true, ToSql compacts whitespace (no '\n' or duplicate spaces).
	CompactOutput bool
}

// ensure we satisfy Sqlizer at compile time.
var _ Sqlizer = (UnionBuilder{})

// ---------------- Rendering ----------------

func (d *unionData) toSql() (string, []any, error) {
	if len(d.Parts) == 0 {
		return "", nil, fmt.Errorf("squirrel: union requires at least one SELECT")
	}

	var buf bytes.Buffer
	var args []any

	// Body: (SELECT ...) [UNION|UNION ALL] (SELECT ...) ...
	for i, p := range d.Parts {
		subSQL, subArgs, err := p.query.ToSql()
		if err != nil {
			return "", nil, fmt.Errorf("squirrel: union subquery %d: %w", i, err)
		}
		if i > 0 {
			buf.WriteByte(' ')
			buf.WriteString(string(p.op))
			buf.WriteByte(' ')
		}
		buf.WriteByte('(')
		buf.WriteString(subSQL)
		buf.WriteByte(')')
		args = append(args, subArgs...)
	}

	// Whole-union clauses.
	if len(d.OrderBy) > 0 {
		buf.WriteString(" ORDER BY ")
		buf.WriteString(strings.Join(d.OrderBy, ", "))
	}
	if d.LimitSet {
		fmt.Fprintf(&buf, " LIMIT %d", d.Limit)
	}
	if d.OffsetSet {
		fmt.Fprintf(&buf, " OFFSET %d", d.Offset)
	}

	// Suffixes (same behavior as SelectBuilder).
	if len(d.Suffixes) > 0 {
		buf.WriteByte(' ')
		var err error
		args, err = appendToSql(d.Suffixes, &buf, " ", args)
		if err != nil {
			return "", nil, err
		}
	}

	sqlStr := buf.String()

	// Placeholder replacement last.
	if d.PlaceholderFormat != nil {
		var err error
		sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sqlStr)
		if err != nil {
			return "", nil, err
		}
	}

	// Optional whitespace compaction for single-line SQL.
	if d.CompactOutput {
		sqlStr = compactSQL(sqlStr)
	}

	return sqlStr, args, nil
}

func (d *unionData) ToSql() (string, []any, error) { return d.toSql() }

// compactSQL collapses all whitespace into single spaces.
// Useful to normalize output if upstream builders include newlines.
func compactSQL(s string) string {
	// strings.Fields splits on all whitespace and trims; join with single spaces.
	return strings.Join(strings.Fields(s), " ")
}

// ---------------- Builder ----------------

type UnionBuilder builder.Builder

func init() {
	builder.Register(UnionBuilder{}, unionData{})
}

// Union constructs a UNION (DISTINCT) chain with the given subqueries.
// The first subquery has no leading operator; subsequent ones use "UNION".
func Union(parts ...Sqlizer) UnionBuilder {
	u := UnionBuilder{}
	for i, p := range parts {
		if i == 0 {
			u = builder.Append(u, "Parts", unionPart{op: "", query: p}).(UnionBuilder)
		} else {
			u = builder.Append(u, "Parts", unionPart{op: unionDistinct, query: p}).(UnionBuilder)
		}
	}
	return u
}

// UnionAll constructs a UNION ALL chain with the given subqueries.
// The first subquery has no leading operator; subsequent ones use "UNION ALL".
func UnionAll(parts ...Sqlizer) UnionBuilder {
	u := UnionBuilder{}
	for i, p := range parts {
		if i == 0 {
			u = builder.Append(u, "Parts", unionPart{op: "", query: p}).(UnionBuilder)
		} else {
			u = builder.Append(u, "Parts", unionPart{op: unionAll, query: p}).(UnionBuilder)
		}
	}
	return u
}

// Union appends another subquery with UNION (DISTINCT).
func (b UnionBuilder) Union(q Sqlizer) UnionBuilder {
	return builder.Append(b, "Parts", unionPart{op: unionDistinct, query: q}).(UnionBuilder)
}

// UnionAll appends another subquery with UNION ALL.
func (b UnionBuilder) UnionAll(q Sqlizer) UnionBuilder {
	return builder.Append(b, "Parts", unionPart{op: unionAll, query: q}).(UnionBuilder)
}

// ----- Options -----

// OrderBy sets ORDER BY on the whole union.
// Example: .OrderBy("id DESC", "created_at")
func (b UnionBuilder) OrderBy(exprs ...string) UnionBuilder {
	return builder.Extend(b, "OrderBy", exprs).(UnionBuilder)
}

// Limit sets LIMIT on the whole union.
func (b UnionBuilder) Limit(n uint64) UnionBuilder {
	b = builder.Set(b, "LimitSet", true).(UnionBuilder)
	return builder.Set(b, "Limit", n).(UnionBuilder)
}

// Offset sets OFFSET on the whole union.
func (b UnionBuilder) Offset(n uint64) UnionBuilder {
	b = builder.Set(b, "OffsetSet", true).(UnionBuilder)
	return builder.Set(b, "Offset", n).(UnionBuilder)
}

// Suffix appends trailing SQL fragments (e.g., comments/hints) to the union.
// Use with Expr(...) or other Sqlizers.
func (b UnionBuilder) Suffix(exprs ...Sqlizer) UnionBuilder {
	return builder.Extend(b, "Suffixes", exprs).(UnionBuilder)
}

// PlaceholderFormat sets the placeholder format (Question, Dollar, Colon, etc.).
// Prefer setting this once at the top-level builder if the union is used inside
// a larger statement (e.g., WITH ... <union>).
func (b UnionBuilder) PlaceholderFormat(f PlaceholderFormat) UnionBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(UnionBuilder)
}

// Compact enables one-line SQL output (no newlines / duplicate spaces).
func (b UnionBuilder) Compact() UnionBuilder {
	return builder.Set(b, "CompactOutput", true).(UnionBuilder)
}

// ----- Sqlizer -----

func (b UnionBuilder) ToSql() (string, []any, error) {
	data := builder.GetStruct(b).(unionData)
	return data.ToSql()
}

func (b UnionBuilder) MustSql() (string, []any) {
	sql, args, err := b.ToSql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

