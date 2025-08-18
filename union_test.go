package squirrel

import (
	"reflect"
	"testing"
)

// compactedEqual compares two SQL strings after collapsing all whitespace.
// This keeps tests stable even if child builders include newlines.
func compactedEqual(got, want string) bool {
	return compactSQL(got) == compactSQL(want)
}

func TestUnion_DistinctBasic(t *testing.T) {
	u := Union(
		Select("id").From("a").Where(Expr("x > ?", 10)),
		Select("id").From("b").Where(Expr("y < ?", 5)),
	).OrderBy("id").Limit(100).Offset(2).
		PlaceholderFormat(Dollar)

	sql, args, err := u.ToSql()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wantSQL := "(SELECT id FROM a WHERE x > $1) UNION (SELECT id FROM b WHERE y < $2) ORDER BY id LIMIT 100 OFFSET 2"
	if !compactedEqual(sql, wantSQL) {
		t.Fatalf("sql mismatch\n got: %s\nwant: %s", sql, wantSQL)
	}
	wantArgs := []any{10, 5}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("args mismatch\n got: %#v\nwant: %#v", args, wantArgs)
	}
}

func TestUnion_AllThreeParts(t *testing.T) {
	u := UnionAll(
		Select("name").From("t1").Where(Expr("a = ?", 1)),
		Select("name").From("t2").Where(Expr("b = ?", 2)),
		Select("name").From("t3").Where(Expr("c = ?", 3)),
	).PlaceholderFormat(Dollar)

	sql, args, err := u.ToSql()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wantSQL := "(SELECT name FROM t1 WHERE a = $1) UNION ALL (SELECT name FROM t2 WHERE b = $2) UNION ALL (SELECT name FROM t3 WHERE c = $3)"
	if !compactedEqual(sql, wantSQL) {
		t.Fatalf("sql mismatch\n got: %s\nwant: %s", sql, wantSQL)
	}
	wantArgs := []any{1, 2, 3}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("args mismatch\n got: %#v\nwant: %#v", args, wantArgs)
	}
}

func TestUnion_AppendChainMix(t *testing.T) {
	u := Union(
		Select("id").From("a").Where(Expr("x = ?", "A")),
	).UnionAll(
		Select("id").From("b").Where(Expr("y = ?", "B")),
	).Union(
		Select("id").From("c").Where(Expr("z = ?", "C")),
	).PlaceholderFormat(Dollar)

	sql, args, err := u.ToSql()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wantSQL := "(SELECT id FROM a WHERE x = $1) UNION ALL (SELECT id FROM b WHERE y = $2) UNION (SELECT id FROM c WHERE z = $3)"
	if !compactedEqual(sql, wantSQL) {
		t.Fatalf("sql mismatch\n got: %s\nwant: %s", sql, wantSQL)
	}
	wantArgs := []any{"A", "B", "C"}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("args mismatch\n got: %#v\nwant: %#v", args, wantArgs)
	}
}

func TestUnion_OrderLimitOffsetBindToWholeUnion(t *testing.T) {
	u := UnionAll(
		Select("id").From("a"),
		Select("id").From("b"),
	).OrderBy("id DESC").Limit(1).Offset(1).PlaceholderFormat(Dollar)

	sql, _, err := u.ToSql()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantSQL := "(SELECT id FROM a) UNION ALL (SELECT id FROM b) ORDER BY id DESC LIMIT 1 OFFSET 1"
	if !compactedEqual(sql, wantSQL) {
		t.Fatalf("sql mismatch\n got: %s\nwant: %s", sql, wantSQL)
	}
}

func TestUnion_SuffixAndArgsOrder(t *testing.T) {
	u := Union(
		Select("id").From("a").Where(Expr("x > ?", 7)),
		Select("id").From("b").Where(Expr("y < ?", 9)),
	).Suffix(Expr("/* suffix */")).
		PlaceholderFormat(Dollar)

	sql, args, err := u.ToSql()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantSQL := "(SELECT id FROM a WHERE x > $1) UNION (SELECT id FROM b WHERE y < $2) /* suffix */"
	if !compactedEqual(sql, wantSQL) {
		t.Fatalf("sql mismatch\n got: %s\nwant: %s", sql, wantSQL)
	}
	wantArgs := []any{7, 9}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("args mismatch\n got: %#v\nwant: %#v", args, wantArgs)
	}
}

func TestUnion_PlaceholderQuestionFormat(t *testing.T) {
	u := Union(
		Select("id").From("a").Where(Expr("x > ?", 1)),
		Select("id").From("b").Where(Expr("y < ?", 2)),
	).PlaceholderFormat(Question)

	sql, args, err := u.ToSql()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantSQL := "(SELECT id FROM a WHERE x > ?) UNION (SELECT id FROM b WHERE y < ?)"
	if !compactedEqual(sql, wantSQL) {
		t.Fatalf("sql mismatch\n got: %s\nwant: %s", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, []any{1, 2}) {
		t.Fatalf("args mismatch\n got: %#v\nwant: %#v", args, []any{1, 2})
	}
}

func TestUnion_CompactCollapsesWhitespace(t *testing.T) {
	u := Union(
		Select("id").From("a"),
		Select("id").From("b"),
	).Suffix(Expr("/* line1 */\n/* line2 */")). // inject a newline
							Compact().
							PlaceholderFormat(Dollar)

	sql, _, err := u.ToSql()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Expect single spaces only.
	wantSQL := "(SELECT id FROM a) UNION (SELECT id FROM b) /* line1 */ /* line2 */"
	if sql != wantSQL {
		t.Fatalf("sql mismatch (compact)\n got: %q\nwant: %q", sql, wantSQL)
	}
}

func TestUnion_EmptyError(t *testing.T) {
	_, _, err := Union().ToSql()
	if err == nil {
		t.Fatalf("expected error for empty union, got nil")
	}
}
