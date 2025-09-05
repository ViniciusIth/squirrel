package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelectBuilderFromSelectLateral(t *testing.T) {
	subQ := Select("c").From("d").Where(Eq{"i": 0})

	b := Select("a", "b").FromSelectLateral(subQ, "subq")

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "SELECT a, b FROM LATERAL (SELECT c FROM d WHERE i = ?) AS subq"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{0}
	assert.Equal(t, expectedArgs, args)
}

func TestSelectBuilderFromSelectLateralNestedDollarPlaceholders(t *testing.T) {
	subQ := Select("c").
		From("t").
		Where(Gt{"c": 1}).
		PlaceholderFormat(Dollar)

	b := Select("c").
		FromSelectLateral(subQ, "subq").
		Where(Lt{"c": 2}).
		PlaceholderFormat(Dollar)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "SELECT c FROM LATERAL (SELECT c FROM t WHERE c > $1) AS subq WHERE c < $2"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{1, 2}
	assert.Equal(t, expectedArgs, args)
}

func TestSelectBuilderJoinLateralSelect(t *testing.T) {
	postsForUser := Select("p.*").
		From("posts p").
		Where(Expr("p.user_id = u.id")).
		OrderBy("p.id DESC").
		Limit(3)

	b := Select("u.id", "p.title").
		From("users u").
		JoinLateralSelect(postsForUser, "p", Expr("TRUE"))

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "SELECT u.id, p.title FROM users u JOIN LATERAL (SELECT p.* FROM posts p WHERE p.user_id = u.id ORDER BY p.id DESC LIMIT 3) AS p ON TRUE"
	assert.Equal(t, expectedSql, sql)
	assert.Empty(t, args)
}

func TestSelectBuilderLeftJoinLateralSelect(t *testing.T) {
	subQ := Select("x.*").From("expensive_source x").Where(Expr("x.key = u.key")).Limit(1)

	b := Select("u.id", "x.value").
		From("users u").
		LeftJoinLateralSelect(subQ, "x", Expr("TRUE")).
		OrderBy("u.id ASC").
		Limit(5).
		Offset(10)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "SELECT u.id, x.value FROM users u LEFT JOIN LATERAL (SELECT x.* FROM expensive_source x WHERE x.key = u.key LIMIT 1) AS x ON TRUE ORDER BY u.id ASC LIMIT 5 OFFSET 10"
	assert.Equal(t, expectedSql, sql)

	assert.Empty(t, args)
}

func TestSelectBuilderCrossJoinLateralSelect(t *testing.T) {
	subQ := Select("g.n").From("generate_series(1, 3) g(n)")

	b := Select("u.id", "g.n").
		From("users u").
		CrossJoinLateralSelect(subQ, "g")

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "SELECT u.id, g.n FROM users u CROSS JOIN LATERAL (SELECT g.n FROM generate_series(1, 3) g(n)) AS g"
	assert.Equal(t, expectedSql, sql)
	assert.Len(t, args, 0)
}

func TestSelectBuilderJoinLateralSelectNestedDollarPlaceholders(t *testing.T) {
	inner := Select("c").
		From("t").
		Where(Gt{"c": 1}).
		PlaceholderFormat(Dollar)

	b := Select("u.id", "subq.c").
		From("users u").
		JoinLateralSelect(inner, "subq", Expr("TRUE")).
		Where(Lt{"subq.c": 2}).
		PlaceholderFormat(Dollar)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "SELECT u.id, subq.c FROM users u JOIN LATERAL (SELECT c FROM t WHERE c > $1) AS subq ON TRUE WHERE subq.c < $2"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{1, 2}
	assert.Equal(t, expectedArgs, args)
}

func TestSelectBuilderRawJoinLateral(t *testing.T) {
	b := Select("u.id", "gs.n").
		From("users u").
		JoinLateralSelect(Expr("generate_series(1,3)"), "gs(n)", Expr("TRUE"))

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "SELECT u.id, gs.n FROM users u JOIN LATERAL (generate_series(1,3)) AS gs(n) ON TRUE"
	assert.Equal(t, expectedSql, sql)
	assert.Len(t, args, 0)
}

func TestSelectBuilderRawLeftAndCrossJoinLateral(t *testing.T) {
	b := Select("u.id", "gs.n").
		From("users u").
		LeftJoinLateralSelect(Expr("generate_series(1,3)"), "gs(n)", Expr("TRUE")).
		CrossJoinLateralSelect(Expr("generate_series(4,6)"), "gs2(n)")

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "SELECT u.id, gs.n FROM users u LEFT JOIN LATERAL (generate_series(1,3)) AS gs(n) ON TRUE CROSS JOIN LATERAL (generate_series(4,6)) AS gs2(n)"
	assert.Equal(t, expectedSql, sql)
	assert.Len(t, args, 0)
}
