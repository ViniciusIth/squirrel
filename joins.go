package squirrel

import (
	"fmt"
	"strings"

	"github.com/lann/builder"
)

type fromSelectLateralPart struct {
	sel   Sqlizer
	alias string
}

func (p fromSelectLateralPart) ToSql() (string, []any, error) {
	subSql, subArgs, err := p.sel.ToSql()
	if err != nil {
		return "", nil, err
	}

	sql := fmt.Sprintf("LATERAL (%s) AS %s", subSql, p.alias)
	return sql, subArgs, nil
}

func (b SelectBuilder) FromSelectLateral(sel Sqlizer, alias string) SelectBuilder {
	sel = forceQuestionPlaceholders(sel)
	return builder.Set(b, "From", fromSelectLateralPart{sel: sel, alias: alias}).(SelectBuilder)
}

type joinLateralSelectPart struct {
	joinType string // "JOIN", "LEFT JOIN", "CROSS JOIN"
	sel      Sqlizer
	alias    string
	on       Sqlizer // nil for CROSS JOIN
}

func (p joinLateralSelectPart) ToSql() (string, []any, error) {
	subSql, subArgs, err := p.sel.ToSql()
	if err != nil {
		return "", nil, err
	}

	var buf strings.Builder
	_, _ = fmt.Fprintf(&buf, "%s LATERAL (%s) AS %s", p.joinType, subSql, p.alias)

	args := subArgs
	if p.on != nil {
		onSql, onArgs, err := p.on.ToSql()
		if err != nil {
			return "", nil, err
		}
		_, _ = fmt.Fprintf(&buf, " ON %s", onSql)
		args = append(args, onArgs...)
	}

	return buf.String(), args, nil
}

func (b SelectBuilder) JoinLateralSelect(sel Sqlizer, alias string, on Sqlizer) SelectBuilder {
	sel = forceQuestionPlaceholders(sel)
	part := joinLateralSelectPart{joinType: "JOIN", sel: sel, alias: alias, on: on}
	return builder.Append(b, "Joins", part).(SelectBuilder)
}

func (b SelectBuilder) LeftJoinLateralSelect(sel Sqlizer, alias string, on Sqlizer) SelectBuilder {
	sel = forceQuestionPlaceholders(sel)
	part := joinLateralSelectPart{joinType: "LEFT JOIN", sel: sel, alias: alias, on: on}
	return builder.Append(b, "Joins", part).(SelectBuilder)
}

func (b SelectBuilder) CrossJoinLateralSelect(sel Sqlizer, alias string) SelectBuilder {
	sel = forceQuestionPlaceholders(sel)
	part := joinLateralSelectPart{joinType: "CROSS JOIN", sel: sel, alias: alias, on: nil}
	return builder.Append(b, "Joins", part).(SelectBuilder)
}
func forceQuestionPlaceholders(s Sqlizer) Sqlizer {
	switch v := any(s).(type) {
	case SelectBuilder:
		return v.PlaceholderFormat(Question)
	case CommonTableExpressionsBuilder:
		return v.PlaceholderFormat(Question)
	default:
		return s
	}
}
