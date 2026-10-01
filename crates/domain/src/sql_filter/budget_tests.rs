use super::*;

#[test]
fn compile_allowances_can_only_tighten_local_ceilings() {
    let mut budget = SqlCompileBudget::with_limits(usize::MAX, usize::MAX, usize::MAX);
    assert_eq!(budget.limits.source_bytes, MAX_SQL_COMPILE_SOURCE_BYTES);
    assert_eq!(budget.limits.tokens, MAX_SQL_COMPILE_TOKENS);
    assert_eq!(budget.limits.nodes, MAX_SQL_COMPILE_NODES);
    assert_eq!(
        budget.charge_source(MAX_SQL_COMPILE_SOURCE_BYTES + 1),
        Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::AggregateSourceBytes,
            maximum: MAX_SQL_COMPILE_SOURCE_BYTES,
        })
    );
    assert_eq!(budget.used(), SqlCompileUsage::default());
    assert_eq!(
        SqlCompileBudget::with_limits(0, 0, 0).limits,
        SqlCompileUsage::default()
    );
}

#[test]
fn evaluation_allowances_can_only_tighten_local_ceilings() {
    let mut budget = SqlEvaluationBudget::with_limits(usize::MAX, usize::MAX);
    assert_eq!(budget.limits.work, crate::MAX_TOPIC_RULE_MATCH_WORK);
    assert_eq!(
        budget.limits.comparison_bytes,
        crate::MAX_TOPIC_RULE_COMPARISON_BYTES
    );
    assert_eq!(
        budget.charge_work(crate::MAX_TOPIC_RULE_MATCH_WORK + 1),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::WorkUnits,
            maximum: crate::MAX_TOPIC_RULE_MATCH_WORK,
        })
    );
    assert_eq!(
        budget.charge_bytes(crate::MAX_TOPIC_RULE_COMPARISON_BYTES + 1),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::ComparisonBytes,
            maximum: crate::MAX_TOPIC_RULE_COMPARISON_BYTES,
        })
    );
    assert_eq!(budget.used(), SqlEvaluationUsage::default());
    assert_eq!(
        SqlEvaluationBudget::with_limits(0, 0).limits,
        SqlEvaluationUsage::default()
    );
}

#[test]
fn exhausted_charges_leave_usage_unchanged() {
    let mut compile = SqlCompileBudget::with_limits(2, 1, 1);
    compile.charge_source(2).expect("exact source allowance");
    compile.charge_tokens(1).expect("exact token allowance");
    compile.charge_node().expect("exact node allowance");
    let before = compile.used();
    assert!(compile.charge_source(1).is_err());
    assert!(compile.charge_tokens(1).is_err());
    assert!(compile.charge_node().is_err());
    assert_eq!(compile.used(), before);

    let mut evaluate = SqlEvaluationBudget::with_limits(2, 3);
    evaluate.charge_work(2).expect("exact work allowance");
    evaluate.charge_bytes(3).expect("exact byte allowance");
    let before = evaluate.used();
    assert!(evaluate.charge_work(1).is_err());
    assert!(evaluate.charge_bytes(1).is_err());
    assert_eq!(evaluate.used(), before);
}

#[test]
fn arithmetic_overflow_is_a_refusal_not_saturation() {
    let mut used = usize::MAX;
    assert!(
        SqlCompileBudget::charge(&mut used, usize::MAX, 1, SqlCompileLimit::AggregateTokens)
            .is_err()
    );
    assert_eq!(used, usize::MAX);
    assert!(
        SqlEvaluationBudget::charge(&mut used, usize::MAX, 1, SqlEvaluationLimit::WorkUnits)
            .is_err()
    );
    assert_eq!(used, usize::MAX);
}
