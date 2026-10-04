use super::*;

fn unlimited() -> ReadLimits {
    ReadLimits {
        max_rows: usize::MAX,
        max_key_bytes: usize::MAX,
        max_value_bytes: usize::MAX,
        max_total_bytes: usize::MAX,
    }
}

#[test]
fn zero_limits_do_not_permit_a_record_even_when_empty() {
    let mut budget = ReadBudget::new(ReadLimits {
        max_rows: 0,
        max_key_bytes: 0,
        max_value_bytes: 0,
        max_total_bytes: 0,
    });
    assert_eq!(
        budget.check_next_row(),
        Err(StorageError::ReadLimitExceeded)
    );
    assert_eq!(budget.consume(0, 0), Err(StorageError::ReadLimitExceeded));
    assert_eq!((budget.rows, budget.bytes), (0, 0));
}

#[test]
fn zero_byte_record_counts_one_row_and_no_bytes() {
    let mut budget = ReadBudget::new(ReadLimits {
        max_rows: 1,
        max_key_bytes: 0,
        max_value_bytes: 0,
        max_total_bytes: 0,
    });
    assert_eq!(budget.check_next_row(), Ok(()));
    assert_eq!((budget.rows, budget.bytes), (0, 0));
    assert_eq!(budget.consume(0, 0), Ok(()));
    assert_eq!((budget.rows, budget.bytes), (1, 0));
    assert_eq!(
        budget.check_next_row(),
        Err(StorageError::ReadLimitExceeded)
    );
}

#[test]
fn exact_individual_and_total_limits_fit() {
    let mut budget = ReadBudget::new(ReadLimits {
        max_rows: 2,
        max_key_bytes: 3,
        max_value_bytes: 4,
        max_total_bytes: 10,
    });
    assert_eq!(budget.consume(3, 4), Ok(()));
    assert_eq!(budget.consume(1, 2), Ok(()));
    assert_eq!((budget.rows, budget.bytes), (2, 10));
}

#[test]
fn every_limit_failure_leaves_both_counters_unchanged() {
    for limits in [
        ReadLimits {
            max_rows: 1,
            ..unlimited()
        },
        ReadLimits {
            max_key_bytes: 1,
            ..unlimited()
        },
        ReadLimits {
            max_value_bytes: 1,
            ..unlimited()
        },
        ReadLimits {
            max_total_bytes: 5,
            ..unlimited()
        },
    ] {
        let mut budget = ReadBudget::new(limits);
        assert_eq!(budget.consume(1, 1), Ok(()));
        assert_eq!(budget.consume(2, 2), Err(StorageError::ReadLimitExceeded));
        assert_eq!((budget.rows, budget.bytes), (1, 2));
    }
}

#[test]
fn record_length_addition_overflow_is_refused_without_mutation() {
    let mut budget = ReadBudget::new(unlimited());
    assert_eq!(
        budget.consume(usize::MAX, 1),
        Err(StorageError::ReadLimitExceeded)
    );
    assert_eq!((budget.rows, budget.bytes), (0, 0));
}

#[test]
fn aggregate_length_addition_overflow_is_refused_without_mutation() {
    let mut budget = ReadBudget::new(unlimited());
    assert_eq!(budget.consume(usize::MAX, 0), Ok(()));
    assert_eq!(budget.consume(0, 1), Err(StorageError::ReadLimitExceeded));
    assert_eq!((budget.rows, budget.bytes), (1, usize::MAX));
}

#[test]
fn row_addition_overflow_is_refused_without_mutation() {
    let mut budget = ReadBudget::new(unlimited());
    budget.rows = usize::MAX;
    budget.bytes = 7;
    assert_eq!(
        budget.check_next_row(),
        Err(StorageError::ReadLimitExceeded)
    );
    assert_eq!(budget.consume(0, 0), Err(StorageError::ReadLimitExceeded));
    assert_eq!((budget.rows, budget.bytes), (usize::MAX, 7));
}

#[test]
fn limit_error_is_static_and_sanitized() {
    let error = StorageError::ReadLimitExceeded;
    assert_eq!(error.to_string(), "storage read limit exceeded");
    assert_eq!(format!("{error:?}"), "ReadLimitExceeded");
}
