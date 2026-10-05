use super::*;

#[test]
fn fixed_copy_budget_boundaries_and_overflow() {
    let limits = COMPLETE_CATALOG_STATE_RECORD_LIMITS;
    assert_eq!(
        (
            limits.max_rows,
            limits.max_key_bytes,
            limits.max_value_bytes
        ),
        (65_536, 1024, 266_240)
    );
    assert_eq!(limits.max_total_bytes, 67_108_864);
    assert_eq!(MAX_COMPLETE_CATALOG_STATE_BYTES, 134_225_920);
    assert_eq!(
        payload_bytes(limits.max_total_bytes, Some((8192, 67_108_864))),
        Ok(MAX_COMPLETE_CATALOG_STATE_BYTES)
    );
    assert_eq!(payload_bytes(0, None), Ok(0));
    for (records, catalog) in [
        (limits.max_total_bytes + 1, None),
        (0, Some((8193, 0))),
        (0, Some((0, 67_108_865))),
        (usize::MAX, Some((1, 1))),
        (0, Some((usize::MAX, usize::MAX))),
    ] {
        assert_eq!(
            payload_bytes(records, catalog),
            Err(CatalogReadError::LimitExceeded)
        );
    }
    let mut rows = RecordShape::new();
    for _ in 0..limits.max_rows {
        rows.consume(0, 0).expect("bounded zero-byte rows");
    }
    assert_eq!(rows.rows, limits.max_rows);
    assert_eq!(rows.bytes, 0);
    assert_eq!(rows.check_next_row(), Err(CatalogReadError::LimitExceeded));
    assert_eq!(rows.consume(0, 0), Err(CatalogReadError::LimitExceeded));
    for (key, value) in [(1025, 0), (0, 266_241), (usize::MAX, 1), (1, usize::MAX)] {
        assert_eq!(
            RecordShape::new().consume(key, value),
            Err(CatalogReadError::LimitExceeded)
        );
    }
    let mut bytes = RecordShape::new();
    for _ in 0..256 {
        bytes
            .consume(0, 262_144)
            .expect("exact 64 MiB scalar budget");
    }
    assert_eq!(bytes.bytes, limits.max_total_bytes);
    assert_eq!(bytes.consume(0, 1), Err(CatalogReadError::LimitExceeded));
}

#[test]
fn impossible_output_capacity_is_fallible() {
    assert!(matches!(
        reserve_rows(usize::MAX),
        Err(CatalogReadError::Allocation)
    ));
    assert!(reserve_rows(0).expect("empty reservation").is_empty());
    assert_eq!(
        copy_bytes(b"bounded bytes").expect("small copy"),
        b"bounded bytes"
    );
}
