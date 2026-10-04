use super::WriteBatch;

#[test]
fn zero_and_sufficient_reservations_preserve_existing_mutations() {
    let mut batch = WriteBatch::default().put(b"key".to_vec(), b"value".to_vec());
    let before = batch.clone();
    batch.try_reserve_mutations(0).unwrap();
    batch.try_reserve_mutations(4).unwrap();
    assert_eq!(batch, before);
}

#[test]
fn capacity_overflow_is_fallible_and_preserves_existing_mutations() {
    let mut batch = WriteBatch::default().put(b"key".to_vec(), b"value".to_vec());
    let before = batch.clone();
    assert!(batch.try_reserve_mutations(usize::MAX).is_err());
    assert_eq!(batch, before);
}
