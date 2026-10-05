use super::*;

#[tokio::test]
async fn attempt_saturation_is_terminal_without_wrapping_or_publication() {
    let (frontier, publisher) = Frontier::new(PairIdentity::new());
    frontier.0.state.lock().unwrap().attempt = u64::MAX;
    assert_eq!(frontier.begin(), Err(Error::Exhausted));
    assert_eq!(
        frontier.receipt(u64::MAX).await.err(),
        Some(Error::Exhausted)
    );
    assert!(!publisher.allows_build(u64::MAX));
    assert_eq!(frontier.begin(), Err(Error::Exhausted));
    assert_eq!(frontier.0.state.lock().unwrap().attempt, u64::MAX);
}
