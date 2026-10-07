use super::super::duration;
use super::*;

#[test]
fn duration_lexer_is_integral_checked_and_canonical() {
    for (text, millis, canonical) in [
        ("PT0S", 0, "PT0S"),
        ("P1D", 86_400_000, "P1D"),
        ("PT60S", 60_000, "PT1M"),
        ("PT1.1S", 1_100, "PT1.1S"),
        ("PT1.0100000S", 1_010, "PT1.01S"),
        ("P1DT2H3M4.005S", 93_784_005, "P1DT2H3M4.005S"),
        (
            "PT922337203685.477S",
            duration::MAX_DURATION_MILLIS,
            "P10675199DT2H48M5.477S",
        ),
    ] {
        assert_eq!(duration::parse(text), Ok(millis), "{text}");
        assert_eq!(duration::format(millis), canonical);
        assert_eq!(duration::parse(canonical), Ok(millis));
    }
}

#[test]
fn duration_refuses_calendar_sign_precision_order_and_overflow_ambiguity() {
    for text in [
        "P",
        "PT",
        "P1DT",
        "P1Y",
        "P1M",
        "-PT1S",
        "+PT1S",
        "pt1s",
        "PT1e2S",
        "PT1,S",
        "PT.1S",
        "PT1.S",
        "PT1.00000001S",
        "PT1.0001S",
        "PT1.0000001S",
        "PT1S1M",
        "P1D1D",
        "PT1H1H",
        "PT1ST1S",
        "PT1.0H",
        "PT1Sx",
        "PT18446744073709551616S",
        "P18446744073709551615D",
        "PT922337203685.478S",
        "P10675199DT2H48M5.4775807S",
    ] {
        assert_eq!(
            duration::parse(text),
            Err(AtomXmlError::InvalidDefinition),
            "{text}"
        );
    }
    assert_eq!(
        parse("<DefaultMessageTimeToLive>PT922337203685.477S</DefaultMessageTimeToLive>")
            .unwrap()
            .config
            .default_time_to_live_millis,
        Some(duration::MAX_DURATION_MILLIS)
    );
}
