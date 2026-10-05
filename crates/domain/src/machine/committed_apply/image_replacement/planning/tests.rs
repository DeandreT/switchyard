mod data;
mod fixture;
mod validation;

use super::*;
use fixture::{Image, TestResult};

fn pair<'old, 'selected>(
    old: &'old Image,
    selected: &'selected Image,
) -> Result<PlannedCreateSendReplacement<'old, 'selected>> {
    plan_create_send_replacement(
        &old.artifact,
        &selected.artifact,
        &old.expectation(),
        &selected.expectation(),
    )
}
