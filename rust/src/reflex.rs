#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReflexAction {
    StopAndWarn,
    WarnLowFood,
}

/// Deterministic survival guard; it never consumes items or depends on an LLM.
pub fn evaluate(health: f32, food: u32) -> Option<ReflexAction> {
    // Unknown/non-finite health is not evidence that the bot is safe.
    // Fail closed and stop movement until the next valid server snapshot.
    if !health.is_finite() || health <= 6.0 {
        Some(ReflexAction::StopAndWarn)
    } else if food <= 6 {
        Some(ReflexAction::WarnLowFood)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn survival_reflexes_prioritize_critical_health() {
        assert_eq!(evaluate(6.0, 20), Some(ReflexAction::StopAndWarn));
        assert_eq!(evaluate(20.0, 6), Some(ReflexAction::WarnLowFood));
        assert_eq!(evaluate(20.0, 20), None);
        assert_eq!(evaluate(f32::NAN, 20), None);
    }
}
