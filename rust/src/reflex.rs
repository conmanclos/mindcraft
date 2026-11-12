#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReflexAction {
    StopAndWarn,
}

/// Deterministic safety layer; it does not call an LLM and never consumes items.
pub fn evaluate(health: f32) -> Option<ReflexAction> {
    if health.is_finite() && health <= 6.0 {
        Some(ReflexAction::StopAndWarn)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn critical_health_triggers_stop_reflex() {
        assert_eq!(evaluate(6.0), Some(ReflexAction::StopAndWarn));
        assert_eq!(evaluate(20.0), None);
        assert_eq!(evaluate(f32::NAN), None);
    }
}
