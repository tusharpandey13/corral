//! Stable exit codes (SPEC.md section 8).

use corral_proto::bus::is_unreachable;
use corral_proto::envelope::{ApiError, ErrorCode};

pub const OK: i32 = 0;
/// Refused (admit, lock busy) and other "no" answers.
pub const REFUSED: i32 = 1;
pub const USAGE: i32 = 2;
pub const NOT_FOUND: i32 = 3;
pub const AUTH_DENIED: i32 = 4;
pub const UNREACHABLE: i32 = 5;
/// Not admitted now / queued for later (`exec`, hook queue decisions).
pub const QUEUED_LATER: i32 = 75;
/// The job ended killed, lost, cancelled or expired.
pub const JOB_ENDED: i32 = 125;

pub fn for_error(e: &ApiError) -> i32 {
    if is_unreachable(e) {
        return UNREACHABLE;
    }
    match e.error {
        ErrorCode::BadRequest | ErrorCode::VersionUnsupported => USAGE,
        ErrorCode::NotFound => NOT_FOUND,
        ErrorCode::Forbidden | ErrorCode::GrantRequired => AUTH_DENIED,
        ErrorCode::Unavailable => UNREACHABLE,
        ErrorCode::Conflict | ErrorCode::StaleLease | ErrorCode::TooLarge => REFUSED,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use corral_proto::bus::unreachable_error;

    /// U-21 (the client half): errors map to the documented, stable codes.
    #[test]
    fn error_codes() {
        assert_eq!(for_error(&ApiError::bad("x")), USAGE);
        assert_eq!(for_error(&ApiError::not_found("x")), NOT_FOUND);
        assert_eq!(for_error(&ApiError::forbidden("x")), AUTH_DENIED);
        assert_eq!(for_error(&ApiError::new(ErrorCode::GrantRequired, "x")), AUTH_DENIED);
        assert_eq!(for_error(&unreachable_error("x")), UNREACHABLE);
        assert_eq!(for_error(&ApiError::new(ErrorCode::Conflict, "x")), REFUSED);
    }
}
