//! The initial HTTPS-only, SAS-authorized Atom queue administration profile.

mod listener;
mod request;
pub(crate) mod xml;

pub use listener::{AtomAdminError, AtomAdminListener};
