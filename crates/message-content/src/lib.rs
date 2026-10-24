//! Transport-free AMQP 1.0 message content, with explicit resource bounds.
//!
//! [`EncodedContent`] owns **only** immutable original bytes. Its [`view`](EncodedContent::view)
//! materializes a transient typed [`Content`]; no second, persistent payload or serde storage
//! contract is introduced. [`decode_value`], [`validate_value`] and [`encode_value`] apply the
//! same bounds to standalone values (including future property updates).
//!
//! All entry points require [`Limits`]. Budgets are shared across the entire operation, not
//! reset per section, container or array element. Lengths and counts are checked before
//! allocating or walking their contents. No third-party recursive decoder is invoked.
//! Depth has an additional implementation ceiling of [`MAX_DEPTH`] to protect the native
//! stack even if a caller supplies an enormous limit. Errors contain no recursive values.
//!
//! Original bytes remain exact; semantic encoding need not reproduce their integer or
//! container encodings. Text, symbol and binary length widths, array constructors, descriptor
//! chains, float/decimal bits and section boundaries are retained in the typed view. Array
//! elements contain their *underlying* values: shared described constructors are stored once
//! on the array rather than copied onto every element. Maps preserve ordered arbitrary keys
//! and duplicate entries; annotation and application-property keys are checked separately.
//! Constructor-less and null-sentinel empty arrays accepted by the existing public codec
//! remain distinct in the typed view. Neither marker is valid for nonempty arrays or inside
//! a described constructor; ordinary array-element constructors still exclude null.
//!
//! Optional list fields distinguish trailing absence from explicit null. A constructed
//! header/properties list with an absent field followed by a present field is unencodable
//! and refused, rather than silently changing absence to null. Defaults are not materialized.
//!
//! This crate does not activate any transport, persistence, queue limit, ID matching,
//! filtering, SQL coercion, broker annotation or store-upgrade behavior.

#![forbid(unsafe_code)]

mod amqp;
mod limits;
mod value;

pub use amqp::{
    AnnotationKey, Annotations, ApplicationProperties, Body, Content, EncodedContent, Field,
    Header, MessageId, Properties, decode_content, encode_content,
};
pub use limits::{Error, Limits, MAX_DEPTH};
pub use value::{
    Array, Binary, Constructor, Descriptor, PrimitiveConstructor, Symbol, Text, Value, Width,
    decode_value, encode_value, validate_value,
};
