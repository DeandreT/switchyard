use x509_parser::{extensions::ParsedExtension, prelude::*};

use super::{MAX_CERT_BYTES, MAX_CHAIN_BYTES, WorkloadError};

#[derive(Clone, Copy)]
pub(super) struct Interval {
    pub(super) from: u64,
    pub(super) until: u64,
}

impl Interval {
    pub(super) fn intersection(self, other: Self) -> Result<Self, WorkloadError> {
        let interval = Self {
            from: self.from.max(other.from),
            until: self.until.min(other.until),
        };
        if interval.from >= interval.until {
            return Err(WorkloadError::NotValid);
        }
        Ok(interval)
    }

    #[cfg(feature = "workload-tls")]
    pub(super) fn check(self, now: u64) -> Result<(), WorkloadError> {
        if self.from <= now && now < self.until {
            Ok(())
        } else {
            Err(WorkloadError::NotValid)
        }
    }
}

pub(super) fn certificate(der: &[u8]) -> Result<X509Certificate<'_>, WorkloadError> {
    if der.len() > MAX_CERT_BYTES {
        return Err(WorkloadError::TooLarge);
    }
    let (remaining, cert) =
        parse_x509_certificate(der).map_err(|_| WorkloadError::InvalidCertificate)?;
    if !remaining.is_empty() {
        return Err(WorkloadError::InvalidCertificate);
    }
    cert.extensions_map()
        .map_err(|_| WorkloadError::InvalidCertificate)?;
    if cert
        .extensions()
        .iter()
        .any(|ext| matches!(ext.parsed_extension(), ParsedExtension::ParseError { .. }))
    {
        return Err(WorkloadError::InvalidCertificate);
    }
    Ok(cert)
}

pub(super) fn interval(cert: &X509Certificate<'_>) -> Result<Interval, WorkloadError> {
    let from = u64::try_from(cert.validity().not_before.timestamp())
        .map_err(|_| WorkloadError::InvalidCertificate)?;
    let until = u64::try_from(cert.validity().not_after.timestamp())
        .map_err(|_| WorkloadError::InvalidCertificate)?;
    if from >= until {
        return Err(WorkloadError::InvalidCertificate);
    }
    Ok(Interval { from, until })
}

pub(super) fn require_ca(cert: &X509Certificate<'_>) -> Result<(), WorkloadError> {
    let ca = cert
        .basic_constraints()
        .map_err(|_| WorkloadError::InvalidCertificate)?;
    let ku = cert
        .key_usage()
        .map_err(|_| WorkloadError::InvalidCertificate)?;
    if ca.is_none_or(|bc| !bc.value.ca) || ku.is_none_or(|ku| !ku.value.key_cert_sign()) {
        return Err(WorkloadError::InvalidCertificate);
    }
    Ok(())
}

pub(super) fn pem_certificates(pem: &[u8], maximum: usize) -> Result<Vec<Vec<u8>>, WorkloadError> {
    // Framing guard only; PEM/base64 and DER are parsed by maintained libraries.
    if pem.len() > MAX_CHAIN_BYTES * 2 {
        return Err(WorkloadError::TooLarge);
    }
    let mut remaining = pem.trim_ascii();
    let mut certificates = Vec::new();
    let mut total = 0;
    while !remaining.is_empty() {
        if !remaining.starts_with(b"-----BEGIN CERTIFICATE-----") {
            return Err(WorkloadError::InvalidCertificate);
        }
        let (item, rest) = rustls_pemfile::read_one_from_slice(remaining)
            .map_err(|_| WorkloadError::InvalidCertificate)?
            .ok_or(WorkloadError::InvalidCertificate)?;
        let rustls_pemfile::Item::X509Certificate(der) = item else {
            return Err(WorkloadError::InvalidCertificate);
        };
        total += der.len();
        if certificates.len() >= maximum || total > MAX_CHAIN_BYTES || der.len() > MAX_CERT_BYTES {
            return Err(WorkloadError::TooLarge);
        }
        certificates.push(der.to_vec());
        remaining = rest.trim_ascii();
    }
    if certificates.is_empty() {
        return Err(WorkloadError::InvalidCertificate);
    }
    Ok(certificates)
}
