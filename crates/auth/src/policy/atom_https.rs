use super::{ResourceScope, ResourceScopeError, Url, decode_path_segment};

impl ResourceScope {
    pub(crate) fn parse_atom_https(audience: &str) -> Result<Self, ResourceScopeError> {
        if audience
            .chars()
            .any(|value| value.is_control() || value.is_whitespace())
            || audience.contains(['\\', '?', '#'])
        {
            return Err(ResourceScopeError::InvalidUri);
        }
        let (scheme, remainder) = audience
            .split_once("://")
            .ok_or(ResourceScopeError::InvalidUri)?;
        if !scheme.eq_ignore_ascii_case("https") {
            return Err(ResourceScopeError::InvalidUri);
        }
        let (authority, raw_path) = remainder.split_once('/').unwrap_or((remainder, ""));
        if authority.is_empty() || authority.contains(['@', '%']) {
            return Err(ResourceScopeError::InvalidUri);
        }

        // Validate literal segments before URL parsing can erase dot segments or
        // reinterpret backslashes. Decode exactly once; never recurse on escapes.
        let path = if raw_path.is_empty() {
            Vec::new()
        } else {
            let raw_path = raw_path.strip_suffix('/').unwrap_or(raw_path);
            if raw_path.is_empty() {
                return Err(ResourceScopeError::InvalidPath);
            }
            raw_path
                .split('/')
                .map(|segment| {
                    let decoded = decode_path_segment(segment)?;
                    if decoded.contains('\\') {
                        return Err(ResourceScopeError::InvalidPath);
                    }
                    Ok(decoded)
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        let url = Url::parse(audience).map_err(|_| ResourceScopeError::InvalidUri)?;
        if url.scheme() != "https"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.port().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(ResourceScopeError::InvalidUri);
        }
        let host = url
            .host_str()
            .filter(|value| !value.is_empty())
            .ok_or(ResourceScopeError::InvalidUri)?
            .to_ascii_lowercase();
        Ok(Self { host, path })
    }
}
