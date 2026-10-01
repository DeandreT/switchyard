use super::*;

/// Maximum owner discovery scans in one native queue-list request.
/// Configuration reads for returned queues are outside this discovery limit.
pub const MAX_NATIVE_QUEUE_SCAN_ROUNDS: usize = 16;
/// Maximum rows returned by native queue discovery scans, including lookahead.
/// This is a metadata-work bound, not a bound on bytes or resident memory.
pub const MAX_NATIVE_QUEUE_SCAN_ROWS: usize = 4_096;

const TOKEN_PREFIX: &str = "v1.";
const SCAN_TOKEN_PREFIX: &str = "queue.scan.v1.";

impl NativeAdminService {
    fn decode_queue_cursor(&self, token: &str) -> Result<Option<QueueCursor>, Status> {
        if token.is_empty() {
            return Ok(None);
        }
        if token.len() > MAX_PAGE_TOKEN_BYTES {
            return Err(Status::invalid_argument("page token is too large"));
        }
        let (raw, encoded) = if let Some(encoded) = token.strip_prefix(SCAN_TOKEN_PREFIX) {
            (true, encoded)
        } else {
            (
                false,
                token
                    .strip_prefix(TOKEN_PREFIX)
                    .ok_or_else(|| Status::invalid_argument("unsupported page token version"))?,
            )
        };
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| Status::invalid_argument("malformed page token"))?;
        if raw && URL_SAFE_NO_PAD.encode(&bytes) != encoded {
            return Err(Status::invalid_argument("noncanonical page token"));
        }
        let cursor = GetEntityRequest::decode(bytes.as_slice())
            .map_err(|_| Status::invalid_argument("malformed page token"))?;
        if cursor.encode_to_vec() != bytes {
            return Err(Status::invalid_argument("noncanonical page token"));
        }
        if cursor.namespace != self.namespace.as_str() {
            return Err(Status::invalid_argument(
                "page token belongs to another namespace",
            ));
        }
        let entity = if raw {
            EntityPath::new(cursor.path)
                .map_err(|error| Status::invalid_argument(error.to_string()))?
        } else {
            self.entity_path(&cursor.path)?
        };
        Ok(Some(QueueCursor {
            namespace: self.namespace.clone(),
            entity,
        }))
    }

    fn encode_queue_cursor(&self, path: &EntityPath, raw: bool) -> String {
        let bytes = GetEntityRequest {
            namespace: self.namespace.as_str().to_owned(),
            path: path.as_str().to_owned(),
        }
        .encode_to_vec();
        let prefix = if raw { SCAN_TOKEN_PREFIX } else { TOKEN_PREFIX };
        format!("{prefix}{}", URL_SAFE_NO_PAD.encode(bytes))
    }

    pub(super) async fn list_queues(
        &self,
        input: &ListEntitiesRequest,
        page_size: usize,
    ) -> Result<ListEntitiesResponse, Status> {
        let mut cursor = self.decode_queue_cursor(&input.page_token)?;
        let mut paths = Vec::with_capacity(page_size);
        let mut rounds = 0;
        let mut remaining_rows = MAX_NATIVE_QUEUE_SCAN_ROWS;
        let mut visible_lookahead = false;
        let mut exhausted = false;
        while rounds < MAX_NATIVE_QUEUE_SCAN_ROUNDS && remaining_rows > 1 {
            let limit = (page_size - paths.len() + 1)
                .min(MAX_QUEUE_PAGE_SIZE)
                .min(remaining_rows - 1);
            let page = self
                .broker
                .queues_page(Some(self.namespace.clone()), cursor.take(), limit)
                .await
                .map_err(submit_status)?;
            rounds += 1;
            // Domain pages retain at most `limit` rows plus one unreturned backend lookahead.
            remaining_rows -= page.queues.len() + usize::from(page.continuation.is_some());
            for (_, path) in &page.queues {
                if path.is_dead_letter_queue() || path.is_subscription_path() {
                    continue;
                }
                if paths.len() == page_size {
                    visible_lookahead = true;
                    break;
                }
                paths.push(path.clone());
            }
            if visible_lookahead {
                break;
            }
            if page.continuation.is_none() {
                exhausted = true;
                break;
            }
            // All returned rows were consumed; the backend lookahead remains after this cursor.
            cursor = page.continuation;
        }
        let next_page_token = if visible_lookahead {
            let path = paths
                .last()
                .ok_or_else(|| Status::internal("missing visible queue cursor"))?;
            self.encode_queue_cursor(path, false)
        } else if exhausted {
            String::new()
        } else {
            let cursor =
                cursor.ok_or_else(|| Status::internal("missing queue discovery cursor"))?;
            self.encode_queue_cursor(&cursor.entity, true)
        };
        let mut entities = Vec::with_capacity(paths.len());
        for path in paths {
            entities.push(self.read_entity(path).await?);
        }
        Ok(ListEntitiesResponse {
            entities,
            next_page_token,
        })
    }
}
