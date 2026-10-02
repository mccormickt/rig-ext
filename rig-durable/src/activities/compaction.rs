use crate::compaction::{Compaction, CompactionOutput, CompactionRequest};

/// Run one compaction round. A worker without a registered [`Compaction`]
/// fails closed; the workflow records the error and keeps its context.
pub async fn compact(
    compaction: Option<&Compaction>,
    request: CompactionRequest,
) -> Result<CompactionOutput, String> {
    let compaction = compaction.ok_or("compaction is not registered on this worker")?;
    compaction.run(request).await
}
