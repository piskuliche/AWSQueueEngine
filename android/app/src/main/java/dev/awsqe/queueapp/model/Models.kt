package dev.awsqe.queueapp.model

import kotlinx.serialization.Serializable

/**
 * Queue and running-job payloads as returned by the host's `list` and `qstat`
 * RPC methods. Fields mirror shared/queue.py:normalize_job_item — keep this
 * loose: unknown fields are ignored, missing fields default sensibly so the
 * app survives schema drift without a forced upgrade.
 */
@Serializable
data class Job(
    val job_id: String? = null,
    val cmd: String = "",
    val priority: Int = 0,
    val queue: String = "default",
    val hosts: List<String>? = null,
    val preempt: Boolean = false,
    val payload: String? = null,
    val payload_s3_uri: String? = null,
    val started_at: Double? = null,
)

@Serializable
data class ListResult(val jobs: List<Job> = emptyList())

@Serializable
data class QstatResult(val running: Map<String, Job> = emptyMap())

@Serializable
data class TailResult(
    val host: String,
    val ok: Boolean,
    val reason: String? = null,
    val tag: String? = null,
    val out: String = "",
    val err: String = "",
)

/**
 * Mirrors host/rpc.py:handle_stats. The host pre-computes everything the
 * dashboard needs so the phone makes a single round trip per refresh.
 */
@Serializable
data class StatsResult(
    val running_count: Int = 0,
    val queued_count: Int = 0,
    val host_total: Int = 0,
    val host_pool: List<String> = emptyList(),
    val running_hosts: List<String> = emptyList(),
    val cooldown_hosts: List<String> = emptyList(),
    val queue_host_map: Map<String, List<String>> = emptyMap(),
    val queued_by_queue: Map<String, Int> = emptyMap(),
    val fraction_empty: Double = 0.0,
)
