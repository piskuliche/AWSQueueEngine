package dev.awsqe.queueapp.ui

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import dev.awsqe.queueapp.model.Job
import dev.awsqe.queueapp.model.StatsResult
import dev.awsqe.queueapp.model.TailResult
import dev.awsqe.queueapp.rpc.RpcAppError
import dev.awsqe.queueapp.rpc.RpcClient
import dev.awsqe.queueapp.rpc.RpcTransportError
import dev.awsqe.queueapp.settings.HostSettings
import dev.awsqe.queueapp.settings.SettingsStore
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch

data class DashboardUiState(
    val stats: StatsResult? = null,
    val loading: Boolean = false,
    val error: String? = null,
)

data class RunningUiState(
    val jobs: Map<String, Job> = emptyMap(),
    val loading: Boolean = false,
    val error: String? = null,
)

data class QueuedUiState(
    val jobs: List<Job> = emptyList(),
    val loading: Boolean = false,
    val error: String? = null,
)

data class TailUiState(
    val host: String = "",
    val output: String = "",
    val tag: String? = null,
    val unreachable: Boolean = false,
    val loading: Boolean = false,
    val error: String? = null,
    val lastFetched: Long = 0L,
)

class QueueViewModel(app: Application) : AndroidViewModel(app) {
    private val store = SettingsStore(app)
    private val rpc = RpcClient()

    private val _settings = MutableStateFlow(store.load())
    val settings: StateFlow<HostSettings> = _settings.asStateFlow()

    private val _dashboard = MutableStateFlow(DashboardUiState())
    val dashboard: StateFlow<DashboardUiState> = _dashboard.asStateFlow()

    private val _running = MutableStateFlow(RunningUiState())
    val running: StateFlow<RunningUiState> = _running.asStateFlow()

    private val _queued = MutableStateFlow(QueuedUiState())
    val queued: StateFlow<QueuedUiState> = _queued.asStateFlow()

    private val _tail = MutableStateFlow(TailUiState())
    val tail: StateFlow<TailUiState> = _tail.asStateFlow()

    fun updateSettings(updated: HostSettings) {
        store.save(updated)
        _settings.value = updated
    }

    fun refreshDashboard() {
        val s = _settings.value
        if (!s.isComplete()) {
            _dashboard.update { it.copy(error = INCOMPLETE_SETTINGS) }
            return
        }
        viewModelScope.launch {
            _dashboard.update { it.copy(loading = true, error = null) }
            try {
                val stats = rpc.stats(s)
                _dashboard.value = DashboardUiState(stats = stats, loading = false, error = null)
            } catch (e: RpcAppError) {
                _dashboard.update { it.copy(loading = false, error = "Host error: ${e.code}: ${e.rpcMessage}") }
            } catch (e: RpcTransportError) {
                _dashboard.update { it.copy(loading = false, error = "SSH: ${e.detail}") }
            } catch (e: Exception) {
                _dashboard.update { it.copy(loading = false, error = e.message ?: e::class.java.simpleName) }
            }
        }
    }

    fun refreshRunning() {
        val s = _settings.value
        if (!s.isComplete()) {
            _running.update { it.copy(error = INCOMPLETE_SETTINGS) }
            return
        }
        viewModelScope.launch {
            _running.update { it.copy(loading = true, error = null) }
            try {
                val result = rpc.qstat(s)
                _running.value = RunningUiState(jobs = result.running, loading = false, error = null)
            } catch (e: RpcAppError) {
                _running.update { it.copy(loading = false, error = "Host error: ${e.code}: ${e.rpcMessage}") }
            } catch (e: RpcTransportError) {
                _running.update { it.copy(loading = false, error = "SSH: ${e.detail}") }
            } catch (e: Exception) {
                _running.update { it.copy(loading = false, error = e.message ?: e::class.java.simpleName) }
            }
        }
    }

    fun refreshQueued() {
        val s = _settings.value
        if (!s.isComplete()) {
            _queued.update { it.copy(error = INCOMPLETE_SETTINGS) }
            return
        }
        viewModelScope.launch {
            _queued.update { it.copy(loading = true, error = null) }
            try {
                val result = rpc.list(s)
                _queued.value = QueuedUiState(jobs = result.jobs, loading = false, error = null)
            } catch (e: RpcAppError) {
                _queued.update { it.copy(loading = false, error = "Host error: ${e.code}: ${e.rpcMessage}") }
            } catch (e: RpcTransportError) {
                _queued.update { it.copy(loading = false, error = "SSH: ${e.detail}") }
            } catch (e: Exception) {
                _queued.update { it.copy(loading = false, error = e.message ?: e::class.java.simpleName) }
            }
        }
    }

    fun beginTail(host: String) {
        _tail.value = TailUiState(host = host)
        fetchTail(host)
    }

    fun fetchTail(host: String, lines: Int = 200) {
        val s = _settings.value
        if (!s.isComplete()) {
            _tail.update { it.copy(error = INCOMPLETE_SETTINGS) }
            return
        }
        viewModelScope.launch {
            _tail.update { it.copy(loading = true, error = null) }
            try {
                val result: TailResult = rpc.tail(s, host, lines)
                _tail.update {
                    it.copy(
                        host = host,
                        output = result.out,
                        tag = result.tag,
                        unreachable = !result.ok,
                        loading = false,
                        lastFetched = System.currentTimeMillis(),
                        error = if (!result.ok) "Host ${result.host}: ${result.reason ?: "unreachable"}" else null,
                    )
                }
            } catch (e: RpcAppError) {
                _tail.update { it.copy(loading = false, error = "Host error: ${e.code}: ${e.rpcMessage}") }
            } catch (e: RpcTransportError) {
                _tail.update { it.copy(loading = false, error = "SSH: ${e.detail}") }
            } catch (e: Exception) {
                _tail.update { it.copy(loading = false, error = e.message ?: e::class.java.simpleName) }
            }
        }
    }

    private companion object {
        const val INCOMPLETE_SETTINGS = "Settings incomplete. Configure host, user, and key on the Settings tab."
    }
}
