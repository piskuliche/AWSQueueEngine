package dev.awsqe.queueapp.ui

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import dev.awsqe.queueapp.model.Job
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

data class QueueUiState(
    val queued: List<Job> = emptyList(),
    val running: Map<String, Job> = emptyMap(),
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

    private val _queue = MutableStateFlow(QueueUiState())
    val queue: StateFlow<QueueUiState> = _queue.asStateFlow()

    private val _tail = MutableStateFlow(TailUiState())
    val tail: StateFlow<TailUiState> = _tail.asStateFlow()

    fun updateSettings(updated: HostSettings) {
        store.save(updated)
        _settings.value = updated
    }

    fun refreshQueue() {
        val s = _settings.value
        if (!s.isComplete()) {
            _queue.update { it.copy(error = "Settings incomplete. Configure host, user, and key on the Settings screen.") }
            return
        }
        viewModelScope.launch {
            _queue.update { it.copy(loading = true, error = null) }
            try {
                val listResult = rpc.list(s)
                val qstatResult = rpc.qstat(s)
                _queue.update {
                    it.copy(
                        queued = listResult.jobs,
                        running = qstatResult.running,
                        loading = false,
                        error = null,
                    )
                }
            } catch (e: RpcAppError) {
                _queue.update { it.copy(loading = false, error = "Host error: ${e.code}: ${e.rpcMessage}") }
            } catch (e: RpcTransportError) {
                _queue.update { it.copy(loading = false, error = "SSH: ${e.detail}") }
            } catch (e: Exception) {
                _queue.update { it.copy(loading = false, error = e.message ?: e::class.java.simpleName) }
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
            _tail.update { it.copy(error = "Settings incomplete.") }
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
}
