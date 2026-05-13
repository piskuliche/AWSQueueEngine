package dev.awsqe.queueapp.rpc

import dev.awsqe.queueapp.model.ListResult
import dev.awsqe.queueapp.model.QstatResult
import dev.awsqe.queueapp.model.TailResult
import dev.awsqe.queueapp.settings.HostSettings
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.put
import net.schmizz.keepalive.KeepAliveProvider
import net.schmizz.sshj.DefaultConfig
import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.connection.channel.direct.Session
import net.schmizz.sshj.transport.verification.PromiscuousVerifier
import java.io.ByteArrayOutputStream
import java.io.IOException

private const val PROTOCOL_VERSION = 1
private const val HOST_RPC_ARGV = "awsqe-host rpc"
private const val DEFAULT_TIMEOUT_MS = 60_000

class RpcTransportError(val detail: String) : IOException(detail)
class RpcAppError(val code: String, val rpcMessage: String) : RuntimeException("$code: $rpcMessage")

class RpcClient(private val json: Json = DEFAULT_JSON) {

    /**
     * Open one SSH connection, invoke `awsqe-host rpc` once, return the
     * `result` JSON element. Throws [RpcTransportError] for SSH or envelope
     * problems and [RpcAppError] for `ok: false` responses.
     *
     * One SSH session per call mirrors the Python client. It's fine for the
     * phone's polling rate (every few seconds), and it keeps the protocol
     * one-shot just like the desktop side.
     */
    suspend fun call(
        settings: HostSettings,
        method: String,
        params: JsonObject = JsonObject(emptyMap()),
    ): JsonElement = withContext(Dispatchers.IO) {
        val request = buildJsonObject {
            put("version", PROTOCOL_VERSION)
            put("method", method)
            put("params", params)
        }
        val requestText = json.encodeToString(JsonObject.serializer(), request)

        val ssh = newSshClient()
        try {
            ssh.connect(settings.host, settings.port)
            ssh.authPublickey(settings.user, settings.keyProvider())

            val session: Session = ssh.startSession()
            try {
                val cmd = session.exec(HOST_RPC_ARGV)
                cmd.outputStream.use { stdin ->
                    stdin.write(requestText.toByteArray(Charsets.UTF_8))
                }
                val stdout = ByteArrayOutputStream()
                cmd.inputStream.copyTo(stdout)
                val stderr = ByteArrayOutputStream()
                cmd.errorStream.copyTo(stderr)
                cmd.join()
                val exit = cmd.exitStatus ?: -1
                if (exit != 0) {
                    val detail = (stderr.toString(Charsets.UTF_8).trim().ifEmpty { stdout.toString(Charsets.UTF_8).trim() })
                        .ifEmpty { "(no output, exit $exit)" }
                    throw RpcTransportError("ssh exit $exit: $detail")
                }
                parseEnvelope(stdout.toString(Charsets.UTF_8))
            } finally {
                runCatching { session.close() }
            }
        } finally {
            runCatching { ssh.disconnect() }
        }
    }

    private fun parseEnvelope(stdout: String): JsonElement {
        val parsed = try {
            json.parseToJsonElement(stdout)
        } catch (e: Exception) {
            throw RpcTransportError("non-JSON response: ${e.message}; raw=${stdout.take(200)}")
        }
        val obj = parsed as? JsonObject ?: throw RpcTransportError("bad envelope (not object)")
        val version = (obj["version"] as? JsonPrimitive)?.intOrNull
        if (version != PROTOCOL_VERSION) {
            throw RpcTransportError("bad envelope version: $version")
        }
        val ok = (obj["ok"] as? JsonPrimitive)?.booleanOrNull ?: false
        if (ok) {
            return obj["result"] ?: JsonObject(emptyMap())
        }
        val err = obj["error"] as? JsonObject ?: throw RpcTransportError("bad error envelope")
        val code = (err["code"] as? JsonPrimitive)?.content ?: "unknown"
        val msg = (err["message"] as? JsonPrimitive)?.content ?: "(no message)"
        throw RpcAppError(code, msg)
    }

    private fun newSshClient(): SSHClient {
        val config = DefaultConfig().apply {
            keepAliveProvider = KeepAliveProvider.KEEP_ALIVE
        }
        val ssh = SSHClient(config)
        ssh.connectTimeout = DEFAULT_TIMEOUT_MS
        ssh.timeout = DEFAULT_TIMEOUT_MS
        // Caller is expected to pin a known-hosts entry once we have a settings
        // UI for it; until then accept any host key. Documented in the README
        // — never ship as-is to anyone but yourself.
        ssh.addHostKeyVerifier(PromiscuousVerifier())
        return ssh
    }

    // High-level wrappers that decode the typed result for each method.
    suspend fun list(settings: HostSettings): ListResult =
        json.decodeFromJsonElement(ListResult.serializer(), call(settings, "list"))

    suspend fun qstat(settings: HostSettings): QstatResult =
        json.decodeFromJsonElement(QstatResult.serializer(), call(settings, "qstat"))

    suspend fun tail(settings: HostSettings, host: String, lines: Int = 200): TailResult {
        val params = buildJsonObject {
            put("host", host)
            put("lines", lines)
        }
        return json.decodeFromJsonElement(TailResult.serializer(), call(settings, "tail", params))
    }

    companion object {
        val DEFAULT_JSON = Json {
            ignoreUnknownKeys = true
            encodeDefaults = false
        }
    }
}
