package dev.awsqe.queueapp.settings

import android.content.Context
import android.content.SharedPreferences
import androidx.security.crypto.EncryptedSharedPreferences
import androidx.security.crypto.MasterKey
import net.schmizz.sshj.userauth.keyprovider.KeyProvider
import net.schmizz.sshj.userauth.keyprovider.OpenSSHKeyFile
import net.schmizz.sshj.userauth.password.PasswordFinder
import net.schmizz.sshj.userauth.password.Resource
import java.io.StringReader

/**
 * The settings the app needs to reach an `awsqe-host`. Mirrors what a desktop
 * user would put in `~/.ssh/config` + `~/.awsqe/client/config.toml`, but
 * compressed into one screen because there's only one host the phone talks to.
 */
data class HostSettings(
    val host: String,
    val port: Int,
    val user: String,
    /** Full PEM/OpenSSH-format private key text. May be encrypted; passphrase below. */
    val privateKeyPem: String,
    val privateKeyPassphrase: String? = null,
) {
    fun keyProvider(): KeyProvider {
        val provider = OpenSSHKeyFile()
        if (privateKeyPassphrase.isNullOrEmpty()) {
            provider.init(StringReader(privateKeyPem))
        } else {
            val passphrase = privateKeyPassphrase
            provider.init(StringReader(privateKeyPem), object : PasswordFinder {
                override fun reqPassword(resource: Resource<*>?): CharArray = passphrase.toCharArray()
                override fun shouldRetry(resource: Resource<*>?): Boolean = false
            })
        }
        return provider
    }

    fun isComplete(): Boolean =
        host.isNotBlank() && port in 1..65535 && user.isNotBlank() && privateKeyPem.isNotBlank()
}

class SettingsStore(context: Context) {
    private val prefs: SharedPreferences by lazy {
        val masterKey = MasterKey.Builder(context)
            .setKeyScheme(MasterKey.KeyScheme.AES256_GCM)
            .build()
        EncryptedSharedPreferences.create(
            context,
            FILE,
            masterKey,
            EncryptedSharedPreferences.PrefKeyEncryptionScheme.AES256_SIV,
            EncryptedSharedPreferences.PrefValueEncryptionScheme.AES256_GCM,
        )
    }

    fun load(): HostSettings = HostSettings(
        host = prefs.getString(KEY_HOST, "") ?: "",
        port = prefs.getInt(KEY_PORT, 22),
        user = prefs.getString(KEY_USER, "") ?: "",
        privateKeyPem = prefs.getString(KEY_PRIVATE_KEY, "") ?: "",
        privateKeyPassphrase = prefs.getString(KEY_PRIVATE_KEY_PASS, null),
    )

    fun save(settings: HostSettings) {
        prefs.edit()
            .putString(KEY_HOST, settings.host.trim())
            .putInt(KEY_PORT, settings.port)
            .putString(KEY_USER, settings.user.trim())
            .putString(KEY_PRIVATE_KEY, settings.privateKeyPem)
            .putString(KEY_PRIVATE_KEY_PASS, settings.privateKeyPassphrase)
            .apply()
    }

    companion object {
        private const val FILE = "awsqe_settings"
        private const val KEY_HOST = "host"
        private const val KEY_PORT = "port"
        private const val KEY_USER = "user"
        private const val KEY_PRIVATE_KEY = "private_key_pem"
        private const val KEY_PRIVATE_KEY_PASS = "private_key_pass"
    }
}
