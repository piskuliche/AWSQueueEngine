package dev.awsqe.queueapp

import android.app.Application
import org.bouncycastle.jce.provider.BouncyCastleProvider
import java.security.Security

/**
 * Replaces Android's stripped Bouncy Castle provider with the real one at
 * process start. Without this, sshj fails with
 * `no such algorithm X25519 for provider BC` when negotiating the
 * `curve25519-sha256` KEX against any modern OpenSSH server. Same provider
 * is used for ed25519 host/user keys.
 */
class AwsqeApp : Application() {
    override fun onCreate() {
        super.onCreate()
        Security.removeProvider("BC")
        Security.insertProviderAt(BouncyCastleProvider(), 1)
    }
}
