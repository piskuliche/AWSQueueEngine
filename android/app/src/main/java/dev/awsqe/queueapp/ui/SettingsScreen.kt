package dev.awsqe.queueapp.ui

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.unit.dp
import dev.awsqe.queueapp.settings.HostSettings

@Composable
fun SettingsScreen(vm: QueueViewModel) {
    val current by vm.settings.collectAsState()

    var host by rememberSaveable(current.host) { mutableStateOf(current.host) }
    var port by rememberSaveable(current.port) { mutableStateOf(current.port.toString()) }
    var user by rememberSaveable(current.user) { mutableStateOf(current.user) }
    var key by rememberSaveable(current.privateKeyPem) { mutableStateOf(current.privateKeyPem) }
    var keyPass by rememberSaveable(current.privateKeyPassphrase ?: "") {
        mutableStateOf(current.privateKeyPassphrase ?: "")
    }
    var savedToast by remember { mutableStateOf<String?>(null) }

    Column(
        modifier = Modifier
            .fillMaxSize()
            .verticalScroll(rememberScrollState())
            .padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp),
    ) {
        Text("Queue host connection", style = androidx.compose.material3.MaterialTheme.typography.titleMedium)

        OutlinedTextField(
            value = host,
            onValueChange = { host = it },
            label = { Text("Host (e.g. queue-manager or 1.2.3.4)") },
            modifier = Modifier.fillMaxWidth(),
            singleLine = true,
        )

        OutlinedTextField(
            value = port,
            onValueChange = { port = it.filter { c -> c.isDigit() }.take(5) },
            label = { Text("SSH port") },
            keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Number),
            modifier = Modifier.fillMaxWidth(),
            singleLine = true,
        )

        OutlinedTextField(
            value = user,
            onValueChange = { user = it },
            label = { Text("SSH user") },
            modifier = Modifier.fillMaxWidth(),
            singleLine = true,
        )

        OutlinedTextField(
            value = key,
            onValueChange = { key = it },
            label = { Text("Private key (paste full PEM/OpenSSH text)") },
            modifier = Modifier.fillMaxWidth(),
            minLines = 6,
        )

        OutlinedTextField(
            value = keyPass,
            onValueChange = { keyPass = it },
            label = { Text("Key passphrase (optional)") },
            keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Password),
            modifier = Modifier.fillMaxWidth(),
            singleLine = true,
        )

        Button(
            onClick = {
                val parsed = HostSettings(
                    host = host.trim(),
                    port = port.toIntOrNull() ?: 22,
                    user = user.trim(),
                    privateKeyPem = key,
                    privateKeyPassphrase = keyPass.ifBlank { null },
                )
                vm.updateSettings(parsed)
                savedToast = "Saved."
            },
            modifier = Modifier.fillMaxWidth(),
        ) {
            Text("Save")
        }

        savedToast?.let { Text(it, style = androidx.compose.material3.MaterialTheme.typography.bodyMedium) }
    }
}
