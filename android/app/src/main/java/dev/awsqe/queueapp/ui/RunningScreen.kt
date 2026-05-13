package dev.awsqe.queueapp.ui

import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Refresh
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import dev.awsqe.queueapp.model.Job
import java.text.SimpleDateFormat
import java.util.Date

@Composable
fun RunningScreen(vm: QueueViewModel, onTapHost: (String) -> Unit) {
    val state by vm.running.collectAsState()

    LaunchedEffect(Unit) { vm.refreshRunning() }

    Column(modifier = Modifier.fillMaxSize().padding(12.dp)) {
        Row(modifier = Modifier.fillMaxWidth(), verticalAlignment = Alignment.CenterVertically) {
            Text(
                "Running (${state.jobs.size})",
                style = MaterialTheme.typography.titleLarge,
                modifier = Modifier.weight(1f),
            )
            if (state.loading) {
                CircularProgressIndicator(modifier = Modifier.height(20.dp).padding(end = 12.dp))
            }
            IconButton(onClick = { vm.refreshRunning() }) {
                Icon(Icons.Filled.Refresh, contentDescription = "Refresh")
            }
        }

        state.error?.let {
            Text(it, color = MaterialTheme.colorScheme.error, modifier = Modifier.padding(vertical = 8.dp))
        }

        if (state.jobs.isEmpty() && state.error == null && !state.loading) {
            Box(Modifier.fillMaxWidth().padding(vertical = 16.dp), contentAlignment = Alignment.Center) {
                Text("No jobs running.", color = MaterialTheme.colorScheme.onSurfaceVariant)
            }
        }

        LazyColumn(verticalArrangement = Arrangement.spacedBy(6.dp)) {
            items(state.jobs.entries.toList(), key = { it.key }) { (host, job) ->
                RunningCard(host, job) { onTapHost(host) }
            }
        }
    }
}

@Composable
private fun RunningCard(host: String, job: Job, onClick: () -> Unit) {
    Card(
        modifier = Modifier.fillMaxWidth().clickable(onClick = onClick),
        elevation = CardDefaults.cardElevation(defaultElevation = 1.dp),
    ) {
        Column(Modifier.padding(12.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text(host, style = MaterialTheme.typography.titleMedium, modifier = Modifier.weight(1f))
                Text(job.job_id ?: "(no id)", style = MaterialTheme.typography.labelSmall)
            }
            Text(job.cmd, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.bodySmall)
            job.started_at?.let { secs ->
                val started = SimpleDateFormat.getDateTimeInstance().format(Date((secs * 1000).toLong()))
                Text("started: $started", style = MaterialTheme.typography.labelSmall)
            }
            Text(
                "tap to tail log →",
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.primary,
            )
        }
    }
}
