package dev.awsqe.queueapp.ui

import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
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
import androidx.compose.material3.HorizontalDivider
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

@Composable
fun QueueScreen(vm: QueueViewModel, onTapRunning: (String) -> Unit) {
    val state by vm.queue.collectAsState()

    LaunchedEffect(Unit) { vm.refreshQueue() }

    Column(modifier = Modifier.fillMaxSize().padding(12.dp)) {
        Row(
            modifier = Modifier.fillMaxWidth(),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            Text("Queue", style = MaterialTheme.typography.titleLarge, modifier = Modifier.weight(1f))
            if (state.loading) {
                CircularProgressIndicator(modifier = Modifier.height(20.dp).padding(end = 12.dp))
            }
            IconButton(onClick = { vm.refreshQueue() }) {
                Icon(Icons.Filled.Refresh, contentDescription = "Refresh")
            }
        }

        state.error?.let {
            Text(it, color = MaterialTheme.colorScheme.error, modifier = Modifier.padding(vertical = 8.dp))
        }

        LazyColumn(verticalArrangement = Arrangement.spacedBy(6.dp)) {
            item {
                SectionHeader("Running (${state.running.size})")
            }
            if (state.running.isEmpty()) {
                item { EmptyHint("No jobs running.") }
            } else {
                items(state.running.entries.toList(), key = { it.key }) { (host, job) ->
                    RunningCard(host, job) { onTapRunning(host) }
                }
            }

            item {
                Spacer(Modifier.height(8.dp))
                SectionHeader("Queued (${state.queued.size})")
            }
            if (state.queued.isEmpty()) {
                item { EmptyHint("Queue is empty.") }
            } else {
                items(state.queued, key = { it.job_id ?: it.cmd }) { job ->
                    QueuedCard(job)
                }
            }
        }
    }
}

@Composable
private fun SectionHeader(text: String) {
    Text(
        text,
        style = MaterialTheme.typography.titleMedium,
        modifier = Modifier.padding(top = 6.dp, bottom = 4.dp),
    )
    HorizontalDivider()
}

@Composable
private fun EmptyHint(text: String) {
    Box(Modifier.fillMaxWidth().padding(vertical = 8.dp), contentAlignment = Alignment.Center) {
        Text(text, color = MaterialTheme.colorScheme.onSurfaceVariant)
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
            val started = job.started_at?.let { java.text.SimpleDateFormat.getDateTimeInstance().format(java.util.Date((it * 1000).toLong())) }
            if (started != null) {
                Text("started: $started", style = MaterialTheme.typography.labelSmall)
            }
            Text("tap to tail log →", style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.primary)
        }
    }
}

@Composable
private fun QueuedCard(job: Job) {
    Card(modifier = Modifier.fillMaxWidth(), elevation = CardDefaults.cardElevation(defaultElevation = 1.dp)) {
        Column(Modifier.padding(12.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text(job.job_id ?: "(no id)", style = MaterialTheme.typography.titleSmall, modifier = Modifier.weight(1f))
                Text("p=${job.priority}", style = MaterialTheme.typography.labelSmall)
            }
            Text(job.cmd, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.bodySmall)
            val hostHint = job.hosts?.takeIf { it.isNotEmpty() }?.joinToString(",") ?: "any"
            Text("queue=${job.queue}  hosts=$hostHint", style = MaterialTheme.typography.labelSmall)
        }
    }
}
