package dev.awsqe.queueapp.ui

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

@Composable
fun QueuedScreen(vm: QueueViewModel) {
    val state by vm.queued.collectAsState()

    LaunchedEffect(Unit) { vm.refreshQueued() }

    Column(modifier = Modifier.fillMaxSize().padding(12.dp)) {
        Row(modifier = Modifier.fillMaxWidth(), verticalAlignment = Alignment.CenterVertically) {
            Text(
                "Queued (${state.jobs.size})",
                style = MaterialTheme.typography.titleLarge,
                modifier = Modifier.weight(1f),
            )
            if (state.loading) {
                CircularProgressIndicator(modifier = Modifier.height(20.dp).padding(end = 12.dp))
            }
            IconButton(onClick = { vm.refreshQueued() }) {
                Icon(Icons.Filled.Refresh, contentDescription = "Refresh")
            }
        }

        state.error?.let {
            Text(it, color = MaterialTheme.colorScheme.error, modifier = Modifier.padding(vertical = 8.dp))
        }

        if (state.jobs.isEmpty() && state.error == null && !state.loading) {
            Box(Modifier.fillMaxWidth().padding(vertical = 16.dp), contentAlignment = Alignment.Center) {
                Text("Queue is empty.", color = MaterialTheme.colorScheme.onSurfaceVariant)
            }
        }

        LazyColumn(verticalArrangement = Arrangement.spacedBy(6.dp)) {
            items(state.jobs, key = { it.job_id ?: it.cmd }) { job ->
                QueuedCard(job)
            }
        }
    }
}

@Composable
private fun QueuedCard(job: Job) {
    Card(modifier = Modifier.fillMaxWidth(), elevation = CardDefaults.cardElevation(defaultElevation = 1.dp)) {
        Column(Modifier.padding(12.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text(
                    job.job_id ?: "(no id)",
                    style = MaterialTheme.typography.titleSmall,
                    modifier = Modifier.weight(1f),
                )
                Text("p=${job.priority}", style = MaterialTheme.typography.labelSmall)
            }
            Text(job.cmd, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.bodySmall)
            val hostHint = job.hosts?.takeIf { it.isNotEmpty() }?.joinToString(",") ?: "any"
            Text("queue=${job.queue}  hosts=$hostHint", style = MaterialTheme.typography.labelSmall)
        }
    }
}
