package dev.awsqe.queueapp.ui

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
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
import dev.awsqe.queueapp.model.StatsResult

@Composable
fun DashboardScreen(vm: QueueViewModel) {
    val state by vm.dashboard.collectAsState()

    LaunchedEffect(Unit) { vm.refreshDashboard() }

    Column(
        modifier = Modifier.fillMaxSize().padding(12.dp).verticalScroll(rememberScrollState()),
        verticalArrangement = Arrangement.spacedBy(12.dp),
    ) {
        Row(modifier = Modifier.fillMaxWidth(), verticalAlignment = Alignment.CenterVertically) {
            Text("Overview", style = MaterialTheme.typography.titleLarge, modifier = Modifier.weight(1f))
            if (state.loading) {
                CircularProgressIndicator(modifier = Modifier.height(20.dp).padding(end = 12.dp))
            }
            IconButton(onClick = { vm.refreshDashboard() }) {
                Icon(Icons.Filled.Refresh, contentDescription = "Refresh")
            }
        }

        state.error?.let {
            Text(it, color = MaterialTheme.colorScheme.error)
        }

        val stats = state.stats
        if (stats == null) {
            if (!state.loading && state.error == null) {
                Text("Pull to refresh.", color = MaterialTheme.colorScheme.onSurfaceVariant)
            }
        } else {
            MetricsRow(stats)
            QueueBreakdownCard(stats)
            HostsCard(stats)
        }
    }
}

@Composable
private fun MetricsRow(stats: StatsResult) {
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.spacedBy(8.dp),
    ) {
        MetricTile(
            modifier = Modifier.weight(1f),
            label = "Running",
            value = stats.running_count.toString(),
        )
        MetricTile(
            modifier = Modifier.weight(1f),
            label = "Hosts",
            value = stats.host_total.toString(),
            sublabel = "${stats.cooldown_hosts.size} on cooldown",
        )
        MetricTile(
            modifier = Modifier.weight(1f),
            label = "Empty",
            value = formatPercent(stats.fraction_empty),
            sublabel = "${stats.host_total - stats.running_count}/${stats.host_total} idle",
        )
    }
}

@Composable
private fun MetricTile(modifier: Modifier, label: String, value: String, sublabel: String? = null) {
    Card(modifier = modifier, elevation = CardDefaults.cardElevation(defaultElevation = 1.dp)) {
        Column(Modifier.padding(12.dp)) {
            Text(label, style = MaterialTheme.typography.labelMedium, color = MaterialTheme.colorScheme.onSurfaceVariant)
            Text(value, style = MaterialTheme.typography.headlineMedium)
            sublabel?.let {
                Text(it, style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
            }
        }
    }
}

@Composable
private fun QueueBreakdownCard(stats: StatsResult) {
    Card(modifier = Modifier.fillMaxWidth(), elevation = CardDefaults.cardElevation(defaultElevation = 1.dp)) {
        Column(Modifier.padding(12.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text("Queued by queue", style = MaterialTheme.typography.titleMedium, modifier = Modifier.weight(1f))
                Text(
                    "total ${stats.queued_count}",
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
            HorizontalDivider(Modifier.padding(vertical = 6.dp))
            val sorted = stats.queued_by_queue.entries.sortedByDescending { it.value }
            if (sorted.isEmpty()) {
                Text("No configured queues.", style = MaterialTheme.typography.bodySmall)
            } else {
                sorted.forEach { (queueName, count) ->
                    Row(
                        modifier = Modifier.fillMaxWidth().padding(vertical = 3.dp),
                        verticalAlignment = Alignment.CenterVertically,
                    ) {
                        Text(queueName, fontFamily = FontFamily.Monospace, modifier = Modifier.weight(1f))
                        Text(count.toString(), style = MaterialTheme.typography.bodyMedium)
                    }
                }
            }
        }
    }
}

@Composable
private fun HostsCard(stats: StatsResult) {
    Card(modifier = Modifier.fillMaxWidth(), elevation = CardDefaults.cardElevation(defaultElevation = 1.dp)) {
        Column(Modifier.padding(12.dp)) {
            Text("Hosts", style = MaterialTheme.typography.titleMedium)
            HorizontalDivider(Modifier.padding(vertical = 6.dp))
            HostRow("running", stats.running_hosts)
            HostRow("cooldown", stats.cooldown_hosts)
            HostRow("pool", stats.host_pool)
        }
    }
}

@Composable
private fun HostRow(label: String, hosts: List<String>) {
    Row(modifier = Modifier.fillMaxWidth().padding(vertical = 3.dp)) {
        Text(
            label,
            style = MaterialTheme.typography.labelMedium,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.weight(0.25f),
        )
        Box(modifier = Modifier.weight(0.75f)) {
            Text(
                if (hosts.isEmpty()) "(none)" else hosts.joinToString(", "),
                fontFamily = FontFamily.Monospace,
                style = MaterialTheme.typography.bodySmall,
            )
        }
    }
}

private fun formatPercent(fraction: Double): String {
    val pct = (fraction * 100).coerceIn(0.0, 100.0)
    return "%.0f%%".format(pct)
}
