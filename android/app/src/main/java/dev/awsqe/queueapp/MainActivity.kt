package dev.awsqe.queueapp

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.viewModels
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.List
import androidx.compose.material.icons.filled.Dashboard
import androidx.compose.material.icons.filled.PlayArrow
import androidx.compose.material.icons.filled.Settings
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.NavigationBar
import androidx.compose.material3.NavigationBarItem
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.navigation.NavDestination.Companion.hierarchy
import androidx.navigation.NavGraph.Companion.findStartDestination
import androidx.navigation.compose.NavHost
import androidx.navigation.compose.composable
import androidx.navigation.compose.currentBackStackEntryAsState
import androidx.navigation.compose.rememberNavController
import dev.awsqe.queueapp.ui.DashboardScreen
import dev.awsqe.queueapp.ui.QueueViewModel
import dev.awsqe.queueapp.ui.QueuedScreen
import dev.awsqe.queueapp.ui.RunningScreen
import dev.awsqe.queueapp.ui.SettingsScreen
import dev.awsqe.queueapp.ui.TailScreen

class MainActivity : ComponentActivity() {
    private val vm: QueueViewModel by viewModels()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContent {
            MaterialTheme {
                Surface(modifier = Modifier.fillMaxSize(), color = MaterialTheme.colorScheme.background) {
                    AppRoot(vm)
                }
            }
        }
    }
}

private sealed class Tab(val route: String, val label: String, val icon: ImageVector) {
    object Dashboard : Tab("dashboard", "Overview", Icons.Filled.Dashboard)
    object Running : Tab("running", "Running", Icons.Filled.PlayArrow)
    object Queued : Tab("queued", "Queued", Icons.AutoMirrored.Filled.List)
    object Settings : Tab("settings", "Settings", Icons.Filled.Settings)
}

private const val TAIL_ROUTE_TEMPLATE = "tail/{host}"
private fun tailRouteFor(host: String) = "tail/$host"

private val TABS = listOf(Tab.Dashboard, Tab.Running, Tab.Queued, Tab.Settings)

@Composable
private fun AppRoot(vm: QueueViewModel) {
    val nav = rememberNavController()
    val backStack by nav.currentBackStackEntryAsState()
    val currentRoute = backStack?.destination?.route

    Scaffold(
        bottomBar = {
            // Hide tabs while tailing — that screen owns back-navigation to Running.
            if (currentRoute?.startsWith("tail/") != true) {
                NavigationBar {
                    TABS.forEach { dest ->
                        val selected = currentRoute == dest.route ||
                            backStack?.destination?.hierarchy?.any { it.route == dest.route } == true
                        NavigationBarItem(
                            selected = selected,
                            onClick = {
                                nav.navigate(dest.route) {
                                    popUpTo(nav.graph.findStartDestination().id) { saveState = true }
                                    launchSingleTop = true
                                    restoreState = true
                                }
                            },
                            icon = { Icon(dest.icon, contentDescription = dest.label) },
                            label = { Text(dest.label) },
                        )
                    }
                }
            }
        },
    ) { inner ->
        NavHost(
            navController = nav,
            startDestination = Tab.Dashboard.route,
            modifier = Modifier.padding(inner),
        ) {
            composable(Tab.Dashboard.route) { DashboardScreen(vm) }
            composable(Tab.Running.route) {
                RunningScreen(vm, onTapHost = { host -> nav.navigate(tailRouteFor(host)) })
            }
            composable(Tab.Queued.route) { QueuedScreen(vm) }
            composable(Tab.Settings.route) { SettingsScreen(vm) }
            composable(TAIL_ROUTE_TEMPLATE) { entry ->
                val host = entry.arguments?.getString("host") ?: ""
                TailScreen(vm, host)
            }
        }
    }
}
