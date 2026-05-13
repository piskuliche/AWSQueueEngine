package dev.awsqe.queueapp

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.viewModels
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.List
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
import androidx.navigation.NavDestination.Companion.hierarchy
import androidx.navigation.NavGraph.Companion.findStartDestination
import androidx.navigation.compose.NavHost
import androidx.navigation.compose.composable
import androidx.navigation.compose.currentBackStackEntryAsState
import androidx.navigation.compose.rememberNavController
import dev.awsqe.queueapp.ui.QueueScreen
import dev.awsqe.queueapp.ui.QueueViewModel
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

private sealed class Dest(val route: String, val label: String) {
    object Queue : Dest("queue", "Queue")
    object Settings : Dest("settings", "Settings")
    object Tail : Dest("tail/{host}", "Tail") {
        fun routeFor(host: String) = "tail/$host"
    }
}

@Composable
private fun AppRoot(vm: QueueViewModel) {
    val nav = rememberNavController()
    val backStack by nav.currentBackStackEntryAsState()
    val currentRoute = backStack?.destination?.route

    Scaffold(
        bottomBar = {
            // Hide tabs while tailing — that screen owns the back-button to return to Queue.
            if (currentRoute?.startsWith("tail/") != true) {
                NavigationBar {
                    val tabs = listOf(Dest.Queue, Dest.Settings)
                    tabs.forEach { dest ->
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
                            icon = {
                                Icon(
                                    when (dest) {
                                        Dest.Queue -> Icons.AutoMirrored.Filled.List
                                        Dest.Settings -> Icons.Filled.Settings
                                        else -> Icons.AutoMirrored.Filled.List
                                    },
                                    contentDescription = dest.label,
                                )
                            },
                            label = { Text(dest.label) },
                        )
                    }
                }
            }
        },
    ) { inner ->
        NavHost(
            navController = nav,
            startDestination = Dest.Queue.route,
            modifier = Modifier.padding(inner),
        ) {
            composable(Dest.Queue.route) {
                QueueScreen(vm, onTapRunning = { host -> nav.navigate(Dest.Tail.routeFor(host)) })
            }
            composable(Dest.Settings.route) { SettingsScreen(vm) }
            composable(Dest.Tail.route) { entry ->
                val host = entry.arguments?.getString("host") ?: ""
                TailScreen(vm, host)
            }
        }
    }
}
