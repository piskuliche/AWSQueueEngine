# awsqe queue (Android)

Read-only Android client for an `awsqe-host` daemon. Lets you check the queue,
see which jobs are running on which workers, and tail a running job's log file
from your phone. Job submission and queue management are intentionally **not**
supported — this is a viewer.

## How it works

The app speaks the same JSON-over-SSH protocol as the desktop `awsqe-client`:
on each refresh it opens an SSH connection to the queue host, runs
`awsqe-host rpc`, pipes the request JSON to stdin, and parses one response
JSON from stdout. Three methods are used:

- `list`    — queued jobs
- `qstat`   — running jobs, keyed by worker host
- `tail`    — last N lines of a worker's current job log (added in this branch)

Tailing is implemented as a 3-second poll, not a streamed `tail -F`. That keeps
the protocol one-shot and matches the desktop client's request/response model.

## Build

Requires Android SDK 36 (build-tools 34.0.0 will be auto-installed on first
build) and a JDK 17+. Android Studio Panda or later works out of the box —
its bundled JBR is fine as `JAVA_HOME`.

```sh
cd android
export JAVA_HOME=/path/to/android-studio/jbr   # or any JDK 17+
export ANDROID_HOME=/path/to/Android/Sdk
./gradlew assembleDebug
# -> app/build/outputs/apk/debug/app-debug.apk  (~21 MB)
```

The Gradle wrapper (`gradlew`, `gradle/wrapper/`) is checked in, so no
external `gradle` install is needed.

## First-time setup on the phone

1. Sideload the APK (`adb install app-debug.apk`).
2. Open the app — it lands on the Queue tab, which will be empty until you
   configure the host.
3. Switch to the Settings tab and fill in:
   - **Host** — DNS name or IP of the queue host.
   - **Port** — usually 22.
   - **User** — the SSH user that `awsqe-host rpc` runs as on the host.
   - **Private key** — paste the full text of an OpenSSH/PEM private key whose
     public half is in the host's `~/.ssh/authorized_keys`. Generate a phone-
     specific key (`ssh-keygen -t ed25519 -f awsqe-phone`) so you can revoke it
     independently of your laptop key.
   - **Passphrase** — optional, only if the key is encrypted.
4. Tap **Save**. Switch back to Queue and pull the refresh icon.

Settings are stored in `EncryptedSharedPreferences` backed by the Android
Keystore (`AES256_GCM`). Cloud backup is disabled for the prefs file.

## Known sharp edges

- **Host key verification is permissive** in v0.1. The app accepts whatever
  host key the server presents. This is the same trust model as
  `StrictHostKeyChecking=no` and is the single biggest thing to harden before
  using over an untrusted network. A future revision should pin the host key
  on first connect (TOFU) in encrypted prefs.
- **No streaming tail.** Output refreshes every 3 seconds. If your job is
  chatty enough that you need real-time, use the desktop `awsqe-client tail`
  with `watch -n0.5` instead.
- **One host at a time.** The Settings screen configures the queue host you
  talk to. If you operate multiple queue hosts, you'll have to re-paste the
  config to switch.
- **Tail returns whatever `tail_remote_log()` returns** — meaning the host
  SSHes from itself to the worker, then ships the bytes back over the
  phone-to-host SSH session. Two hops, but it reuses the existing logic.

## What lives where

| File | Purpose |
|---|---|
| `app/src/main/java/.../rpc/RpcClient.kt` | sshj-based JSON-over-SSH client. Mirrors `shared/rpc_client.py`. |
| `app/src/main/java/.../model/Models.kt` | DTOs for `list` / `qstat` / `tail` responses. |
| `app/src/main/java/.../settings/SettingsStore.kt` | Encrypted prefs holding the host config + private key. |
| `app/src/main/java/.../ui/QueueViewModel.kt` | Single `AndroidViewModel` driving all three screens. |
| `app/src/main/java/.../ui/QueueScreen.kt` | Lists running + queued jobs. Tap a running row to tail it. |
| `app/src/main/java/.../ui/TailScreen.kt` | Mono-font log view with 3s auto-refresh + pause toggle. |
| `app/src/main/java/.../ui/SettingsScreen.kt` | Host/user/key form. |
| `app/src/main/java/.../MainActivity.kt` | Two-tab nav + tail destination. |
