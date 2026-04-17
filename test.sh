ssh eci13 'bash -s' <<'EOF'
REMOTE_LOG_DIR=/home/ubuntu/manager_jobs
GRACE_SECONDS=3

pidfiles=$(ls -1 "${REMOTE_LOG_DIR}"/*.pid 2>/dev/null || true)
roots=""
tracked_pidfiles=""

for pidfile in $pidfiles; do
  pid=$(cat "$pidfile" 2>/dev/null | tr -d '[:space:]')
  if [ -z "$pid" ]; then
    continue
  fi
  if ps -p "$pid" -o pid= >/dev/null 2>&1; then
    roots="$roots $pid"
    tracked_pidfiles="$tracked_pidfiles $pidfile"
  fi
done

if [ -z "$roots" ]; then
  roots=$(pgrep -f '[M]ANAGER_TAG=' || true)
fi

if [ -n "$roots" ]; then
  all=""
  for root in $roots; do
    queue="$root"
    descendants="$root"
    while [ -n "$queue" ]; do
      next=""
      for q in $queue; do
        kids=$(pgrep -P "$q" 2>/dev/null || true)
        if [ -n "$kids" ]; then
          next="$next $kids"
        fi
      done
      queue=$(echo $next)
      if [ -n "$queue" ]; then
        descendants="$descendants $queue"
      fi
    done
    all="$all $descendants"
  done

  final=$(echo $all | tr ' ' '\n' | grep -E '.' | sort -n | uniq | tr '\n' ' ')
  if [ -n "$final" ]; then
    kill -TERM $final 2>/dev/null || true
    sleep "$GRACE_SECONDS"
    kill -KILL $final 2>/dev/null || true
  fi
fi

for pidfile in $tracked_pidfiles; do
  rm -f "$pidfile" 2>/dev/null || true
done

pkill -f '[p]memd.cuda' || true
pkill -f '[p]memd.cuda.MPI' || true
EOF

