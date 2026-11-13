#!/usr/bin/env bash
# Snapshot files and both crontabs before a non-git change, so it can be rolled back with restore.sh.
#
#   ops/backup.sh "<reason>" [file ...]
#
# Writes $JOBCRAWL_OPS_HOME/backups/<UTC timestamp>/ (default ~/jobcrawl-ops), readable only by this
# user: files/<absolute path>, crontab.<user>, crontab.root (if sudo can read it) and a MANIFEST with
# the reason and a sha256 per file. Backups older than 30 days are removed. Prints the backup folder.
set -euo pipefail

reason=${1:?usage: backup.sh \"<reason>\" [file ...]}
shift
home=${JOBCRAWL_OPS_HOME:-$HOME/jobcrawl-ops}
ts=$(date -u +%Y%m%dT%H%M%SZ)
dest="$home/backups/$ts"

umask 077
mkdir -p "$dest/files"

crontab -l > "$dest/crontab.$(id -un)" 2>/dev/null || echo "# no crontab" > "$dest/crontab.$(id -un)"
sudo -n crontab -l > "$dest/crontab.root" 2>/dev/null || rm -f "$dest/crontab.root"

{
  echo "timestamp: $ts"
  echo "reason: $reason"
  echo "user: $(id -un)"
} > "$dest/MANIFEST"

for f in "$@"; do
  abs=$(realpath -e -- "$f")
  case "$abs" in
    */jobcrawl/settings.py|*.pem|*.key|*/.env)
      echo "refusing to back up a file that holds credentials: $abs" >&2
      exit 1 ;;
  esac
  cp -a --parents -- "$abs" "$dest/files/"
  echo "file: $abs $(sha256sum -- "$abs" | cut -d' ' -f1)" >> "$dest/MANIFEST"
done

find "$home/backups" -mindepth 1 -maxdepth 1 -type d -mtime +30 -exec rm -rf -- {} +
echo "$dest"
