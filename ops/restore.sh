#!/usr/bin/env bash
# Roll back a backup made by backup.sh. Never overwrites: each current file is first renamed to
# <file>.pre-restore-<now>, and the current crontab is saved next to the backup before it is replaced.
#
#   ops/restore.sh <timestamp>                 # show what would be restored
#   ops/restore.sh <timestamp> --yes           # restore the backed-up files
#   ops/restore.sh <timestamp> --yes --crontab # also restore the user's crontab (and root's, if backed up)
set -euo pipefail

ts=${1:?usage: restore.sh <timestamp> [--yes] [--crontab]}
shift
yes=0; crontabs=0
for arg in "$@"; do
  case "$arg" in
    --yes) yes=1 ;;
    --crontab) crontabs=1 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done

home=${JOBCRAWL_OPS_HOME:-$HOME/jobcrawl-ops}
src="$home/backups/$ts"
[ -f "$src/MANIFEST" ] || { echo "no backup at $src" >&2; exit 1; }
now=$(date -u +%Y%m%dT%H%M%SZ)
user=$(id -un)

cat "$src/MANIFEST"
echo
while read -r _ path _; do
  if [ -e "$path" ]; then echo "would rename $path -> $path.pre-restore-$now, then restore it"
  else echo "would restore $path"; fi
done < <(grep '^file: ' "$src/MANIFEST")
[ "$crontabs" = 1 ] && echo "would restore crontab for $user$([ -f "$src/crontab.root" ] && echo ' and root')"

if [ "$yes" != 1 ]; then
  echo; echo "dry run - re-run with --yes to restore"
  exit 0
fi

while read -r _ path _; do
  [ -e "$path" ] && mv -- "$path" "$path.pre-restore-$now"
  cp -a -- "$src/files$path" "$path"
  echo "restored $path"
done < <(grep '^file: ' "$src/MANIFEST")

if [ "$crontabs" = 1 ]; then
  crontab -l > "$src/crontab.$user.pre-restore-$now" 2>/dev/null || true
  crontab "$src/crontab.$user"
  echo "restored crontab for $user (previous saved as $src/crontab.$user.pre-restore-$now)"
  if [ -f "$src/crontab.root" ]; then
    sudo -n crontab -l > "$src/crontab.root.pre-restore-$now" 2>/dev/null || true
    sudo -n crontab "$src/crontab.root"
    echo "restored root crontab (previous saved as $src/crontab.root.pre-restore-$now)"
  fi
fi
