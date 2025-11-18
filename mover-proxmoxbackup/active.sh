#! /bin/bash

set -e -o pipefail

echo "VolSync proxmox-backup-client container version: ${version:-unknown}"

SCRIPT_FULLPATH="$(realpath "$0")"
SCRIPT="$(basename "$SCRIPT_FULLPATH")"
SCRIPT_DIR="$(dirname "$SCRIPT_FULLPATH")"

# Do not do this debug mover code if this is already the
# mover script copy in /tmp
if [[ $DEBUG_MOVER -eq 1 && "$SCRIPT_DIR" != "/tmp" ]]; then
  MOVER_SCRIPT_COPY="/tmp/$SCRIPT"
  cp "$SCRIPT_FULLPATH" "$MOVER_SCRIPT_COPY"

  END_DEBUG_FILE="/tmp/exit-debug-if-removed"
  touch "$END_DEBUG_FILE"

  echo ""
  echo "##################################################################"
  echo "DEBUG_MOVER is enabled, this pod will sleep indefinitely."
  echo ""
  echo "The mover script that would normally run has been copied to"
  echo "$MOVER_SCRIPT_COPY".
  echo ""
  echo "To debug, you can modify this file and run it with:"
  echo "$MOVER_SCRIPT_COPY" "$@"
  echo ""
  echo "If you wish to exit this pod after debugging, delete the"
  echo "file $END_DEBUG_FILE from the system."
  echo "##################################################################"

  # Wait for user to delete the file before exiting
  while [[ -f "${END_DEBUG_FILE}" ]]; do
    sleep 10
  done

  echo ""
  echo "##################################################################"
  echo "Debug done, exiting."
  echo "##################################################################"
  sleep 2
  exit 0
fi

function error {
    local rc="$1"
    shift
    echo "error: $*"
    exit "$rc"
}

# Proxmox Backup Client environment variables
# PBS_REPOSITORY, PBS_USERNAME, PBS_PASSWORD, PBS_ENCRYPTION_PASSWORD, PBS_FINGERPRINT
# should be set as environment variables as they are sensitive or frequently used.
# The original script used RCLONE_CONFIG_SECTION, which would now map to the 'datastore'
# part of the PBS_REPOSITORY, or the full PBS_REPOSITORY if it's already complete.
# For simplicity, we'll assume PBS_REPOSITORY is fully defined outside this script.

[[ -n "${PBS_BACKUP_ID_SUFFIX}" ]] || error 1 "PBS_BACKUP_ID_SUFFIX must be defined (used for backup-id)"
[[ -n "${DIRECTION}" ]] || error 1 "DIRECTION must be defined"
[[ -n "${PRIVILEGED_MOVER}" ]] || error 1 "PRIVILEGED_MOVER must be defined"
[[ -n "${PBS_REPOSITORY}" ]] || error 1 "PBS_REPOSITORY must be defined"

# Proxmox Backup Client specific options
# For simplicity, we'll use a fixed backup-id based on hostname and the destination path.
# In a real VolSync integration, PBS_BACKUP_ID_SUFFIX might be a separate env var.
BACKUP_ID="volsync-$(hostname)-$(echo "${PBS_BACKUP_ID_SUFFIX}" | tr -cd '[:alnum:]_.-')"
ARCHIVE_NAME="data.pxar" # Standard file archive name for pxar backups
BACKUP_TYPE="host" # Assuming this is a host-level file backup

# Optional: Set PBS_LOG for debug output
# Uncomment the following line for verbose debugging if needed.
# export PBS_LOG="debug"

START_TIME=$SECONDS
case "${DIRECTION}" in
source)
    echo "Starting Proxmox Backup Client backup for '${MOUNT_PATH}' to repository '${PBS_REPOSITORY}'"
    # proxmox-backup-client backup <archive_name>:<source_path> --repository <repo> --backup-id <id> --backup-type <type>
    # Note: Proxmox Backup Client automatically handles permissions/ACLs within .pxar archives
    # if the backup source and target are file systems it supports (Linux, specifically).
    # --one-file-system equivalent behavior is default for pxar, if it's a single source path.
    # --create-empty-src-dirs is not directly applicable as PBS backs up content, not just empty dirs.
    # Exclusions can be handled by creating a .pxarexclude file in MOUNT_PATH or by using --exclude flags.
    # For a general directory backup, it's typically 'data.pxar:/MOUNT_PATH'

    # Capture ACLs using getfacl if advanced ACLs are important and pxar might not fully preserve them
    # in all scenarios or for restoration directly on top of existing files (though restore does have --overwrite).
    # However, pxar *does* preserve ACLs. Keeping getfacl/setfacl separate for clarity if needed,
    # but the primary mechanism for file-level backups is the pxar archive itself.
    # If the goal is a complete file system backup, pxar handles this well.
    # For this conversion, we assume pxar handles permissions sufficiently, removing the separate .facl file.

    proxmox-backup-client backup \
        "${ARCHIVE_NAME}:${MOUNT_PATH}" \
        --repository "${PBS_REPOSITORY}" \
        --backup-id "${BACKUP_ID}" \
        --backup-type "${BACKUP_TYPE}" \
        --ns "${PBS_NAMESPACE:-default}" # Use a default namespace or allow user override
    ;;
destination)
    echo "Starting Proxmox Backup Client restore for '${BACKUP_ID}' from repository '${PBS_REPOSITORY}' to '${MOUNT_PATH}'"
    # To restore the latest snapshot, we first need to list them and get the most recent one.
    # The 'latest' flag for restore would be ideal, but is not directly available in PBS client.
    # We will get the latest snapshot of the specific backup ID.

    # Find the latest snapshot for the given backup ID
    # Output format is text, parse with awk/tail. Alternatively, use --output-format json for more robust parsing with `jq`.
    # For simplicity and common use-case, filtering by backup-id and sorting by time.
    LATEST_SNAPSHOT_FULLPATH=$(proxmox-backup-client snapshot list \
        --repository "${PBS_REPOSITORY}" \
        --output-format json \
        | jq -r --arg BID "${BACKUP_ID}" \
            '.[] | select(.backup_id == $BID) | "\(.type)/\(.backup_id)/\(.backup_time)Z"' \
        | sort -r | head -n 1)

    if [[ -z "${LATEST_SNAPSHOT_FULLPATH}" ]]; then
        error 1 "No snapshots found for backup ID '${BACKUP_ID}' in repository '${PBS_REPOSITORY}'."
    fi

    echo "Found latest snapshot: ${LATEST_SNAPSHOT_FULLPATH}"

    # Restore command: proxmox-backup-client restore <snapshot> <archive> <target_dir>
    # --overwrite and --allow-existing-dirs are crucial for restoring into a potentially non-empty MOUNT_PATH
    proxmox-backup-client restore \
        "${LATEST_SNAPSHOT_FULLPATH}" \
        "${ARCHIVE_NAME}" \
        "${MOUNT_PATH}" \
        --repository "${PBS_REPOSITORY}" \
        --ns "${PBS_NAMESPACE:-default}" \
        --overwrite \
        --allow-existing-dirs \
        --log-level debug # Using --log-level debug to get verbose output during restore

    # The original script had `sync -f "${MOUNT_PATH}"`
    # For proxmox-backup-client restoring to a directory, the filesystem should be consistent.
    # However, for safety or if the target is a block device, a 'sync' might still be desired.
    # For directory restores, a simple `sync` might not be strictly necessary, but doesn't hurt.
    sync -f "${MOUNT_PATH}" || true

    # The original script had setfacl --restore=/tmp/permissions.facl
    # While .pxar archives store ACLs, if the intent was to apply *additional* or *system-wide*
    # ACLs, this might still be relevant. However, for typical file data, the pxar restore
    # handles them. If `permissions.facl` was meant for a different purpose or for non-pxar data,
    # you might need to reconsider. For a direct Rclone -> PBS conversion,
    # this step is often redundant as PBS should handle native file permissions.
    # We remove the creation/restoration of a separate permissions.facl as pxar is designed
    # to handle permissions within the archive itself.
    ;;
*)
    error 1 "unknown value for DIRECTION: ${DIRECTION}"
    ;;
esac
echo "Proxmox Backup Client operation completed in $(( SECONDS - START_TIME ))s"
