//go:build !disable_proxmoxbackup

/*
Copyright 2022 The VolSync authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published
by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package proxmoxbackup

import (
	"regexp"
)

// proxmoxbackupRegex identifies common log lines indicative of progress or success
// for proxmox-backup-client operations (backup and restore), based on observed log formats.
var proxmoxbackupRegex = regexp.MustCompile(
	`^\s*(?:[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:[+-][0-9]{2}:[0-9]{2})?:(?: pbs-plus: \[info\]:)?\s*)?` + // Optional timestamp and 'pbs-plus: [info]:' prefix
		`([sS]tarting (backup|restore):)|` + // e.g., "Starting backup: host/AD-D001/2025-11-18T16:31:03Z"
		`([cC]lient name:)|` + // e.g., "Client name: phoenix-pbs"
		`([sS]tarting backup protocol:)|` + // e.g., "Starting backup protocol: Tue Nov 18 11:31:03 2025"
		`(Downloading previous manifest)|` + // e.g., "Downloading previous manifest (Mon Nov 17 11:31:03 2025)"
		`(Upload directory '.+' to '.+' as .+)|` + // e.g., "Upload directory '/mnt/...' as AD-D001---C.mpxar.didx"
		`(Using previous index as metadata reference for '.+')|` + // e.g., "Using previous index as metadata reference..."
		`(processed \d+\.\d+ (?:GiB|MiB) in \d+m, uploaded \d+\.\d+ (?:GiB|MiB))|` + // Progress, e.g., "processed 2.128 GiB in 1m, uploaded 81.92 MiB"
		`(Change detection summary:)|` + // Summary start
		`(\s*-\s*\d+ total files)|` + // Summary detail
		`(\s*-\s*\d+ unchanged, reusable files with .+ data)|` + // Summary detail
		`(\s*-\s*\d+ changed or non-reusable files with .+ data)|` + // Summary detail
		`(\s*-\s*\d+\.\d+ (?:MiB|GiB) padding in \d+ partially reused chunks)|` + // Summary detail
		`(.+?: reused \d+\.\d+ (?:MiB|GiB) from previous snapshot for unchanged files \(\d+ chunks\))|` + // Archive reuse info
		`(.+?: had to backup \d+\.\d+ (?:MiB|GiB) of \d+\.\d+ (?:MiB|GiB) \(compressed \d+\.\d+ (?:MiB|GiB|KiB)\) in \d+\.\d+ s \(average \d+\.\d+ (?:MiB|KiB)\/s\))|` + // Archive backup stats
		`(.+?: backup was done incrementally, reused \d+\.\d+ (?:MiB|GiB) \(\d+\.\d+%\))|` + // Archive incremental info
		`(Uploaded \d+ chunks in \d+ seconds)|` + // General chunk upload (if it appears)
		`(restored \d+ bytes, \d+ files)|` + // Restore-specific: e.g., "restored 12345 bytes, 12 files"
		`(Duration: \d+\.\d+s)|` + // e.g., "Duration: 188.80s"
		`([eE]nd [tT]ime:)|` + // e.g., "End Time: Tue Nov 18 11:34:12 2025"
		`(TASK OK)`) // Definitive success message

// Filter proxmoxbackup log lines for a successful mover job
func LogLineFilterSuccess(line string) *string {
	if proxmoxbackupRegex.MatchString(line) {
		return &line
	}
	return nil
}
