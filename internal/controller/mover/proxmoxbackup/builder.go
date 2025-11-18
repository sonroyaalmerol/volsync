//go:build !disable_proxmoxbackup

/*
Copyright 2021 The VolSync authors.

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
	"flag"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/spf13/viper"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/backube/volsync/internal/controller/mover"
	"github.com/backube/volsync/internal/controller/utils"
	"github.com/backube/volsync/internal/controller/volumehandler"
)

const (
	pbsMoverName = "proxmox-backup" // Renamed from rcloneMoverName
	// defaultPBSContainerImage is the default container image for the proxmox-backup data mover
	defaultPBSContainerImage = "ghcr.io/sonroyaalmerol/volsync:pbs-client" // Renamed from defaultRcloneContainerImage
	// Command line flag will be checked first
	// If command line flag not set, the RELATED_IMAGE_ env var will be used
	pbsContainerImageFlag   = "proxmox-backup-container-image"      // Renamed from rcloneContainerImageFlag
	pbsContainerImageEnvVar = "RELATED_IMAGE_PROXMOX_BACKUP_CLIENT" // Renamed from rcloneContainerImageEnvVar
)

type Builder struct {
	viper *viper.Viper  // For unit tests to be able to override - global viper will be used by default in Register()
	flags *flag.FlagSet // For unit tests to be able to override - global flags will be used by default in Register()
}

var _ mover.Builder = &Builder{}

func Register() error {
	// Use global viper & command line flags
	b, err := newBuilder(viper.GetViper(), flag.CommandLine)
	if err != nil {
		return err
	}

	mover.Register(b)
	return nil
}

func newBuilder(viper *viper.Viper, flags *flag.FlagSet) (*Builder, error) {
	b := &Builder{
		viper: viper,
		flags: flags,
	}

	// Set default proxmox-backup container image - will be used if both command line flag and env var are not set
	b.viper.SetDefault(pbsContainerImageFlag, defaultPBSContainerImage)

	// Setup command line flag for the proxmox-backup container image
	b.flags.String(pbsContainerImageFlag, defaultPBSContainerImage,
		"The container image for the proxmox-backup data mover")
	// Viper will check for command line flag first, then fallback to the env var
	err := b.viper.BindEnv(pbsContainerImageFlag, pbsContainerImageEnvVar)

	return b, err
}

func (rb *Builder) Name() string { return pbsMoverName } // Renamed

func (rb *Builder) VersionInfo() string {
	return fmt.Sprintf("Proxmox Backup Client container: %s", rb.getPBSContainerImage()) // Renamed
}

// getPBSContainerImage is the container image name of the proxmox-backup data mover
func (rb *Builder) getPBSContainerImage() string { // Renamed
	return rb.viper.GetString(pbsContainerImageFlag) // Renamed
}

func (rb *Builder) FromSource(client client.Client, logger logr.Logger,
	eventRecorder events.EventRecorder,
	source *volsyncv1alpha1.ReplicationSource, privileged bool,
) (mover.Mover, error) {
	// Only build if the CR belongs to us
	// Assuming `ProxmoxBackup` field exists in ReplicationSourceSpec
	if source.Spec.ProxmoxBackup == nil {
		return nil, nil
	}

	if source.Status.LatestMoverStatus == nil {
		source.Status.LatestMoverStatus = &volsyncv1alpha1.MoverStatus{}
	}

	vh, err := volumehandler.NewVolumeHandler(
		volumehandler.WithClient(client),
		volumehandler.WithRecorder(eventRecorder),
		volumehandler.WithOwner(source),
		volumehandler.FromSource(&source.Spec.ProxmoxBackup.ReplicationSourceVolumeOptions),
	)
	if err != nil {
		return nil, err
	}

	isSource := true

	saHandler := utils.NewSAHandler(client, source, isSource, privileged,
		source.Spec.ProxmoxBackup.MoverServiceAccount)

	return &Mover{
		client:                  client,
		logger:                  logger.WithValues("method", "ProxmoxBackup"), // Renamed
		eventRecorder:           eventRecorder,
		owner:                   source,
		vh:                      vh,
		saHandler:               saHandler,
		containerImage:          rb.getPBSContainerImage(), // Renamed
		proxmoxBackupRepository: source.Spec.ProxmoxBackup.ProxmoxBackupRepository,
		proxmoxBackupIDSuffix:   source.Spec.ProxmoxBackup.ProxmoxBackupIDSuffix,
		proxmoxBackupSecret:     source.Spec.ProxmoxBackup.ProxmoxBackupSecret,
		isSource:                isSource,
		paused:                  source.Spec.Paused,
		mainPVCName:             &source.Spec.SourcePVC,
		customCASpec:            source.Spec.ProxmoxBackup.CustomCA,
		privileged:              privileged,
		latestMoverStatus:       source.Status.LatestMoverStatus,
		moverConfig:             source.Spec.ProxmoxBackup.MoverConfig,
		moverVolumes:            source.Spec.ProxmoxBackup.MoverVolumes,
		proxmoxBackupNamespace:  source.Spec.ProxmoxBackup.ProxmoxBackupNamespace,
	}, nil
}

func (rb *Builder) FromDestination(client client.Client, logger logr.Logger,
	eventRecorder events.EventRecorder,
	destination *volsyncv1alpha1.ReplicationDestination, privileged bool) (mover.Mover, error) {
	// Only build if the CR belongs to us
	// Assuming `ProxmoxBackup` field exists in ReplicationDestinationSpec
	if destination.Spec.ProxmoxBackup == nil {
		return nil, nil
	}

	if destination.Status.LatestMoverStatus == nil {
		destination.Status.LatestMoverStatus = &volsyncv1alpha1.MoverStatus{}
	}

	vh, err := volumehandler.NewVolumeHandler(
		volumehandler.WithClient(client),
		volumehandler.WithRecorder(eventRecorder),
		volumehandler.WithOwner(destination),
		volumehandler.FromDestination(&destination.Spec.ProxmoxBackup.ReplicationDestinationVolumeOptions),
	)
	if err != nil {
		return nil, err
	}

	isSource := false

	saHandler := utils.NewSAHandler(client, destination, isSource, privileged,
		destination.Spec.ProxmoxBackup.MoverServiceAccount)

	return &Mover{
		client:                  client,
		logger:                  logger.WithValues("method", "ProxmoxBackup"),
		eventRecorder:           eventRecorder,
		owner:                   destination,
		vh:                      vh,
		saHandler:               saHandler,
		containerImage:          rb.getPBSContainerImage(),
		proxmoxBackupRepository: destination.Spec.ProxmoxBackup.ProxmoxBackupRepository,
		proxmoxBackupIDSuffix:   destination.Spec.ProxmoxBackup.ProxmoxBackupIDSuffix,
		proxmoxBackupSecret:     destination.Spec.ProxmoxBackup.ProxmoxBackupSecret,
		isSource:                isSource,
		paused:                  destination.Spec.Paused,
		mainPVCName:             destination.Spec.ProxmoxBackup.DestinationPVC,
		cleanupTempPVC:          destination.Spec.ProxmoxBackup.CleanupTempPVC,
		customCASpec:            destination.Spec.ProxmoxBackup.CustomCA,
		privileged:              privileged,
		latestMoverStatus:       destination.Status.LatestMoverStatus,
		moverConfig:             destination.Spec.ProxmoxBackup.MoverConfig,
		moverVolumes:            destination.Spec.ProxmoxBackup.MoverVolumes,
		proxmoxBackupNamespace:  destination.Spec.ProxmoxBackup.ProxmoxBackupNamespace,
	}, nil
}
