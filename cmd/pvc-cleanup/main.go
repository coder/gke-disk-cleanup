package main

import (
	"context"
	"fmt"
	"os"
	"time"

	computev1 "cloud.google.com/go/compute/apiv1"
	"github.com/googleapis/gax-go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"golang.org/x/xerrors"
	"google.golang.org/api/iterator"
	computepb "google.golang.org/genproto/googleapis/cloud/compute/v1"
	"k8s.io/utils/pointer"
)

var (
	filterGoogGkeVolume         = "labels.goog-gke-volume:*"
	labelMarkedForDeletion      = "marked-for-deletion"
	errLastAttachedWithinCutoff = xerrors.Errorf("disk last attached within cutoff")
	errAlreadyLabelled          = xerrors.Errorf("disk already labelled")
	errDryRun                   = xerrors.Errorf("dry run enabled")
)

// disksClient is an interface for the compute API methods we use here
type disksClient interface {
	CreateSnapshot(context.Context, *computepb.CreateSnapshotDiskRequest, ...gax.CallOption) (*computev1.Operation, error)
	Delete(context.Context, *computepb.DeleteDiskRequest, ...gax.CallOption) (*computev1.Operation, error)
	List(context.Context, *computepb.ListDisksRequest, ...gax.CallOption) *computev1.DiskIterator
	SetLabels(context.Context, *computepb.SetLabelsDiskRequest, ...gax.CallOption) (*computev1.Operation, error)
}

type diskIterator interface {
	Next() (*computepb.Disk, error)
}

//go:generate moq -fmt goimports -out mock_disks_client.go . disksClient
//go:generate moq -fmt goimports -out mock_disk_iterator.go . diskIterator

func main() {
	var (
		disksClient            *computev1.DisksClient
		err                    error
		dryRun                 bool
		doSnapshot             bool
		lastAttachedCutoffDays int64
		projectID              string
		zone                   string
		filter                 string
		verbose                bool
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCmd := &cobra.Command{
		Use:   "pvc-cleanup",
		Short: "mark and clean up persistent disks in gcloud",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
	}
	rootCmd.PersistentFlags().BoolVar(&dryRun, "dry-run", true, "only log the actions that would be taken")
	rootCmd.PersistentFlags().StringVar(&projectID, "project-id", "default", "google project id")
	rootCmd.PersistentFlags().StringVar(&zone, "zone", "us-east1-a", "google compute zone")
	rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "verbose output")

	markCmd := &cobra.Command{
		Use:   "mark",
		Short: "mark disks for later deletion",
		RunE: func(cmd *cobra.Command, args []string) error {
			setupLogging(verbose)
			cutoff := 24 * time.Hour * time.Duration(lastAttachedCutoffDays)
			return doMarkCmd(ctx, disksClient, projectID, zone, filter, cutoff, dryRun)
		},
	}
	markCmd.PersistentFlags().StringVar(&filter, "filter", filterGoogGkeVolume, "filters for list disk request")
	markCmd.PersistentFlags().Int64Var(&lastAttachedCutoffDays, "cutoff", 30, "how many days since the disk was last attached or detached")

	cleanupCmd := &cobra.Command{
		Use:   "cleanup",
		Short: "cleanup disks in gcloud",
		RunE: func(cmd *cobra.Command, _ []string) error {
			setupLogging(verbose)
			return doCleanupCmd(ctx, disksClient, projectID, zone, doSnapshot, dryRun)
		},
	}

	cleanupCmd.PersistentFlags().BoolVar(&doSnapshot, "do-snapshot", true, "create a snapshot of the volume prior to deletion")

	disksClient, err = computev1.NewDisksRESTClient(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("init disks client")
	}

	rootCmd.AddCommand(markCmd, cleanupCmd)

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		log.Error().Err(err).Msg("failed to execute")
	}
}

func doMarkCmd(ctx context.Context, disksClient disksClient, projectID, zone, filter string, cutoff time.Duration, dryRun bool) error {
	if dryRun {
		log.Info().Msg("dry run mode is enabled -- no write operations will be performed")
	}
	diskIter := disksClient.List(ctx, &computepb.ListDisksRequest{
		Project: projectID,
		Zone:    zone,
		Filter:  &filter,
	})
	for {
		err := doMarkOne(ctx, disksClient, diskIter, projectID, zone, cutoff, dryRun)
		switch err {
		case iterator.Done:
			return nil
		case errLastAttachedWithinCutoff:
			log.Debug().Msg("ignoring disk last attached within cutoff")
		case errDryRun:
			log.Debug().Msg("not labelling disk as dry run enabled")
		default:
			log.Error().Err(err).Msg("unable to label disk for cleanup")
		}
	}
}

func doMarkOne(ctx context.Context, dc disksClient, di diskIterator, projectID, zone string, cutoff time.Duration, dryRun bool) error {
	disk, err := di.Next()
	if err == iterator.Done {
		return err
	}
	if err != nil {
		return xerrors.Errorf("iterating disks: %w", err)
	}
	log.Debug().Str("diskName", disk.GetName()).Int64("sizeGB", disk.GetSizeGb()).Str("lastAttachTime", disk.GetLastAttachTimestamp()).Str("labels", fmt.Sprintf("%v", disk.GetLabels())).Msg("got another disk")
	lastAttachTimestampRFC3339 := disk.GetLastAttachTimestamp()
	// if last attached timestamp is empty then the disk was never attached
	var lastAttachTime time.Time
	if lastAttachTimestampRFC3339 != "" {
		lastAttachTime, err = time.Parse(time.RFC3339, lastAttachTimestampRFC3339)
		if err != nil {
			return xerrors.Errorf("disk %s: last attached timestamp: %w", disk.GetName(), err)
		}
	}
	if lastAttachTime.Add(cutoff).After(time.Now()) {
		return errLastAttachedWithinCutoff
	}
	diskLabels := disk.GetLabels()
	if diskLabels == nil {
		diskLabels = make(map[string]string)
	}
	if dryRun {
		log.Warn().Str("diskName", disk.GetName()).Int64("sizeGB", disk.GetSizeGb()).Time("lastAttachTime", lastAttachTime).Str("labels", fmt.Sprintf("%+v", diskLabels)).Msg("would mark disk for deletion")
		return errDryRun
	}
	if _, found := diskLabels[labelMarkedForDeletion]; found {
		log.Debug().Str("diskName", disk.GetName()).Int64("sizeGB", disk.GetSizeGb()).Time("lastAttachTime", lastAttachTime).Str("labels", fmt.Sprintf("%+v", diskLabels)).Msg("disk already labelled")
		return errAlreadyLabelled
	}
	log.Warn().Str("diskName", disk.GetName()).Int64("sizeGB", disk.GetSizeGb()).Time("lastAttachTime", lastAttachTime).Str("labels", fmt.Sprintf("%+v", diskLabels)).Msg("marking disk for deletion")
	diskLabels[labelMarkedForDeletion] = "true"
	reqID := fmt.Sprintf("mark-for-cleanup-%s", disk.GetName())
	diskLabelsFingerprint := disk.GetLabelFingerprint()
	setLabelsReq := &computepb.SetLabelsDiskRequest{
		Project:   projectID,
		RequestId: &reqID,
		Resource:  fmt.Sprintf("%d", disk.GetId()),
		Zone:      zone,
		ZoneSetLabelsRequestResource: &computepb.ZoneSetLabelsRequest{
			Labels:           diskLabels,
			LabelFingerprint: &diskLabelsFingerprint,
		},
	}
	if _, err := dc.SetLabels(ctx, setLabelsReq); err != nil {
		return xerrors.Errorf("error updating disk labels: %w", err)
	}
	return nil
}

func doCleanupCmd(ctx context.Context, disksClient disksClient, projectID, zone string, doSnapshot bool, dryRun bool) error {
	if dryRun {
		log.Info().Msg("dry run mode is enabled -- no delete operations will be performed")
	}
	diskIter := disksClient.List(ctx, &computepb.ListDisksRequest{
		Project: projectID,
		Zone:    zone,
		Filter:  pointer.String(fmt.Sprintf("labels.%s:true", labelMarkedForDeletion)),
	})
	for {
		err := doCleanupOne(ctx, disksClient, diskIter, projectID, zone, doSnapshot, dryRun)
		switch err {
		case iterator.Done:
			return nil
		case errDryRun:
			log.Debug().Msg("not labelling disk as dry run enabled")
		default:
			log.Error().Err(err).Msg("unable to delete disk")
		}
	}
}

func doCleanupOne(ctx context.Context, dc disksClient, di diskIterator, projectID, zone string, doSnapshot, dryRun bool) error {
	disk, err := di.Next()
	if err == iterator.Done {
		return err
	}

	if err != nil {
		return xerrors.Errorf("iterating disks: %w", err)
	}

	diskLabels := disk.GetLabels()

	if diskLabels == nil {
		return xerrors.Errorf("skipping disk %s: missing required label", disk.GetName())
	}

	if labelValue, found := diskLabels[labelMarkedForDeletion]; !found {
		return xerrors.Errorf("skipping disk %s: missing required label", disk.GetName())
	} else if labelValue != "true" {
		return xerrors.Errorf("skipping disk %s: expected label value true but got %q", disk.GetName(), labelValue)
	}

	if doSnapshot {
		if dryRun {
			log.Info().Str("diskName", disk.GetName()).Int64("sizeGB", disk.GetSizeGb()).Str("lastAttachTime", disk.GetLastAttachTimestamp()).Str("labels", fmt.Sprintf("%+v", diskLabels)).Msg("dry run - would snapshot disk prior to deletion")
		} else {
			log.Info().Str("diskName", disk.GetName()).Int64("sizeGB", disk.GetSizeGb()).Str("lastAttachTime", disk.GetLastAttachTimestamp()).Str("labels", fmt.Sprintf("%+v", diskLabels)).Msg("snapshotting disk prior to deletion")
			req := &computepb.CreateSnapshotDiskRequest{
				Disk:             disk.GetName(),
				Project:          projectID,
				RequestId:        pointer.String(fmt.Sprintf("snapshot-disk-%s", disk.GetName())),
				SnapshotResource: nil, // TODO: does this need to be non-nil?
				Zone:             zone,
			}
			op, err := dc.CreateSnapshot(ctx, req)
			if err != nil {
				return xerrors.Errorf("disk %s: failed to create snapshot before deletion: %w", disk.GetName(), err)
			}

			// wait for snapshot to complete
			err = op.Wait(ctx)
			if err != nil {
				return xerrors.Errorf("disk %s: failed to wait for snapshot to be ready: %w", disk.GetName(), err)
			}
		}
	}

	if dryRun {
		log.Warn().Str("diskName", disk.GetName()).Int64("sizeGB", disk.GetSizeGb()).Str("lastAttachTime", disk.GetLastAttachTimestamp()).Str("labels", fmt.Sprintf("%+v", diskLabels)).Msg("dry run -- would delete disk")
		return errDryRun
	}

	log.Warn().Str("diskName", disk.GetName()).Int64("sizeGB", disk.GetSizeGb()).Str("lastAttachTime", disk.GetLastAttachTimestamp()).Str("labels", fmt.Sprintf("%+v", diskLabels)).Msg("deleting disk")
	req := &computepb.DeleteDiskRequest{
		Disk:      disk.GetName(),
		Project:   projectID,
		RequestId: pointer.String(fmt.Sprintf("delete-disk-%s", disk.GetName())),
		Zone:      zone,
	}
	_, err = dc.Delete(ctx, req)
	if err != nil {
		return xerrors.Errorf("failed to delete disk %s: %w", disk.GetName(), err)
	}

	return nil
}

func setupLogging(verbose bool) {
	// pretty logging
	if verbose {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).Level(zerolog.DebugLevel)
		return
	}
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).Level(zerolog.InfoLevel)
}
