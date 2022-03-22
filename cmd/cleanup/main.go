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
)

var (
	labelMarkedForDeletion      = "marked-for-deletion"
	errLastAttachedWithinCutoff = xerrors.Errorf("disk last attached within cutoff")
	errDryRun                   = xerrors.Errorf("dry run enabled")
)

// disksClient is an interface for the compute API methods we use here
type disksClient interface {
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
		lastAttachedCutoffDays int64
		projectID              string
		zone                   string
	)
	// pretty logging
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
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
	rootCmd.PersistentFlags().Int64Var(&lastAttachedCutoffDays, "last-attached-cutoff-days", 30, "how many days since the disk was last attached or detached")
	rootCmd.PersistentFlags().StringVar(&projectID, "project-id", "default", "google project id")
	rootCmd.PersistentFlags().StringVar(&zone, "zone", "us-east1-a", "google compute zone")

	markCmd := &cobra.Command{
		Use:   "mark",
		Short: "mark disks for later deletion",
		RunE: func(cmd *cobra.Command, args []string) error {
			cutoff := 24 * time.Hour * time.Duration(lastAttachedCutoffDays)
			return doMarkCmd(ctx, disksClient, projectID, zone, cutoff, dryRun)
		},
	}

	cleanupCmd := &cobra.Command{
		Use:   "cleanup",
		Short: "cleanup disks in gcloud",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return doCleanupCmd(ctx, disksClient, projectID, dryRun)
		},
	}

	disksClient, err = computev1.NewDisksRESTClient(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("init disks client")
	}

	rootCmd.AddCommand(markCmd, cleanupCmd)

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		log.Error().Err(err).Msg("failed to execute")
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
	if lastAttachTimestampRFC3339 == "" {
		return xerrors.Errorf("disk %s: last attached timestamp is empty", disk.GetName())
	}
	lastAttachTime, err := time.Parse(time.RFC3339, lastAttachTimestampRFC3339)
	if err != nil {
		return xerrors.Errorf("disk %s: last attached timestamp: %w", disk.GetName(), err)
	}
	if lastAttachTime.Add(cutoff).After(time.Now()) {
		return errLastAttachedWithinCutoff
	}
	diskLabels := disk.GetLabels()
	if diskLabels == nil {
		diskLabels = make(map[string]string)
	}
	diskLabels[labelMarkedForDeletion] = time.Now().Format(time.RFC3339)
	reqID := fmt.Sprintf("mark-for-cleanup-%s", disk.GetName())
	diskLabelsFingerprint := disk.GetLabelFingerprint()
	if dryRun {
		return errDryRun
	}
	log.Warn().Str("diskName", disk.GetName()).Int64("sizeGB", disk.GetSizeGb()).Time("lastAttachTime", lastAttachTime).Dur("cutoff", cutoff).Str("labels", fmt.Sprintf("%+v", diskLabels)).Msg("marking disk for deletion")
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

func doMarkCmd(ctx context.Context, disksClient disksClient, projectID, zone string, cutoff time.Duration, dryRun bool) error {
	if dryRun {
		log.Info().Msg("dry run mode is enabled -- no write operations will be performed")
	}
	diskIter := disksClient.List(ctx, &computepb.ListDisksRequest{
		Project: projectID,
		Zone:    zone,
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
			log.Error().Err(err).Msg("handling disk")
		}
	}
}

func doCleanupCmd(ctx context.Context, disksClient disksClient, projectID string, dryRun bool) error {
	return xerrors.Errorf("TODO: not implemented yet")
}
