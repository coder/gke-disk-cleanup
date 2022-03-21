package main

import (
	"context"
	"os"
	"time"

	computev1 "cloud.google.com/go/compute/apiv1"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"golang.org/x/xerrors"

	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/cloud/compute/v1"
)

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

func doMarkCmd(ctx context.Context, disksClient *computev1.DisksClient, projectID, zone string, cutoff time.Duration, dryRun bool) error {
	diskIter := disksClient.List(ctx, &compute.ListDisksRequest{
		Project: projectID,
		Zone:    zone,
	})
	for {
		disk, err := diskIter.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return xerrors.Errorf("iterating disks: %w", err)
		}
		lastAttachTimestampRFC3339 := disk.GetLastAttachTimestamp()
		if lastAttachTimestampRFC3339 == "" {
			log.Error().Str("diskName", disk.GetName()).Msg("disk last attached timestamp is nil")
			continue
		}
		lastAttachTime, err := time.Parse(time.RFC3339, lastAttachTimestampRFC3339)
		if err != nil {
			log.Error().Str("diskName", disk.GetName()).Str("lastAttachTimestamp", lastAttachTimestampRFC3339).Err(err).Msg("invalid last attached timestamp")
			continue
		}
		if lastAttachTime.Add(cutoff).Before(time.Now()) {
			log.Debug().Str("diskName", disk.GetName()).Int64("sizeGB", disk.GetSizeGb()).Time("lastAttachTime", lastAttachTime).Dur("cutoff", cutoff).Msg("ignoring disk attached within cutoff")
			continue
		}
		log.Info().Str("diskName", disk.GetName()).Int64("sizeGB", disk.GetSizeGb()).Time("lastAttachTime", lastAttachTime).Dur("cutoff", cutoff).Msg("marking disk last attached before cutoff")
		if !dryRun {
			// TODO: actually label the disk
			log.Debug().Msg("TODO: implement labelling")
		}
	}
}

func doCleanupCmd(ctx context.Context, disksClient *computev1.DisksClient, projectID string, dryRun bool) error {
	return xerrors.Errorf("TODO: not implemented yet")
}
