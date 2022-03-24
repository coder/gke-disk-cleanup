package main

import (
	"context"
	"testing"
	"time"

	computev1 "cloud.google.com/go/compute/apiv1"
	"github.com/googleapis/gax-go"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"
	"google.golang.org/api/iterator"
	computepb "google.golang.org/genproto/googleapis/cloud/compute/v1"
	"k8s.io/utils/pointer"
)

func Test_MarkCmd(t *testing.T) {
	t.Parallel()
	type params struct {
		ctx       context.Context
		dc        disksClient
		di        diskIterator
		projectID string
		zone      string
		cutoff    time.Duration
		dryRun    bool
	}

	setup := func(t *testing.T) *params {
		return &params{
			ctx:       context.Background(),
			dc:        &disksClientMock{},
			di:        &diskIteratorMock{},
			projectID: "testing",
			zone:      "testzone",
			cutoff:    30 * 24 * time.Hour,
			dryRun:    true,
		}
	}

	t.Run("done", func(t *testing.T) {
		t.Parallel()
		p := setup(t)

		p.di = &diskIteratorMock{
			NextFunc: func() (*computepb.Disk, error) {
				return nil, iterator.Done
			},
		}

		err := doMarkOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.cutoff, p.dryRun)
		require.EqualError(t, err, iterator.Done.Error())
	})

	t.Run("iteration error", func(t *testing.T) {
		t.Parallel()
		p := setup(t)

		p.di = &diskIteratorMock{
			NextFunc: func() (*computepb.Disk, error) {
				return nil, xerrors.Errorf("test error")
			},
		}

		err := doMarkOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.cutoff, p.dryRun)
		require.EqualError(t, err, "iterating disks: test error")
	})

	t.Run("empty timestamp", func(t *testing.T) {
		t.Parallel()
		p := setup(t)

		p.di = &diskIteratorMock{
			NextFunc: func() (*computepb.Disk, error) {
				return &computepb.Disk{
					Name:                pointer.String("test-disk"),
					LastAttachTimestamp: pointer.String(""),
				}, nil
			},
		}
		err := doMarkOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.cutoff, p.dryRun)
		require.ErrorContains(t, err, "disk test-disk: last attached timestamp is empty")
	})

	t.Run("invalid timestamp", func(t *testing.T) {
		t.Parallel()
		p := setup(t)

		p.di = &diskIteratorMock{
			NextFunc: func() (*computepb.Disk, error) {
				return &computepb.Disk{
					Name:                pointer.String("test-disk"),
					LastAttachTimestamp: pointer.String("invalid"),
				}, nil
			},
		}
		err := doMarkOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.cutoff, p.dryRun)
		require.ErrorContains(t, err, "cannot parse \"invalid\"")
	})

	t.Run("noop", func(t *testing.T) {
		t.Parallel()
		p := setup(t)

		p.di = &diskIteratorMock{
			NextFunc: func() (*computepb.Disk, error) {
				return &computepb.Disk{
					Name:                pointer.String("test-disk"),
					LastAttachTimestamp: pointer.String(time.Now().Format(time.RFC3339)),
				}, nil
			},
		}
		err := doMarkOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.cutoff, p.dryRun)
		require.EqualError(t, err, errLastAttachedWithinCutoff.Error())
	})

	t.Run("noop - label already present", func(t *testing.T) {
		t.Parallel()
		p := setup(t)
		p.dryRun = false

		p.di = &diskIteratorMock{
			NextFunc: func() (*computepb.Disk, error) {
				return &computepb.Disk{
					Name:                pointer.String("test-disk"),
					LastAttachTimestamp: pointer.String(time.Now().Add(-60 * 24 * time.Hour).Format(time.RFC3339)),
					Labels:              map[string]string{labelMarkedForDeletion: "sometime"},
				}, nil
			},
		}
		err := doMarkOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.cutoff, p.dryRun)
		require.ErrorContains(t, err, errAlreadyLabelled.Error())
	})

	t.Run("dry run", func(t *testing.T) {
		t.Parallel()
		p := setup(t)

		p.di = &diskIteratorMock{
			NextFunc: func() (*computepb.Disk, error) {
				return &computepb.Disk{
					Name:                pointer.String("test-disk"),
					LastAttachTimestamp: pointer.String(time.Now().Add(-60 * 24 * time.Hour).Format(time.RFC3339)),
				}, nil
			},
		}
		err := doMarkOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.cutoff, p.dryRun)
		require.EqualError(t, err, errDryRun.Error())
	})

	t.Run("error updating label", func(t *testing.T) {
		t.Parallel()
		p := setup(t)
		p.dryRun = false

		p.di = &diskIteratorMock{
			NextFunc: func() (*computepb.Disk, error) {
				return &computepb.Disk{
					Name:                pointer.String("test-disk"),
					LastAttachTimestamp: pointer.String(time.Now().Add(-60 * 24 * time.Hour).Format(time.RFC3339)),
				}, nil
			},
		}
		p.dc = &disksClientMock{
			SetLabelsFunc: func(contextMoqParam context.Context, setLabelsDiskRequest *computepb.SetLabelsDiskRequest, callOptions ...gax.CallOption) (*computev1.Operation, error) {
				require.Equal(t, setLabelsDiskRequest.Project, p.projectID)
				require.Equal(t, setLabelsDiskRequest.GetRequestId(), "mark-for-cleanup-test-disk")
				return nil, xerrors.Errorf("test error")
			},
		}
		err := doMarkOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.cutoff, p.dryRun)
		require.EqualError(t, err, "error updating disk labels: test error")
	})

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		p := setup(t)
		p.dryRun = false

		p.di = &diskIteratorMock{
			NextFunc: func() (*computepb.Disk, error) {
				return &computepb.Disk{
					Name:                pointer.String("test-disk"),
					LastAttachTimestamp: pointer.String(time.Now().Add(-60 * 24 * time.Hour).Format(time.RFC3339)),
				}, nil
			},
		}
		p.dc = &disksClientMock{
			SetLabelsFunc: func(contextMoqParam context.Context, setLabelsDiskRequest *computepb.SetLabelsDiskRequest, callOptions ...gax.CallOption) (*computev1.Operation, error) {
				require.Equal(t, setLabelsDiskRequest.Project, p.projectID)
				require.Equal(t, setLabelsDiskRequest.GetRequestId(), "mark-for-cleanup-test-disk")
				return nil, nil
			},
		}
		err := doMarkOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.cutoff, p.dryRun)
		require.NoError(t, err)
	})
}

func Test_CleanupCmd(t *testing.T) {
	t.Parallel()
	type params struct {
		ctx        context.Context
		dc         disksClient
		di         diskIterator
		projectID  string
		zone       string
		doSnapshot bool
		dryRun     bool
	}

	setup := func(t *testing.T) *params {
		return &params{
			ctx:        context.Background(),
			dc:         &disksClientMock{},
			di:         &diskIteratorMock{},
			projectID:  "testing",
			zone:       "testzone",
			doSnapshot: true,
			dryRun:     true,
		}
	}

	t.Run("done", func(t *testing.T) {
		t.Parallel()
		p := setup(t)

		p.di = &diskIteratorMock{
			NextFunc: func() (*computepb.Disk, error) {
				return nil, iterator.Done
			},
		}

		err := doCleanupOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.doSnapshot, p.dryRun)
		require.EqualError(t, err, iterator.Done.Error())
	})

	t.Run("iteration error", func(t *testing.T) {
		t.Parallel()
		p := setup(t)

		p.di = &diskIteratorMock{
			NextFunc: func() (*computepb.Disk, error) {
				return nil, xerrors.Errorf("test error")
			},
		}

		err := doCleanupOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.doSnapshot, p.dryRun)
		require.EqualError(t, err, "iterating disks: test error")
	})

	t.Run("disk labels nil", func(t *testing.T) {
		t.Parallel()
		p := setup(t)

		p.di = &diskIteratorMock{
			NextFunc: func() (*computepb.Disk, error) {
				return &computepb.Disk{
					Name:   pointer.String("test-disk"),
					Labels: nil,
				}, nil
			},
		}
		err := doCleanupOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.doSnapshot, p.dryRun)
		require.ErrorContains(t, err, "disk test-disk: missing required label")
	})

	t.Run("disk label missing", func(t *testing.T) {
		t.Parallel()
		p := setup(t)

		p.di = &diskIteratorMock{
			NextFunc: func() (*computepb.Disk, error) {
				return &computepb.Disk{
					Name:   pointer.String("test-disk"),
					Labels: map[string]string{},
				}, nil
			},
		}
		err := doCleanupOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.doSnapshot, p.dryRun)
		require.ErrorContains(t, err, "disk test-disk: missing required label")
	})

	t.Run("disk label wrong value", func(t *testing.T) {
		t.Parallel()
		p := setup(t)

		p.di = &diskIteratorMock{
			NextFunc: func() (*computepb.Disk, error) {
				return &computepb.Disk{
					Name:   pointer.String("test-disk"),
					Labels: map[string]string{labelMarkedForDeletion: "false"},
				}, nil
			},
		}
		err := doCleanupOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.doSnapshot, p.dryRun)
		require.ErrorContains(t, err, "disk test-disk: expected label value true but got \"false\"")
	})

	t.Run("create snapshot error", func(t *testing.T) {
		t.Parallel()
		p := setup(t)
		p.dryRun = false

		p.di = &diskIteratorMock{
			NextFunc: func() (*computepb.Disk, error) {
				return &computepb.Disk{
					Name:   pointer.String("test-disk"),
					Labels: map[string]string{labelMarkedForDeletion: "true"},
				}, nil
			},
		}

		p.dc = &disksClientMock{
			CreateSnapshotFunc: func(contextMoqParam context.Context, createSnapshotDiskRequest *computepb.CreateSnapshotDiskRequest, callOptions ...gax.CallOption) (*computev1.Operation, error) {
				require.Equal(t, createSnapshotDiskRequest.Disk, "test-disk")
				require.Equal(t, createSnapshotDiskRequest.Project, p.projectID)
				require.Equal(t, createSnapshotDiskRequest.Zone, p.zone)
				return nil, xerrors.Errorf("google says no")
			},
		}

		err := doCleanupOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.doSnapshot, p.dryRun)
		require.ErrorContains(t, err, "disk test-disk: failed to create snapshot before deletion: google says no")
	})

	t.Run("dry run", func(t *testing.T) {
		t.Parallel()
		p := setup(t)

		p.di = &diskIteratorMock{
			NextFunc: func() (*computepb.Disk, error) {
				return &computepb.Disk{
					Name:   pointer.String("test-disk"),
					Labels: map[string]string{labelMarkedForDeletion: "true"},
				}, nil
			},
		}
		err := doCleanupOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.doSnapshot, p.dryRun)
		require.EqualError(t, err, errDryRun.Error())
	})

	t.Run("delete error", func(t *testing.T) {
		t.Parallel()
		p := setup(t)
		p.dryRun = false
		p.doSnapshot = false // to side-step op.Wait(ctx) panic in unit test

		p.di = &diskIteratorMock{
			NextFunc: func() (*computepb.Disk, error) {
				return &computepb.Disk{
					Name:   pointer.String("test-disk"),
					Labels: map[string]string{labelMarkedForDeletion: "true"},
				}, nil
			},
		}

		p.dc = &disksClientMock{
			CreateSnapshotFunc: func(contextMoqParam context.Context, createSnapshotDiskRequest *computepb.CreateSnapshotDiskRequest, callOptions ...gax.CallOption) (*computev1.Operation, error) {
				require.Equal(t, createSnapshotDiskRequest.Disk, "test-disk")
				require.Equal(t, createSnapshotDiskRequest.Project, p.projectID)
				require.Equal(t, createSnapshotDiskRequest.Zone, p.zone)
				return &computev1.Operation{}, nil
			},
			DeleteFunc: func(contextMoqParam context.Context, deleteDiskRequest *computepb.DeleteDiskRequest, callOptions ...gax.CallOption) (*computev1.Operation, error) {
				require.Equal(t, deleteDiskRequest.Disk, "test-disk")
				require.Equal(t, deleteDiskRequest.Project, p.projectID)
				require.Equal(t, deleteDiskRequest.RequestId, pointer.String("delete-disk-test-disk"))
				require.Equal(t, deleteDiskRequest.Zone, p.zone)

				return nil, xerrors.Errorf("google says no")
			},
		}

		err := doCleanupOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.doSnapshot, p.dryRun)
		require.ErrorContains(t, err, "failed to delete disk test-disk: google says no")
	})

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		p := setup(t)
		p.dryRun = false
		p.doSnapshot = false // to side-step op.Wait(ctx) panic in unit test

		p.di = &diskIteratorMock{
			NextFunc: func() (*computepb.Disk, error) {
				return &computepb.Disk{
					Name:   pointer.String("test-disk"),
					Labels: map[string]string{labelMarkedForDeletion: "true"},
				}, nil
			},
		}

		p.dc = &disksClientMock{
			DeleteFunc: func(contextMoqParam context.Context, deleteDiskRequest *computepb.DeleteDiskRequest, callOptions ...gax.CallOption) (*computev1.Operation, error) {
				require.Equal(t, deleteDiskRequest.Disk, "test-disk")
				require.Equal(t, deleteDiskRequest.Project, p.projectID)
				require.Equal(t, deleteDiskRequest.RequestId, pointer.String("delete-disk-test-disk"))
				require.Equal(t, deleteDiskRequest.Zone, p.zone)

				return &computev1.Operation{}, nil
			},
		}
		err := doCleanupOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.doSnapshot, p.dryRun)
		require.NoError(t, err)
	})
}