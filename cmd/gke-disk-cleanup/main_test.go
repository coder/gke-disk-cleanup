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
		require.NoError(t, err)
	})

	t.Run("noop - label already present", func(t *testing.T) {
		t.Parallel()
		p := setup(t)
		p.dryRun = false

		p.di = &diskIteratorMock{
			NextFunc: func() (*computepb.Disk, error) {
				return &computepb.Disk{
					Name:                pointer.String("test-disk"),
					LastAttachTimestamp: pointer.String(time.Now().AddDate(0, 0, -60).Format(time.RFC3339)),
					Labels:              map[string]string{labelMarkedForDeletion: "true"},
				}, nil
			},
		}
		err := doMarkOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.cutoff, p.dryRun)
		require.EqualError(t, err, errAlreadyLabelled.Error())
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
				require.NotEmpty(t, setLabelsDiskRequest.GetRequestId())
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
				require.NotEmpty(t, setLabelsDiskRequest.GetRequestId())
				return nil, nil
			},
		}
		err := doMarkOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.cutoff, p.dryRun)
		require.NoError(t, err)
	})

	t.Run("success - never attached", func(t *testing.T) {
		t.Parallel()
		p := setup(t)
		p.dryRun = false

		p.di = &diskIteratorMock{
			NextFunc: func() (*computepb.Disk, error) {
				return &computepb.Disk{
					Name:                pointer.String("test-disk"),
					LastAttachTimestamp: pointer.String(""),
				}, nil
			},
		}
		p.dc = &disksClientMock{
			SetLabelsFunc: func(contextMoqParam context.Context, setLabelsDiskRequest *computepb.SetLabelsDiskRequest, callOptions ...gax.CallOption) (*computev1.Operation, error) {
				require.Equal(t, setLabelsDiskRequest.Project, p.projectID)
				require.NotEmpty(t, setLabelsDiskRequest.GetRequestId())
				return nil, nil
			},
		}
		err := doMarkOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.cutoff, p.dryRun)
		require.NoError(t, err)
	})
}

func Test_HandleMarkAction(t *testing.T) {
	testCases := []struct {
		name                string
		lastAttachTimestamp string
		labels              map[string]string
		cutoff              time.Duration
		expectedAction      action
		expectedError       string
	}{
		{
			name:                "should mark empty timestamp",
			lastAttachTimestamp: "",
			labels:              nil,
			cutoff:              24 * time.Hour,
			expectedAction:      actionMark,
			expectedError:       "",
		},
		{
			name:                "should skip invalid timestamp",
			lastAttachTimestamp: "foobarbaz",
			labels:              nil,
			cutoff:              24 * time.Hour,
			expectedAction:      actionSkip,
			expectedError:       `parse last attached timestamp: parsing time "foobarbaz" as "2006-01-02T15:04:05Z07:00": cannot parse "foobarbaz" as "2006"`,
		},
		{
			name:                "should skip already marked for deletion if last attached before cutoff",
			lastAttachTimestamp: time.Now().AddDate(-1, 0, 0).Format(time.RFC3339),
			labels:              map[string]string{labelMarkedForDeletion: "true"},
			cutoff:              24 * time.Hour,
			expectedAction:      actionSkip,
			expectedError:       errAlreadyLabelled.Error(),
		},
		{
			name:                "should mark for deletion if last attached before cutoff",
			lastAttachTimestamp: time.Now().AddDate(-1, 0, 0).Format(time.RFC3339),
			labels:              nil,
			cutoff:              24 * time.Hour,
			expectedAction:      actionMark,
			expectedError:       "",
		},
		{
			name:                "should unmark if already marked and last attached within cutoff",
			lastAttachTimestamp: time.Now().Format(time.RFC3339),
			labels:              map[string]string{labelMarkedForDeletion: "true"},
			cutoff:              24 * time.Hour,
			expectedAction:      actionUnmark,
			expectedError:       "",
		},
		{
			name:                "should skip if not already marked and last attached within cutoff",
			lastAttachTimestamp: time.Now().Format(time.RFC3339),
			labels:              nil,
			cutoff:              24 * time.Hour,
			expectedAction:      actionSkip,
			expectedError:       "",
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			actualAction, actualError := handleMarkAction(testCase.lastAttachTimestamp, testCase.labels, testCase.cutoff)
			require.Equal(t, testCase.expectedAction, actualAction)
			if testCase.expectedError == "" {
				require.NoError(t, actualError)
			} else {
				require.Equal(t, testCase.expectedError, actualError.Error())
			}
		})
	}
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
					Region: pointer.String("test-region"),
				}, nil
			},
		}

		p.dc = &disksClientMock{
			CreateSnapshotFunc: func(contextMoqParam context.Context, createSnapshotDiskRequest *computepb.CreateSnapshotDiskRequest, callOptions ...gax.CallOption) (*computev1.Operation, error) {
				require.Equal(t, createSnapshotDiskRequest.GetSnapshotResource().GetName(), "test-disk-snapshot")
				require.Contains(t, createSnapshotDiskRequest.GetSnapshotResource().GetStorageLocations(), "test-region")
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
				require.Equal(t, createSnapshotDiskRequest.SnapshotResource.Name, "test-disk-snapshot")
				require.Equal(t, createSnapshotDiskRequest.Disk, "test-disk")
				require.Equal(t, createSnapshotDiskRequest.Project, p.projectID)
				require.Equal(t, createSnapshotDiskRequest.Zone, p.zone)
				return &computev1.Operation{}, nil
			},
			DeleteFunc: func(contextMoqParam context.Context, deleteDiskRequest *computepb.DeleteDiskRequest, callOptions ...gax.CallOption) (*computev1.Operation, error) {
				require.Equal(t, deleteDiskRequest.Disk, "test-disk")
				require.Equal(t, deleteDiskRequest.Project, p.projectID)
				require.NotEmpty(t, deleteDiskRequest.RequestId)
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
				require.NotEmpty(t, deleteDiskRequest.RequestId)
				require.Equal(t, deleteDiskRequest.Zone, p.zone)

				return &computev1.Operation{}, nil
			},
		}
		err := doCleanupOne(p.ctx, p.dc, p.di, p.projectID, p.zone, p.doSnapshot, p.dryRun)
		require.NoError(t, err)
	})
}
