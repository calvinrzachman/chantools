package main

import (
	"path/filepath"
	"testing"

	"github.com/coreos/bbolt"
	"github.com/stretchr/testify/require"
)

// createTestDB creates a fresh bbolt database in a temporary directory.
// When addMarker is true, both the external lifecycle marker bucket and
// the network result store bucket are created, matching the state lnd
// produces on startup with the switchrpc tag. When addEntries is also
// true, a dummy attempt entry is written to the store bucket.
func createTestDB(t *testing.T, addMarker, addEntries bool) string {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "channel.db")

	db, err := bbolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)

	if addMarker {
		err = db.Update(func(tx *bbolt.Tx) error {
			_, err := tx.CreateBucket(
				externalLifecycleMarkerBucket,
			)
			if err != nil {
				return err
			}

			// Always create the store bucket alongside the
			// marker, since lnd creates both on startup.
			bucket, err := tx.CreateBucket(
				networkResultStoreBucketKey,
			)
			if err != nil {
				return err
			}

			if addEntries {
				// Write a dummy 8-byte attempt ID with
				// an arbitrary value to simulate a
				// pending entry.
				attemptID := []byte{
					0, 0, 0, 0, 0, 0, 0, 1,
				}

				return bucket.Put(
					attemptID, []byte("test-entry"),
				)
			}

			return nil
		})
		require.NoError(t, err)
	}

	require.NoError(t, db.Close())

	return dbPath
}

// markerExists opens the database in read-only mode and reports whether
// the external lifecycle marker bucket is present.
func markerExists(t *testing.T, dbPath string) bool {
	t.Helper()

	db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var exists bool
	err = db.View(func(tx *bbolt.Tx) error {
		exists = tx.Bucket(externalLifecycleMarkerBucket) != nil
		return nil
	})
	require.NoError(t, err)

	return exists
}

func TestClearRemoteMarker(t *testing.T) {
	tests := []struct {
		name       string
		addMarker  bool
		addEntries bool
		force      bool
		wantErr    string
		markerGone bool
	}{
		{
			// When no marker exists the command is a silent
			// no-op.
			name:       "no marker present",
			markerGone: true,
		},
		{
			// Normal migration path: marker present, store
			// bucket exists but has been drained by the
			// remote router.
			name:       "marker with empty store",
			addMarker:  true,
			markerGone: true,
		},
		{
			// Safety check: refuse to clear while attempt
			// entries still exist.
			name:       "marker with non-empty store is rejected",
			addMarker:  true,
			addEntries: true,
			wantErr:    "attempt entries still exist",
			markerGone: false,
		},
		{
			// Force flag bypasses the safety check for the
			// stuck-attempt recovery scenario.
			name:       "marker with non-empty store and force",
			addMarker:  true,
			addEntries: true,
			force:      true,
			markerGone: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			_ = h // Initializes the test logger.

			dbPath := createTestDB(
				t, tc.addMarker, tc.addEntries,
			)

			cmd := &clearRemoteMarkerCommand{
				ChannelDB: dbPath,
				Force:     tc.force,
			}

			err := cmd.Execute(nil, nil)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}

			if tc.markerGone {
				require.False(
					t, markerExists(t, dbPath),
					"marker bucket should not exist",
				)
			} else {
				require.True(
					t, markerExists(t, dbPath),
					"marker bucket should still exist",
				)
			}
		})
	}
}
