package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/coreos/bbolt"
	"github.com/spf13/cobra"
)

var (
	// networkResultStoreBucketKey is the top-level bucket in channel.db
	// that stores the network result for each payment attempt ID. This
	// must match the key defined in lnd's
	// htlcswitch/payment_result.go.
	networkResultStoreBucketKey = []byte("network-result-store-bucket")

	// externalLifecycleMarkerBucket is a top-level bucket whose existence
	// indicates that the payment lifecycle is managed externally by a
	// switchrpc build. This must match the key defined in lnd's
	// htlcswitch/payment_result.go.
	externalLifecycleMarkerBucket = []byte("external-lifecycle-marker")
)

type clearRemoteMarkerCommand struct {
	ChannelDB string
	Force     bool

	cmd *cobra.Command
}

func newClearRemoteMarkerCommand() *cobra.Command {
	cc := &clearRemoteMarkerCommand{}
	cc.cmd = &cobra.Command{
		Use:   "clearremotemarker",
		Short: "Remove the external lifecycle marker from a channel DB",
		Long: `When lnd is built with the switchrpc tag and started, it writes
a marker to the channel database indicating that the payment lifecycle is
managed by an external entity (a "remote router"). This marker prevents a
normal (non-switchrpc) lnd build from starting, which protects against
accidentally cleaning up HTLC attempt state owned by the remote router.

To migrate back from switchrpc mode to normal lnd operation:

1. Ensure the remote router has drained all pending attempts.
2. Stop lnd.
3. Run this command to verify the attempt store is empty and remove the marker.
4. Restart lnd with a normal (non-switchrpc) build.

By default the command refuses to clear the marker if the network result store
still contains attempt entries. Use --force to skip this safety check (e.g. if
a remote router bug left orphaned entries that will never be cleaned up).

CAUTION: Using --force when real in-flight attempts exist will allow the next
local-mode lnd startup to clean state that the remote router still depends on,
which can lead to stuck or lost payments.`,
		Example: `chantools clearremotemarker \
	--channeldb ~/.lnd/data/graph/mainnet/channel.db

chantools clearremotemarker --force \
	--channeldb ~/.lnd/data/graph/mainnet/channel.db`,
		RunE: cc.Execute,
	}
	cc.cmd.Flags().StringVar(
		&cc.ChannelDB, "channeldb", "", "lnd channel.db file to "+
			"remove the external lifecycle marker from",
	)
	cc.cmd.Flags().BoolVar(
		&cc.Force, "force", false, "skip the empty-store safety "+
			"check and remove the marker even if attempt "+
			"entries still exist",
	)

	return cc.cmd
}

func (c *clearRemoteMarkerCommand) Execute(_ *cobra.Command,
	_ []string) error {

	if c.ChannelDB == "" {
		return errors.New("channel DB is required")
	}

	if _, err := os.Stat(c.ChannelDB); os.IsNotExist(err) {
		return fmt.Errorf("channel DB file not found: %s",
			c.ChannelDB)
	}

	db, err := bbolt.Open(c.ChannelDB, 0600, &bbolt.Options{
		FreelistType: bbolt.FreelistMapType,
		Timeout:      10 * time.Second,
	})
	if err != nil {
		if errors.Is(err, bbolt.ErrTimeout) {
			return fmt.Errorf("error opening %s: make sure "+
				"lnd is not running, database is locked "+
				"by another process", c.ChannelDB)
		}

		return fmt.Errorf("error opening channel DB: %w", err)
	}
	defer func() { _ = db.Close() }()

	return db.Update(func(tx *bbolt.Tx) error {
		// Check whether the marker exists at all.
		if tx.Bucket(externalLifecycleMarkerBucket) == nil {
			log.Infof("No external lifecycle marker found, " +
				"nothing to do")

			return nil
		}

		// Unless --force is set, verify the attempt store is empty.
		if !c.Force {
			bucket := tx.Bucket(networkResultStoreBucketKey)
			if bucket != nil {
				k, _ := bucket.Cursor().First()
				if k != nil {
					return errors.New("attempt entries " +
						"still exist in the network " +
						"result store; drain all " +
						"pending attempts before " +
						"clearing the marker, or " +
						"use --force to skip this " +
						"check")
				}
			}
		}

		// Delete the marker bucket.
		err := tx.DeleteBucket(externalLifecycleMarkerBucket)
		if err != nil && !errors.Is(err, bbolt.ErrBucketNotFound) {
			return fmt.Errorf("error deleting marker "+
				"bucket: %w", err)
		}

		log.Infof("External lifecycle marker removed successfully")

		return nil
	})
}
