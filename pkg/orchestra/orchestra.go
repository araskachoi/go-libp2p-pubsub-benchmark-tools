package orchestra

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/araskachoi/go-libp2p-pubsub-benchmark-tools/pkg/client"
	"github.com/araskachoi/go-libp2p-pubsub-benchmark-tools/pkg/logger"
	"github.com/araskachoi/go-libp2p-pubsub-benchmark-tools/pkg/orchestra/config"
	"github.com/araskachoi/go-libp2p-pubsub-benchmark-tools/pkg/subnet"
	"github.com/google/uuid"
)

// New returns a new Orchestra struct
func New(props Props) (*Orchestra, error) {
	return &Orchestra{
		props: props,
	}, nil
}

// Orchestrate begins the test
func (o *Orchestra) Orchestrate(stop chan os.Signal) error {
	var hostAddrs []string
	eChan := make(chan error)
	started := make(chan struct{})

	rand.Seed(time.Now().UnixNano())

	if !o.props.Conf.Orchestra.OmitSubnet {
		sConf := config.BuildSubnetConfig(o.props.Conf)

		// create the subnet
		snet, err := subnet.New(&subnet.Props{
			CTX:  o.props.CTX,
			Conf: sConf,
		})
		if err != nil {
			logger.Errorf("err creating subnet:\n%v", err)
			return err
		}

		// start the subnet
		if !o.props.Conf.Orchestra.OmitSubnet {
			go func(s *subnet.Subnet, e chan error) {
				if err = s.Start(started); err != nil {
					logger.Errorf("err starting subnet\n%v", err)
					e <- err
				}
			}(snet, eChan)
		}

		select {
		case <-started:
			logger.Info("subnet started")
		}

		hostAddrs = snet.RPCAddresses()
	}

	logger.Info("enter warmup")
	time.Sleep(time.Duration(o.props.Conf.Orchestra.TestWarmUpSeconds) * time.Second)

	if o.props.Conf.Orchestra.OmitSubnet {
		hostAddrs = o.props.Conf.Orchestra.HostRPCAddressesIfOmitSubnet
	}

	if len(hostAddrs) == 0 {
		logger.Errorf("no host addresses rpc:\n%v", hostAddrs)
		return ErrNoHostRPCAddresses
	}

	ticker := time.NewTicker(time.Duration(o.props.Conf.Orchestra.MessageNanoSecondInterval) * time.Nanosecond)
	defer ticker.Stop()

	testDuration := time.NewTicker(time.Duration(o.props.Conf.Orchestra.TestDurationSeconds) * time.Second)
	defer testDuration.Stop()

	cooldownTicker := time.NewTicker(time.Duration(o.props.Conf.Orchestra.TestCoolDownSeconds) * time.Second)
	cooldownTicker.Stop()

	logger.Infof("finished warmup; starting tests.\nmessage interval (ns): %d", o.props.Conf.Orchestra.MessageNanoSecondInterval)

	numMsgs := 0

	logger.Info("number of msgs to be sent via Orchestra:", int(float64(o.props.Conf.Orchestra.TestDurationSeconds) / (float64(o.props.Conf.Orchestra.MessageNanoSecondInterval) / 1E9)))

	// TODO: check if config results in an integer number of messages to be sent
	for {
		select {
		case <-stop:
			// note: I don't like '^C' showing up on the same line as the next logged line...
			fmt.Println("")
			logger.Info("Received stop signal from os. Shutting down...")
			return nil

		case <-ticker.C:

			numMsgs++
			if numMsgs >= int(float64(o.props.Conf.Orchestra.TestDurationSeconds) / (float64(o.props.Conf.Orchestra.MessageNanoSecondInterval) / 1E9)) {
				ticker.Stop()
				logger.Infof("Last message sent. Stopping Orchestra at msg %d...", numMsgs)	
			}

			go func(peers []string, c config.Config, e chan error) {
				id, err := uuid.NewRandom()
				if err != nil {
					logger.Errorf("err generating uuid:\n%v", err)
					eChan <- err
				}

				peerIdx := randBetween(0, len(peers)-1)
				peer := peers[peerIdx]

				logger.Infof("sending message to %s for publish", peer)
				if err := client.Publish([]byte(id.String()), c.Orchestra.MessageLocation, peer, c.Orchestra.MessageByteSize, c.Orchestra.ClientTimeoutSeconds); err != nil {
					logger.Fatalf("err sending messages\n%v", err)
					e <- err
				}
			}(hostAddrs, o.props.Conf, eChan)

		case <-testDuration.C:
			testDuration.Stop()
			cooldownTicker = time.NewTicker(time.Duration(o.props.Conf.Orchestra.TestCoolDownSeconds) * time.Second)
			logger.Info("finished tests. entering cooldown")

		case <-cooldownTicker.C:
			logger.Info("done with cooldown")
			return nil

		case err := <-eChan:
			logger.Errorf("received err on channel:\n%v", err)
			return err
		}
	}
}
