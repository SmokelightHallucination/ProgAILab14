package coord

import (
	"context"
	"hash/fnv"
	"log"
	"sort"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"airquality/collector/internal/model"
)

const membersPrefix = "/aq/collectors/"

// EtcdCoordinator registers this instance under an etcd lease and keeps a live
// view of the membership set. Shard ownership uses rendezvous (highest-random-
// weight) hashing: for a station, the member with the largest hash(member|station)
// owns it. This gives an even, stable split that only minimally reshuffles when
// a member joins or leaves.
type EtcdCoordinator struct {
	id      string
	cli     *clientv3.Client
	leaseID clientv3.LeaseID

	mu      sync.RWMutex
	members []string

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewEtcd connects to etcd, registers the instance with a TTL lease + keepalive,
// and starts watching the membership prefix.
func NewEtcd(ctx context.Context, endpoints []string, id string, ttl int64) (*EtcdCoordinator, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	lease, err := cli.Grant(ctx, ttl)
	if err != nil {
		_ = cli.Close()
		return nil, err
	}
	if _, err = cli.Put(ctx, membersPrefix+id, time.Now().UTC().Format(time.RFC3339), clientv3.WithLease(lease.ID)); err != nil {
		_ = cli.Close()
		return nil, err
	}
	keepAlive, err := cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		_ = cli.Close()
		return nil, err
	}

	cctx, cancel := context.WithCancel(ctx)
	c := &EtcdCoordinator{id: id, cli: cli, leaseID: lease.ID, cancel: cancel}

	// Drain keepalive responses so the channel never blocks the lease.
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for range keepAlive {
		}
	}()

	if err := c.refresh(cctx); err != nil {
		cancel()
		_ = cli.Close()
		return nil, err
	}
	c.wg.Add(1)
	go c.watch(cctx)

	log.Printf("[coord] registered %q with etcd, members=%v", id, c.snapshot())
	return c, nil
}

// refresh reloads the current membership set from etcd.
func (c *EtcdCoordinator) refresh(ctx context.Context) error {
	resp, err := c.cli.Get(ctx, membersPrefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return err
	}
	members := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		members = append(members, string(kv.Key[len(membersPrefix):]))
	}
	sort.Strings(members)
	c.mu.Lock()
	c.members = members
	c.mu.Unlock()
	return nil
}

// watch reacts to membership changes and recomputes the shard.
func (c *EtcdCoordinator) watch(ctx context.Context) {
	defer c.wg.Done()
	wch := c.cli.Watch(ctx, membersPrefix, clientv3.WithPrefix())
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-wch:
			if !ok {
				return
			}
			if err := c.refresh(ctx); err != nil {
				log.Printf("[coord] refresh after watch failed: %v", err)
				continue
			}
			log.Printf("[coord] membership changed, members=%v", c.snapshot())
		}
	}
}

func (c *EtcdCoordinator) snapshot() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, len(c.members))
	copy(out, c.members)
	return out
}

// Owns implements rendezvous hashing: the member with the highest combined
// hash for (member, station) wins ownership of that station.
func (c *EtcdCoordinator) Owns(stationID string) bool {
	members := c.snapshot()
	if len(members) == 0 {
		return true
	}
	var bestMember string
	var bestScore uint64
	for _, m := range members {
		s := score(m, stationID)
		if s > bestScore {
			bestScore, bestMember = s, m
		}
	}
	return bestMember == c.id
}

func (c *EtcdCoordinator) Assigned(stations []model.Station) []model.Station {
	out := make([]model.Station, 0, len(stations))
	for _, s := range stations {
		if c.Owns(s.ID) {
			out = append(out, s)
		}
	}
	return out
}

func (c *EtcdCoordinator) Members() int { return len(c.snapshot()) }

func (c *EtcdCoordinator) Close() error {
	c.cancel()
	// Best-effort lease revoke so the slot frees immediately instead of after TTL.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = c.cli.Revoke(ctx, c.leaseID)
	err := c.cli.Close()
	c.wg.Wait()
	return err
}

func score(member, station string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(member))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(station))
	return h.Sum64()
}
