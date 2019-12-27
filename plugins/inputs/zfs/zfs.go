package zfs

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

type Sysctl func(metric string) ([]string, error)
type Zpool func() ([]string, error)
type ZpoolIostat func(ctx context.Context, out chan string, outErr chan error)

type Zfs struct {
	KstatPath             string
	KstatMetrics          []string
	PoolMetrics           bool
	PoolIostatMetrics     bool
	sysctl                Sysctl
	zpool                 Zpool
	zpoolIostat           ZpoolIostat
	zpoolIostatSource     chan string
	zpoolIostatError      chan error
	zpoolIostatCancelFunc context.CancelFunc
}

var sampleConfig = `
  ## ZFS kstat path. Ignored on FreeBSD
  ## If not specified, then default is:
  # kstatPath = "/proc/spl/kstat/zfs"

  ## By default, telegraf gather all zfs stats
  ## If not specified, then default is:
  # kstatMetrics = ["arcstats", "zfetchstats", "vdev_cache_stats"]
  ## For Linux, the default is:
  # kstatMetrics = ["abdstats", "arcstats", "dnodestats", "dbufcachestats",
  #   "dmu_tx", "fm", "vdev_mirror_stats", "zfetchstats", "zil"]
  ## By default, don't gather zpool stats
  ## Before turning it on, please, check that zpool is available from $PATH
  # poolMetrics = false
  ## By default, don't gather zpool stats
  # poolIostatMetrics = false
`

func (z *Zfs) SampleConfig() string {
	return sampleConfig
}

func (z *Zfs) getZpoolStats() (map[string]map[string]interface{}, error) {

	poolFields := map[string]map[string]interface{}{}

	lines, err := z.zpool()
	if err != nil {
		return poolFields, err
	}

	for _, line := range lines {
		col := strings.Split(line, "\t")
		if len(col) != 10 {
			continue
		}

		health := col[1]
		name := col[0]

		fields := map[string]interface{}{
			"name":   name,
			"health": health,
		}

		if health == "UNAVAIL" {

			fields["size"] = int64(0)

		} else {

			size, err := strconv.ParseInt(col[2], 10, 64)
			if err != nil {
				return poolFields, fmt.Errorf("Error parsing size: %s", err)
			}
			fields["size"] = size

			alloc, err := strconv.ParseInt(col[3], 10, 64)
			if err != nil {
				return poolFields, fmt.Errorf("Error parsing allocation: %s", err)
			}
			fields["allocated"] = alloc

			free, err := strconv.ParseInt(col[4], 10, 64)
			if err != nil {
				return poolFields, fmt.Errorf("Error parsing free: %s", err)
			}
			fields["free"] = free

			frag, err := strconv.ParseInt(strings.TrimSuffix(col[5], "%"), 10, 0)
			if err != nil { // This might be - for RO devs
				frag = 0
			}
			fields["fragmentation"] = frag

			capval, err := strconv.ParseInt(col[6], 10, 0)
			if err != nil {
				return poolFields, fmt.Errorf("Error parsing capacity: %s", err)
			}
			fields["capacity"] = capval

			dedup, err := strconv.ParseFloat(strings.TrimSuffix(col[7], "x"), 32)
			if err != nil {
				return poolFields, fmt.Errorf("Error parsing dedupratio: %s", err)
			}
			fields["dedupratio"] = dedup

			freeing, err := strconv.ParseInt(col[8], 10, 64)
			if err != nil {
				return poolFields, fmt.Errorf("Error parsing freeing: %s", err)
			}
			fields["freeing"] = freeing

			leaked, err := strconv.ParseInt(col[9], 10, 64)
			if err != nil {
				return poolFields, fmt.Errorf("Error parsing leaked: %s", err)
			}
			fields["leaked"] = leaked
		}
		poolFields[name] = fields
	}
	return poolFields, nil
}

func (z *Zfs) Description() string {
	return "Read metrics of ZFS from arcstats, zfetchstats, vdev_cache_stats, and pools"
}

func run(command string, args ...string) ([]string, error) {
	cmd := exec.Command(command, args...)
	var outbuf, errbuf bytes.Buffer
	cmd.Stdout = &outbuf
	cmd.Stderr = &errbuf
	err := cmd.Run()

	stdout := strings.TrimSpace(outbuf.String())
	stderr := strings.TrimSpace(errbuf.String())

	if execErr, ok := err.(*exec.Error); ok {
		return nil, fmt.Errorf("%s was not found or not executable. Wrapped error: %s", execErr.Name, execErr.Err)
	}
	if _, ok := err.(*exec.ExitError); ok {
		return nil, fmt.Errorf("%s error: %s", command, stderr)
	}
	return strings.Split(stdout, "\n"), nil
}

func zpool() ([]string, error) {
	return run("zpool", []string{"list", "-Hp", "-o", "name,health,size,alloc,free,fragmentation,capacity,dedupratio,freeing,leaked"}...)
}
