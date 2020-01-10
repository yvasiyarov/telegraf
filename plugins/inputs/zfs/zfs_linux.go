// +build linux

package zfs

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/inputs"
)

// zpool iostat buffer should be big enough to keep one line per zfs pool per second
// for the duration of interval (10 second by default)
const ZpoolIostatBufferSize = 1000

type poolInfo struct {
	name       string
	ioFilename string
}

func getPools(kstatPath string) []poolInfo {
	pools := make([]poolInfo, 0)
	poolsDirs, _ := filepath.Glob(kstatPath + "/*/io")

	for _, poolDir := range poolsDirs {
		poolDirSplit := strings.Split(poolDir, "/")
		pool := poolDirSplit[len(poolDirSplit)-2]
		pools = append(pools, poolInfo{name: pool, ioFilename: poolDir})
	}

	return pools
}

func getSinglePoolKstat(pool poolInfo) (map[string]interface{}, error) {
	fields := make(map[string]interface{})

	lines, err := internal.ReadLines(pool.ioFilename)
	if err != nil {
		return fields, err
	}

	if len(lines) != 3 {
		return fields, err
	}

	keys := strings.Fields(lines[1])
	values := strings.Fields(lines[2])

	keyCount := len(keys)

	if keyCount != len(values) {
		return fields, fmt.Errorf("Key and value count don't match Keys:%v Values:%v", keys, values)
	}

	for i := 0; i < keyCount; i++ {
		value, err := strconv.ParseInt(values[i], 10, 64)
		if err != nil {
			return fields, err
		}
		fields[keys[i]] = value
	}

	return fields, nil
}

func (z *Zfs) getKstatMetrics() []string {
	kstatMetrics := z.KstatMetrics
	if len(kstatMetrics) == 0 {
		// vdev_cache_stats is deprecated
		// xuio_stats are ignored because as of Sep-2016, no known
		// consumers of xuio exist on Linux
		kstatMetrics = []string{"abdstats", "arcstats", "dnodestats", "dbufcachestats",
			"dmu_tx", "fm", "vdev_mirror_stats", "zfetchstats", "zil"}
	}
	return kstatMetrics
}

func (z *Zfs) getKstatPath() string {
	kstatPath := z.KstatPath
	if len(kstatPath) == 0 {
		kstatPath = "/proc/spl/kstat/zfs"
	}
	return kstatPath
}

func (z *Zfs) gatherZfsKstats(acc telegraf.Accumulator, poolNames string) error {
	tags := map[string]string{"pools": poolNames}
	fields := make(map[string]interface{})
	kstatPath := z.getKstatPath()

	for _, metric := range z.getKstatMetrics() {
		lines, err := internal.ReadLines(kstatPath + "/" + metric)
		if err != nil {
			continue
		}
		for i, line := range lines {
			if i == 0 || i == 1 {
				continue
			}
			if len(line) < 1 {
				continue
			}
			rawData := strings.Split(line, " ")
			key := metric + "_" + rawData[0]
			if metric == "zil" || metric == "dmu_tx" || metric == "dnodestats" {
				key = rawData[0]
			}
			rawValue := rawData[len(rawData)-1]
			value, _ := strconv.ParseInt(rawValue, 10, 64)
			fields[key] = value
		}
	}
	acc.AddFields("zfs", fields, tags)
	return nil
}

func parseZpoolIostatLine(line string) (map[string]interface{}, error) {
	col := strings.Split(line, "\t")
	if len(col) == 1 {
		return nil, nil
	}

	for i := 0; i < len(col); i++ {
		if col[i] == "-" {
			col[i] = "0"
		}
	}

	fields := map[string]interface{}{"name": col[0]}

	fieldsToParse := map[string]int{
		"iostat_alloc":                  1,
		"iostat_free":                   2,
		"operations_read":               3,
		"operations_write":              4,
		"bandwidth_read":                5,
		"bandwidth_write":               6,
		"total_wait_read":               7,
		"total_wait_write":              8,
		"disk_wait_read":                9,
		"disk_wait_write":               10,
		"syncq_wait_read":               11,
		"syncq_wait_write":              12,
		"asyncq_wait_read":              13,
		"asyncq_wait_write":             14,
		"scrub_wait":                    15,
		"syncq_read_operations_pend":    16,
		"syncq_read_operations_activ":   17,
		"syncq_write_operations_pend":   18,
		"syncq_write_operations_activ":  19,
		"asyncq_read_operations_pend":   20,
		"asyncq_read_operations_activ":  21,
		"asyncq_write_operations_pend":  22,
		"asyncq_write_operations_activ": 23,
		"scrubq_read_pend":              24,
		"scrubq_read_activ":             25,
	}
	for k, position := range fieldsToParse {
		v, err := strconv.ParseInt(col[position], 10, 64)
		if err != nil {
			return fields, fmt.Errorf("Error parsing %s: \"%s\" can not be parsed into int. Error: %v", k, col[position], err)
		}
		fields[k] = v
	}

	return fields, nil
}

func sumIostatsLines(exist map[string]interface{}, added map[string]interface{}) map[string]interface{} {
	exist["iostat_alloc"] = added["iostat_alloc"]
	exist["iostat_free"] = added["iostat_free"]

	for k, v := range exist {
		if k != "iostat_alloc" && k != "iostat_free" && k != "name" {
			exist[k] = v.(int64) + added[k].(int64)
		}
	}
	return exist
}

//Parse and aggregate zpool iostat output
func (z *Zfs) getZpoolIostats(numberOfPools int) (map[string]map[string]interface{}, error) {

	poolFields := map[string]map[string]interface{}{}

	if z.zpoolIostatSource == nil {
		return poolFields, nil
	}

	moreLines := true
	linesCount := 0
	for moreLines {
		select {
		case line := <-z.zpoolIostatSource:
			if fields, err := parseZpoolIostatLine(line); err != nil {
				return poolFields, err
			} else {
				if fields == nil {
					break
				}

				if name, ok := fields["name"]; !ok {
					return poolFields, fmt.Errorf("Can not parse pool name from string %s", line)
				} else {
					nameAsString := name.(string)
					linesCount++
					if existsPoolStats, ok := poolFields[nameAsString]; ok {
						poolFields[nameAsString] = sumIostatsLines(existsPoolStats, fields)
					} else {
						poolFields[nameAsString] = fields
					}
				}
			}
		default:
			// We need to pull from "zpool iostat" at least one line for every zfs pool
			// if for whatever reasons we pulled less then we should continue pulling
			if linesCount < numberOfPools {
				time.Sleep(time.Millisecond * 100)
			} else {
				moreLines = false
			}
		}

		// we wanna linesCount be multiple of numberOfPools
		if (linesCount%numberOfPools == 0) && (len(z.zpoolIostatSource) < numberOfPools) {
			break
		}

	}

	linesPerPool := float64(linesCount / numberOfPools)

	for poolName, _ := range poolFields {
		for k, v := range poolFields[poolName] {
			if k != "iostat_free" && k != "iostat_alloc" && k != "name" {
				poolFields[poolName][k] = int64(math.Round(float64(v.(int64)) / linesPerPool))
			}
		}
	}
	return poolFields, nil
}

// proxy stderr of "zpool iostat" to the main process stderr
// just to make sure we do not hide any error message
func zpoolIostatStderrReader(stderr io.ReadCloser) {
	if _, err := io.Copy(os.Stderr, stderr); err != nil {
		log.Printf("Copy zpool iostat stderr to main stderr error: %v", err)
	}
}

// run zpool iostat -Hp -l -q -y 1 in background
// this command emit one line per zsf pool every second
func zpoolIostat(ctx context.Context, out chan string, outErr chan error) {
	command := "zpool"
	args := []string{"iostat", "-Hp", "-l", "-q", "-y", "1"}

	cmd := exec.CommandContext(ctx, command, args...)

	stderr, err := cmd.StderrPipe()
	if err != nil {
		fmt.Printf("Command StderrPipe() error: %v\n", err)
		outErr <- err
		return
	}
	go zpoolIostatStderrReader(stderr)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		outErr <- err
		return
	}

	err = cmd.Start()
	if err != nil {
		fmt.Printf("Command start error: %v\n", err)
		outErr <- err
		return
	}

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		out <- line
	}

	if err := scanner.Err(); err != nil {
		outErr <- err
		return
	}

	err = cmd.Wait()
	if execErr, ok := err.(*exec.Error); ok {
		outErr <- fmt.Errorf("%s was not found or not executable. Wrapped error: %s", execErr.Name, execErr.Err)
		return
	}
	if exiterr, ok := err.(*exec.ExitError); ok {
		if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
			log.Printf("zpool iostat exit, Exit Status: %d", status.ExitStatus())
		}
		return
	} else {
		outErr <- fmt.Errorf("Wait() returned unknown error: %#v", err)
		return
	}
}

func (z *Zfs) Gather(acc telegraf.Accumulator) error {

	//Gather pools metrics from kstats
	poolFields, err := z.getZpoolStats()
	if err != nil {
		return err
	}

	poolNames := []string{}
	pools := getPools(z.getKstatPath())

	poolIostatsFields, err := z.getZpoolIostats(len(pools))
	if err != nil {
		return err
	}

	for _, pool := range pools {
		poolNames = append(poolNames, pool.name)

		if z.PoolMetrics {

			//Merge zpool list with kstats
			fields, err := getSinglePoolKstat(pool)
			if err != nil {
				return err
			} else {
				for k, v := range poolFields[pool.name] {
					fields[k] = v
				}
				if _, ok := poolIostatsFields[pool.name]; ok {
					for k, v := range poolIostatsFields[pool.name] {
						fields[k] = v
					}
				}
				tags := map[string]string{
					"pool":   pool.name,
					"health": fields["health"].(string),
				}

				delete(fields, "name")
				delete(fields, "health")

				acc.AddFields("zfs_pool", fields, tags)
			}
		}
	}

	return z.gatherZfsKstats(acc, strings.Join(poolNames, "::"))
}

func (z *Zfs) Start(acc telegraf.Accumulator) error {

	if z.PoolIostatMetrics {
		z.zpoolIostatSource = make(chan string, ZpoolIostatBufferSize)

		// We make errors channel buffered to avoid deadlocks
		// in zpoolIostat() we report just one error and return, so make(chan error, 1) is enough
		// if we are going to return more than one error its better to increase the channel buffer size
		z.zpoolIostatError = make(chan error, 1)

		ctx, cancel := context.WithCancel(context.Background())

		// run zpool iostat collector in separate goroutine
		go z.zpoolIostat(ctx, z.zpoolIostatSource, z.zpoolIostatError)

		z.zpoolIostatCancelFunc = cancel

		// watchdog goroutine
		// in case of any failure collect the error and restart zpool iostat
		go func() {
			err := <-z.zpoolIostatError
			log.Printf("zpoolIostat return error:%v, restarting it", err)
			z.Stop()
			z.Start(acc)
		}()
	}
	return nil
}

func (z *Zfs) Stop() {
	if z.zpoolIostatCancelFunc != nil {
		z.zpoolIostatCancelFunc()
	}
}

func init() {
	inputs.Add("zfs", func() telegraf.Input {
		return &Zfs{
			zpool:       zpool,
			zpoolIostat: zpoolIostat,
		}
	})
}
