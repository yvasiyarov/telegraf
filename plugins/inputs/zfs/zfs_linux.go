// +build linux

package zfs

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
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
		//empty line, just skip
		return nil, nil
	}

	for i := 0; i < len(col); i++ {
		if col[i] == "-" {
			col[i] = "0"
		}
	}

	fields := map[string]interface{}{"name": col[0]}

	alloc, err := strconv.ParseInt(col[1], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing alloc: %s", err)
	}
	fields["iostat_alloc"] = alloc

	free, err := strconv.ParseInt(col[2], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing free: %s", err)
	}
	fields["iostat_free"] = free

	operationsRead, err := strconv.ParseInt(col[3], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing operations->read: %s", err)
	}
	fields["operations_read"] = operationsRead

	operationsWrite, err := strconv.ParseInt(col[4], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing operations->write: %s", err)
	}
	fields["operations_write"] = operationsWrite

	bandwidthRead, err := strconv.ParseInt(col[5], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing bandwidth->read: %s", err)
	}
	fields["bandwidth_read"] = bandwidthRead

	bandwidthWrite, err := strconv.ParseInt(col[6], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing bandwidth->write: %s", err)
	}
	fields["bandwidth_write"] = bandwidthWrite

	totalWaitRead, err := strconv.ParseInt(col[7], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing total_wait->read: %s", err)
	}
	fields["total_wait_read"] = totalWaitRead

	totalWaitWrite, err := strconv.ParseInt(col[8], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing total_wait->write: %s", err)
	}
	fields["total_wait_write"] = totalWaitWrite

	diskWaitRead, err := strconv.ParseInt(col[9], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing disk_wait->read: %s", err)
	}
	fields["disk_wait_read"] = diskWaitRead

	diskWaitWrite, err := strconv.ParseInt(col[10], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing disk_wait->write: %s", err)
	}
	fields["disk_wait_write"] = diskWaitWrite

	syncqWaitRead, err := strconv.ParseInt(col[11], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing syncq_wait->read: %s", err)
	}
	fields["syncq_wait_read"] = syncqWaitRead

	syncqWaitWrite, err := strconv.ParseInt(col[12], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing syncq_wait->write: %s", err)
	}
	fields["syncq_wait_write"] = syncqWaitWrite

	asyncqWaitRead, err := strconv.ParseInt(col[13], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing asyncq_wait->read: %s", err)
	}
	fields["asyncq_wait_read"] = asyncqWaitRead

	asyncqWaitWrite, err := strconv.ParseInt(col[14], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing asyncq_wait->write: %s", err)
	}
	fields["asyncq_wait_write"] = asyncqWaitWrite

	scrubWait, err := strconv.ParseInt(col[15], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing scrub_wait: %s", err)
	}
	fields["scrub_wait"] = scrubWait

	syncqReadOperationsPend, err := strconv.ParseInt(col[16], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing syncq_read_operations->pend: %s", err)
	}
	fields["syncq_read_operations_pend"] = syncqReadOperationsPend

	syncqReadOperationsActiv, err := strconv.ParseInt(col[17], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing syncq_read_operations->activ: %s", err)
	}
	fields["syncq_read_operations_activ"] = syncqReadOperationsActiv

	syncqWriteOperationsPend, err := strconv.ParseInt(col[18], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing syncq_write_operations->pend: %s", err)
	}
	fields["syncq_write_operations_pend"] = syncqWriteOperationsPend

	syncqWriteOperationsActiv, err := strconv.ParseInt(col[19], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing syncq_write_operations->activ: %s", err)
	}
	fields["syncq_write_operations_activ"] = syncqWriteOperationsActiv

	asyncqReadOperationsPend, err := strconv.ParseInt(col[20], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing asyncq_read_operations->pend: %s", err)
	}
	fields["asyncq_read_operations_pend"] = asyncqReadOperationsPend

	asyncqReadOperationsActiv, err := strconv.ParseInt(col[21], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing asyncq_read_operations->activ: %s", err)
	}
	fields["asyncq_read_operations_activ"] = asyncqReadOperationsActiv

	asyncqWriteOperationsPend, err := strconv.ParseInt(col[22], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing asyncq_write_operations->pend: %s", err)
	}
	fields["asyncq_write_operations_pend"] = asyncqWriteOperationsPend

	asyncqWriteOperationsActiv, err := strconv.ParseInt(col[23], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing asyncq_write_operations->activ: %s", err)
	}
	fields["asyncq_write_operations_activ"] = asyncqWriteOperationsActiv

	scrubqReadPend, err := strconv.ParseInt(col[24], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing scrubq_read->pend: %s", err)
	}
	fields["scrubq_read_pend"] = scrubqReadPend

	scrubqReadActiv, err := strconv.ParseInt(col[25], 10, 64)
	if err != nil {
		return fields, fmt.Errorf("Error parsing scrubq_read->activ: %s", err)
	}
	fields["scrubq_read_activ"] = scrubqReadActiv

	return fields, nil
}

func sumIostatsLines(exist map[string]interface{}, added map[string]interface{}) map[string]interface{} {
	exist["iostat_alloc"] = added["iostat_alloc"]
	exist["iostat_free"] = added["iostat_free"]

	fieldsToSum := []string{"operations_read", "operations_write", "bandwidth_read", "bandwidth_write", "total_wait_read", "total_wait_write",
		"disk_wait_read", "disk_wait_write", "syncq_wait_read", "syncq_wait_write", "asyncq_wait_read", "asyncq_wait_write", "scrub_wait",
		"syncq_read_operations_pend", "syncq_read_operations_activ", "syncq_write_operations_pend", "syncq_write_operations_activ",
		"asyncq_read_operations_pend", "asyncq_read_operations_activ", "asyncq_write_operations_pend", "asyncq_write_operations_activ",
		"scrubq_read_pend", "scrubq_read_activ",
	}
	for _, v := range fieldsToSum {
		exist[v] = exist[v].(int64) + added[v].(int64)
	}
	return exist
}

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
				//empty line
				if fields == nil {
					break
				}

				if name, ok := fields["name"]; !ok {
					return poolFields, fmt.Errorf("Can not parse pool name from string %s", line)
				} else {
					nameAsString := name.(string)
					if existsPoolStats, ok := poolFields[nameAsString]; ok {
						poolFields[nameAsString] = sumIostatsLines(existsPoolStats, fields)
						linesCount++
					} else {
						poolFields[nameAsString] = fields
					}
				}
			}
		default:
			// We need to pool from zpool iostat at least one line for every zfs pool
			// if for varyous reasons we pooled less than that we should continue pooling
			if linesCount < numberOfPools {
				time.Sleep(time.Millisecond * 100)
			} else {
				fmt.Printf("Stop reading zpool iostat lines, read %v\n", linesCount)
				moreLines = false
			}
		}

	}
	if linesCount > 0 {
		//		fieldsToAverage :=[]string{"syncq_read_operations_pend", "syncq_read_operations_activ", "syncq_write_operations_pend", "syncq_write_operations_activ",
		//			"asyncq_read_operations_pend", "asyncq_read_operations_activ", "asyncq_write_operations_pend", "asyncq_write_operations_activ",
		//			"scrubq_read_pend", "scrubq_read_activ",
		//		}
		//		for poolName, _ := range poolFields {
		//			for _, v := range fieldsToAverage {
		//				poolFields[poolName][v] = poolFields[poolName][v].(int) / linesCount
		//			}
		//		}
	}

	return poolFields, nil
}

func (z *Zfs) Gather(acc telegraf.Accumulator) error {

	//Gather pools metrics from kstats
	poolFields, err := z.getZpoolStats()
	if err != nil {
		return err
	}

	poolNames := []string{}
	pools := getPools(z.getKstatPath())
	//fmt.Printf("Pools:%v\n", pools)

	poolIostatsFields, err := z.getZpoolIostats(len(pools))
	if err != nil {
		return err
	}
	//fmt.Printf("PoolIostatFields: %#v\n", poolIostatsFields)

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

				//fmt.Printf("Fields: %v\n", fields)
				//fmt.Printf("Tags: %v\n", tags)
				acc.AddFields("zfs_pool", fields, tags)
			}
		}
	}

	return z.gatherZfsKstats(acc, strings.Join(poolNames, "::"))
}

// read stderr of zpool iostat command
// proxy it to
func zpoolIostatStderrReader(stderr io.ReadCloser) {
	if _, err := io.Copy(os.Stderr, stderr); err != nil {
		log.Printf("Copy zpool iostat stderr to main stderr error: %v", err)
	}
	/*
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			log.Print(scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			log.Printf("zpool iostat stderr reading error: %v", err)
		}
	*/
}

func zpoolIostat(ctx context.Context, out chan string, outErr chan error) {
	fmt.Printf("zpoolIostat started\n")

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

	fmt.Printf("Command started\n")
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Printf("Command stdout read %s\n", line)
		out <- line
	}
	fmt.Printf("Command stdout read\n")

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
	}
}

func (z *Zfs) Start(acc telegraf.Accumulator) error {
	if z.PoolIostatMetrics {
		z.zpoolIostatError = make(chan error, 1)
		z.zpoolIostatSource = make(chan string, ZpoolIostatBufferSize)

		ctx, cancel := context.WithCancel(context.Background())
		go z.zpoolIostat(ctx, z.zpoolIostatSource, z.zpoolIostatError)

		z.zpoolIostatCancelFunc = cancel

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
		fmt.Printf("Cancelling zpool iostat\n")
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
