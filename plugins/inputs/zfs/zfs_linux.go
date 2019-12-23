// +build linux

package zfs

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

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
	
	fieldsToSum :=[]string{"operations_read", "operations_write", "bandwidth_read", "bandwidth_write", "total_wait_read", "total_wait_write", 
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

func (z *Zfs) getZpoolIostats() (map[string]map[string]interface{}, error) {

	fmt.Printf("Zpool Iostat\n")
	poolFields := map[string]map[string]interface{}{}

	if z.zpoolIostatSource == nil {
		return poolFields, nil
	}

	//TODO
	// - buffered or unbuffered
	// - how to read from channel without blocking
	// - group multiple lines by pool
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
			fmt.Printf("No IO stat \n")
			moreLines = false
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

	poolIostatsFields, err := z.getZpoolIostats()
	if err != nil {
		return err
	}
	//fmt.Printf("PoolIostatFields: %#v\n", poolIostatsFields)

	poolNames := []string{}
	pools := getPools(z.getKstatPath())
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
				//fmt.Printf("Pool: %#v\n", pool)
				//fmt.Printf("Fields: %#v\n", fields)
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

//zpool iostat -Hp -l -q -y 1
const tzpoolIostatContents = `
rpool	203525095936	116449967616	0	238	0	1612537	-	143137	-	69820	-	3456	-	82830	-	0	0	0	0	0	0	0	0	0	0
zd01	1041502023680	14901416579072	0	1491	0	164485664	-	77190832	-	5273927	-	4608	-	72408643	-	0	0	0	0	0	0	184	4	0	0
rpool	203525095936	116449967616	0	0	0	0	-	-	-	-	-	-	-	-	-	0	0	0	0	0	0	0	0	0	0
zd01	1041502023680	14901416579072	0	1510	0	165553394	-	74766449	-	5269671	-	3328	-	69849836	-	0	0	0	0	0	0	248	6	0	0
rpool	203525095936	116449967616	0	0	0	0	-	-	-	-	-	-	-	-	-	0	0	0	0	0	0	0	0	0	0
zd01	1041502023680	14901416579072	0	1476	0	167216354	-	61012954	-	5416006	-	3840	-	56158448	-	0	0	0	0	0	0	271	8	0	0
rpool	203525095936	116449967616	0	0	0	0	-	-	-	-	-	-	-	-	-	0	0	0	0	0	0	0	0	0	0
zd01	1041502023680	14901416579072	0	1471	0	163783365	-	80763358	-	5747755	-	4224	-	75404145	-	0	0	0	0	0	0	291	8	0	0
rpool	203525095936	116449967616	0	0	0	0	-	-	-	-	-	-	-	-	-	0	0	0	0	0	0	0	0	0	0
zd01	1041502023680	14901416579072	0	1184	0	133041318	-	76985756	-	6452099	-	3072	-	70534877	-	0	0	0	0	0	0	211	6	0	0
rpool	203523718144	116451345408	0	273	0	1699656	-	169797	-	68276	-	3072	-	103372	-	0	0	0	0	0	0	0	0	0	0
zd01	1041502023680	14901416579072	0	1275	0	141177045	-	76358227	-	6738397	-	4608	-	70201298	-	0	0	0	0	0	0	226	8	0	0
rpool	203523718144	116451345408	0	0	0	0	-	-	-	-	-	-	-	-	-	0	0	0	0	0	0	0	0	0	0
zd01	1041502023680	14901416579072	0	7966	0	1001163487	-	117373966	-	1161228	-	6144	-	116303952	-	0	0	0	0	0	0	4559	12	0	0
rpool	203523718144	116451345408	0	0	0	0	-	-	-	-	-	-	-	-	-	0	0	0	0	0	0	0	0	0	0
zd01	1041502023680	14901416579072	0	8009	0	1014721417	-	251578050	-	1199958	-	3072	-	250375641	-	0	0	0	0	0	0	281	8	0	0
rpool	203523718144	116451345408	0	0	0	0	-	-	-	-	-	-	-	-	-	0	0	0	0	0	0	0	0	0	0
zd01	1042354585600	14900564017152	0	1338	0	113423258	-	67588940	-	4967609	-	3225	-	64129430	-	0	0	0	0	0	0	0	1	0	0
rpool	203523718144	116451345408	0	0	0	0	-	-	-	-	-	-	-	-	-	0	0	0	0	0	0	0	0	0	0
zd01	1042353745920	14900564856832	0	1374	0	133165167	-	62578016	-	5457761	-	2730	-	57825773	-	0	0	0	0	0	0	229	8	0	0
`

func zpoolIostat(out chan string, outErr chan error) {
	lines := strings.Split(tzpoolIostatContents, "\n")
	for _, line := range lines {
		out <- line
	}
	return
}

func (z *Zfs) Start(acc telegraf.Accumulator) error {
	if z.PoolIostatMetrics {
		z.zpoolIostatError = make(chan error, 1)
		z.zpoolIostatSource = make(chan string, ZpoolIostatBufferSize)
		go zpoolIostat(z.zpoolIostatSource, z.zpoolIostatError)
	}
	return nil
}

//TODO: implement stop of zpool iostat
func (z *Zfs) Stop() {
}

func init() {
	inputs.Add("zfs", func() telegraf.Input {
		return &Zfs{
			zpool: zpool, 
			zpoolIostat: zpoolIostat, 
		}
	})
}
