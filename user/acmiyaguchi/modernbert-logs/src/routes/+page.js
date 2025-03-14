/*
Find the epilog from the end of the log file, and parse it into a json file.

---------------------------------------
Begin Slurm Epilog: Feb-14-2025 04:34:15
Job ID:        2769820
Array Job ID:  2769820_0
User ID:       amiyaguchi3
Account:       paceship-dsgt_clef2025
Job name:      longeval-embed-nomic-ai/modernbert-embed-test/2023_06/English/Documents
Resources:     cpu=6,gres/gpu:v100=1,mem=64G,node=1
Rsrc Used:     cput=04:54:06,vmem=0,walltime=00:49:01,mem=16773720K,energy_used=0
Partition:     gpu-v100
QOS:           embers
Nodes:         atl1-1-02-005-28-0
---------------------------------------
*/
function parseReportLog(log) {
    let lines = log.split('\n')
    // from begin slurm epilog to the second to the last line
    let epilog = lines.slice(lines.findIndex(line => line.startsWith('Begin Slurm Epilog:')), -2)
    let report = {}
    for (let line of epilog) {
        let [key, value] = line.split(':')
        // get the rest of the line by slicing by key length
        report[key.trim()] = line.slice(key.length + 1).trim()
    }
    return report
}

/* Get a useful dictionary out of the following line:
cput=04:54:06,vmem=0,walltime=00:49:01,mem=16773720K,energy_used=0
*/
function parseResources(line) {
    let resources = {}
    let pairs = line.split(',')
    for (let pair of pairs) {
        let [key, value] = pair.split('=')
        resources[key] = value
    }
    return resources
}

function png(jobid) {
    return `/logs/Report-${jobid}-nvidia-logs-utilization.png`;
}

function stringTimeToSeconds(s) {
    let [hours, minutes, seconds] = s.split(':').map(Number)
    return hours * 3600 + minutes * 60 + seconds
}


export async function load({ fetch }) {
    let manifest = await (await fetch('/manifest.json')).json()
    // let's extract information from each of the report logs
    let logs = await Promise.all(manifest.map(async jobid => {
        let logfile = `/logs/Report-${jobid}.out`
        let report = await (await fetch(logfile)).text()
        let parsed = parseReportLog(report)
        return {
            jobid,
            report: parsed,
            resource: parseResources(parsed["Resources"]),
            resource_used: parseResources(parsed["Rsrc Used"]),
            png: png(jobid)
        }
    }))

    // now sort by report["Array Job ID"]
    logs.sort((a, b) => {
        let a_id = a.report["Array Job ID"]
        let b_id = b.report["Array Job ID"]
        // extract the array part
        let a_part = a_id.split('_')[1]
        let b_part = b_id.split('_')[1]
        return a_part - b_part
    })

    let cputime_total = logs.map(({ resource_used }) => stringTimeToSeconds(resource_used.cput)).reduce((a, b) => a + b, 0)
    let walltime_total = logs.map(({ resource_used }) => stringTimeToSeconds(resource_used.walltime)).reduce((a, b) => a + b, 0)

    return {
        logs: logs,
        cputime_total,
        walltime_total,
    }
}
