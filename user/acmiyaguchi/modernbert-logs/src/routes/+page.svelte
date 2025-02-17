<script>
	let { data } = $props();
	$effect(() => console.log(data));

	// Get information from the first element to display at the top
	let row = data.logs[0];
</script>

<h1>{row.report['Job name']}</h1>

<p>
	This is a generated report of an embedding job in the longeval-2025 project on the PACE cluster.
	The job had the following settings:
</p>

<table>
	<tbody>
		{#each ['User ID', 'Account', 'Partition', 'QOS'] as key}
			<tr>
				<td>{key}</td>
				<td>{row.report[key]}</td>
			</tr>
		{/each}
		{#each Object.keys(row.resource) as key}
			<tr>
				<td>{key}</td>
				<td>{row.resource[key]}</td>
			</tr>
		{/each}
	</tbody>
</table>

<p>
	Across {data.logs.length} array jobs, the average wall time to completion was {(
		data.walltime_total /
		data.logs.length /
		60
	).toFixed(1)} minutes. The total wall time was {(data.walltime_total / 60).toFixed(1)} minutes. The
	total cpu time was {(data.cputime_total / 60).toFixed(1)} minutes. There were {50 -
		data.logs.length} job(s) that failed.
</p>

{#each data.logs as log}
	<div>
		<h3>Array Job ID {log.report['Array Job ID']} at {log.report['Begin Slurm Epilog']}</h3>
		<!-- cput and walltime from rsc used -->
		<p>
			<b>CPU time:</b>
			{log.resource_used.cput}
			<b>Wall time:</b>
			{log.resource_used.walltime}
		</p>

		<img src={log.png} alt={log.jobid} />
	</div>
{/each}

<!-- Show this in a responsive grid -->
<style>
	div {
		display: inline-block;
		margin: 10px;
	}
	img {
		max-width: 500px;
	}
	table td {
		border: 1px solid black;
		padding: 5px;
	}
</style>
