import argparse
import time
from pathlib import Path
import luigi


class DummyTask(luigi.Task):
    """Write a success file after a delay."""

    output_path = luigi.Parameter()
    delay = luigi.IntParameter(default=5)

    def output(self):
        return luigi.LocalTarget(f"{self.output_path}/_SUCCESS")

    def run(self):
        print(f"Running task: {self.output_path}", flush=True)
        time.sleep(self.delay)

        Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)
        with self.output().open("w") as f:
            f.write("")


class Workflow(luigi.WrapperTask):
    """A dummy workflow with two tasks."""

    output_path = luigi.Parameter()
    num_tasks = luigi.IntParameter(default=10)
    # NOTE: do not use task_id as it is a reserved keyword
    sample_id = luigi.OptionalIntParameter()

    def requires(self):
        # either we run a single task or we run all the tasts
        print("Sample ID:", self.sample_id, flush=True)
        if self.sample_id is not None:
            task_ids = [self.sample_id]
        else:
            task_ids = list(range(self.num_tasks))

        print(f"Running tasks: {task_ids}", flush=True)
        tasks = []
        for task_id in task_ids:
            output_path = f"{self.output_path}/task_{task_id}"
            task = DummyTask(output_path=output_path, delay=10)
            tasks.append(task)
        yield tasks


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-path", type=str)
    parser.add_argument("--scheduler-host", type=str, default=None)
    parser.add_argument("--sample-id", type=int, default=None)
    parser.add_argument("--num-tasks", type=int, default=10)
    parser.add_argument("--num-workers", type=int, default=1)
    args = parser.parse_args()

    kwargs = {}
    if args.scheduler_host:
        kwargs["scheduler_host"] = args.scheduler_host
    else:
        kwargs["local_scheduler"] = True

    luigi.build(
        [
            Workflow(
                output_path=args.output_path,
                sample_id=args.sample_id,
                num_tasks=args.num_tasks,
            )
        ],
        workers=args.num_workers,
        **kwargs,
    )
