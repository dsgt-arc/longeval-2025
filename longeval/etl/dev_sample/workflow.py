"""Create a dev dataset using the first few examples of each partition"""

import json
from pathlib import Path
from shutil import copyfile
from multiprocessing import Pool

import luigi
import tqdm
import typer
from typing_extensions import Annotated


class DevSampleTask(luigi.Task):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    limit_json = luigi.IntParameter(default=10)

    def output(self):
        return luigi.LocalTarget(f"{self.output_path}/_SUCCESS")

    def _process_file(self, path):
        if path.is_dir():
            return
        # dont copy over trec files
        if "trec" in path.parent.name.lower() or "trec" in path.name.lower():
            return

        # skip urls.json
        if "urls.json" in path.name:
            return

        target = Path(self.output_path) / path.relative_to(self.input_path)
        target.parent.mkdir(parents=True, exist_ok=True)

        #  keep the first limit_json examples for json documents
        if "json" in path.parent.name.lower():
            with path.open() as f:
                data = json.load(f)
            data = data[: self.limit_json]
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("w") as f:
                json.dump(data, f)
        else:
            copyfile(path, target)

    def run(self):
        # copy files from input to output preserving the structure of the input
        paths = list(Path(self.input_path).glob("**/*"))

        with Pool() as p:
            list(tqdm.tqdm(p.imap(self._process_file, paths), total=len(paths)))

        # We're done with work now
        Path(self.output_path, "_SUCCESS").touch()


def main(
    input_path: Annotated[str, typer.Argument(help="Input root directory")],
    output_path: Annotated[str, typer.Argument(help="Output root directory")],
    scheduler_host: Annotated[str, typer.Argument(help="Scheduler host")] = None,
):
    """Create a dev dataset using the first few examples of each partition"""
    kwargs = {}
    if scheduler_host:
        kwargs["scheduler_host"] = scheduler_host
    else:
        kwargs["local_scheduler"] = True

    luigi.build(
        [DevSampleTask(input_path=input_path, output_path=output_path)],
        **kwargs,
    )
