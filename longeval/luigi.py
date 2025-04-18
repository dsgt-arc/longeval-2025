from typing import Optional, Any
from luigi.contrib.external_program import ExternalProgramTask
from textwrap import dedent
import tempfile


def luigi_kwargs(scheduler_host: Optional[str] = None) -> dict[str, Any]:
    """Get the kwargs for luigi build."""
    kwargs = {}
    if scheduler_host:
        kwargs["scheduler_host"] = scheduler_host
    else:
        kwargs["local_scheduler"] = True
    return kwargs


class BashScriptTask(ExternalProgramTask):
    def script_text(self) -> str:
        """The contents of to write to a bash script for running."""
        return dedent(
            """
            #!/bin/bash
            echo 'hello world'
            exit 1
            """
        )

    def program_args(self):
        """Execute the script."""
        script_text = self.script_text().strip()
        script_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
        script_file.write(script_text)
        script_file.close()
        print(f"Script file: {script_file.name}")
        print(script_text)
        return ["/bin/bash", script_file.name]
