from typing import Optional, Any


def luigi_kwargs(scheduler_host: Optional[str] = None) -> dict[str, Any]:
    """Get the kwargs for luigi build."""
    kwargs = {}
    if scheduler_host:
        kwargs["scheduler_host"] = scheduler_host
    else:
        kwargs["local_scheduler"] = True
    return kwargs
