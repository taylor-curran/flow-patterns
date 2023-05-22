from typing import List
from prefect import Flow, Task, flow
from prefect.states import Completed
import functools


## -------------------------------------------------------------------------------------
# Avoid submission of tasks passing skip_tasks around


def task(__fn=None, **kwargs):
    if __fn:
        return CustomTask(fn=__fn, **kwargs)
    else:
        return functools.partial(task, **kwargs)


class CustomTask(Task):
    def submit(self, *args, **kwargs):
        skip_tasks = kwargs.pop("skip_tasks", [])
        if self.name in skip_tasks:
            return None
        return super().submit(*args, **kwargs)


@task
def foo():
    print("foo")


@task
def bar():
    print("bar")


@flow
def my_flow_from_direct(skip_tasks: List[str] = []):
    foo.submit(skip_tasks=skip_tasks)
    bar.submit(skip_tasks=skip_tasks)


my_flow_from_direct(skip_tasks=["foo"])
