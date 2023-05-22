from typing import List
from prefect import Flow, Task, flow
from prefect.states import Completed
import functools
from contextvars import ContextVar

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





## -------------------------------------------------------------------------------------
# Avoid submission of tasks by using context variable

from contextvars import ContextVar


_SKIP_TASKS = ContextVar("skip_tasks", default=[])


def task(__fn=None, **kwargs):
    if __fn:
        return CustomTask(fn=__fn, **kwargs)
    else:
        return functools.partial(task, **kwargs)


class CustomTask(Task):
    # To take action before a task is submitted / a task run is created then override
    # a function in the task class itself
    def submit(self, *args, **kwargs):
        skip_tasks = kwargs.pop("skip_tasks", [])
        if self.name in skip_tasks:
            return None
        return super().submit(*args, **kwargs)


## here?

class CustomFlow(Flow):
    def __call__(self, *args, **kwargs):
        skip_tasks = kwargs.pop("skip_tasks", [])

        token = _SKIP_TASKS.set(skip_tasks)
        try:
            retval = super().__call__(*args, **kwargs)
        finally:
            _SKIP_TASKS.reset(token)

        return retval


def flow(__fn=None, **kwargs):
    if __fn:
        return CustomFlow(fn=__fn, **kwargs)
    else:
        return functools.partial(flow, **kwargs)



## -------------------------------------------------------------------------------------
# Create a task run then mark as SKIPPED using context variable




###
## -------------------------------------------------------------------------------------
# Create a task run then mark as SKIPPED and mark downstreams as skipped


class SKIPPED:
    pass


def task(__fn=None, **kwargs):
    if __fn:
        return CustomTask(fn=my_task_wrapper(__fn), **kwargs)
    else:
        return functools.partial(task, **kwargs)


def my_task_wrapper(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        skip_tasks = kwargs.pop("skip_tasks", _SKIP_TASKS.get())
        if fn.__name__ in skip_tasks or SKIPPED in args or SKIPPED in kwargs.values():
            return Completed(name="Skipped", data=SKIPPED)
        return fn(*args, **kwargs)

    return wrapper

@task
def not_skipped():
    print("not_skipped")

@task
def foo_wrapped():
    print("foo")


@task
def bar_wrapped(x):
    print("bar")


@flow
def my_flow_return_skipped_with_downstream():
    not_skipped()
    upstream = foo_wrapped()
    bar_wrapped(upstream)


my_flow_return_skipped_with_downstream(skip_tasks=["foo_wrapped"])