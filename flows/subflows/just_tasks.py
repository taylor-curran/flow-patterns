from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from child_flows import child_flow_a, child_flow_b, child_flow_c
from pydantic import BaseModel


@task()
def upstream_task_h():
    print("upstream task")
    return {"h": "upstream task"}


@task()
def upstream_task_i():
    print("upstream task")
    return {"i": "upstream task"}


@task()
def downstream_task_p(h):
    print(h)
    return {"p": "downstream task"}


@task()
def downstream_task_j(a):
    print("downstream task")
    return {"j": "downstream task"}


@task()
async def downstream_task_j(a, c, sim_failure_downstream_task_j):
    if sim_failure_downstream_task_j:
        raise Exception("This is a test exception")
    else:
        print("downstream task")
        return {"j": "downstream task"}


@task()
def downstream_task_k():
    print("downstream task")
    return {"k": "downstream task"}


@task
def task_f():
    print("task f")
    return {"f": "task f"}


@task
def task_m():
    print("task m")
    return {"m": "task m"}


@task
def task_n(m):
    print(m)
    print("task n")
    return {"n": "task n"}


@task
def task_o():
    print("task o")
    return {"o": "task o"}


# --
@task
def child_flow_a(i, sim_failure_child_flow_a):
    print(f"i: {i}")
    if sim_failure_child_flow_a:
        raise Exception("This is a test exception")
    else:
        return {"a": "child flow a"}


@task
def child_flow_b(i={"i": "upstream task"}, sim_failure_child_flow_b=False):
    print(f"i: {i}")
    if sim_failure_child_flow_b:
        raise Exception("This is a test exception")
    else:
        return {"b": "child flow b"}


@task
def child_flow_d():
    return {"d": "child flow d"}


@task
def child_flow_c():
    d = "child_flow_d"
    return {"c": d}


class SimulatedFailure(BaseModel):
    child_flow_a: bool = False
    child_flow_b: bool = False
    downstream_task_j: bool = False


default_simulated_failure = SimulatedFailure(
    child_flow_a=False, child_flow_b=False, downstream_task_j=False
)


# prefect deployment build just_tasks.py:just_tasks -n dep-just-tasks -t subflows -t just-tasks -t parent -a
@flow(task_runner=ConcurrentTaskRunner(), persist_result=True)
def just_tasks(sim_failure: SimulatedFailure = default_simulated_failure):
    h = upstream_task_h.submit()
    i = upstream_task_i.submit()
    p = downstream_task_p.submit(h)
    a = child_flow_a.submit(i, sim_failure.child_flow_a)
    b = child_flow_b.submit(
        sim_failure_child_flow_b=sim_failure.child_flow_b, wait_for=[i]
    )
    c = child_flow_c.submit()
    j = downstream_task_j.submit(a, c, sim_failure.downstream_task_j)
    k = downstream_task_k.submit(wait_for=[b])

    return {"j": j, "k": k}


# ---

if __name__ == "__main__":
    just_tasks(
        sim_failure=SimulatedFailure(
            child_flow_a=False, child_flow_b=False, downstream_task_j=False
        )
    )
