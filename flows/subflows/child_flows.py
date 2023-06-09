from prefect import flow, task
from prefect.deployments import run_deployment


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


# prefect deployment build child_flows.py:child_flow_a -n dep-child-a -t sub-flows -t child -a
@flow(persist_result=True)
def child_flow_a(i, sim_failure_child_flow_a):
    print(f"i: {i}")
    task_f()
    if sim_failure_child_flow_a:
        raise Exception("This is a test exception")
    else:
        return {"a": "child flow a"}


# prefect deployment build child_flows.py:child_flow_b -n dep-child-b -t sub-flows -t child -a
@flow(persist_result=True)
def child_flow_b(i={"i": "upstream task"}, sim_failure_child_flow_b=False):
    print(f"i: {i}")
    if sim_failure_child_flow_b:
        raise Exception("This is a test exception")
    else:
        return {"b": "child flow b"}


# prefect deployment build child_flows.py:child_flow_d -n dep-child-d -t sub-flows -t child -a
@flow(persist_result=True)
def child_flow_d():
    o = task_o()
    return {"d": "child flow d"}


# -- Nested Child Flow --
# prefect deployment build child_flows.py:child_flow_c -n dep-child-c -t sub-flows -t child -a
@flow(persist_result=True)
def child_flow_c():
    d = run_deployment("child-flow-d/dep-child-d")
    m = task_m()
    n = task_n(m)
    return {"c": d.state.result()}
