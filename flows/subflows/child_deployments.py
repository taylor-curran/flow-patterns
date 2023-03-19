from prefect import flow
from prefect.deployments import run_deployment

# TODO: Set persistence of the child flows to True

# prefect deployment build child_deployments.py:child_flow_a -n dep-child-a -t sub-flows -t child -a
@flow(persist_result=True)
def child_flow_a(i, sim_failure_child_flow_a):
    print(f'i: {i}')
    if sim_failure_child_flow_a:
        raise Exception("This is a test exception")
    else:
        return {'a': 'child flow a'}

# prefect deployment build child_deployments.py:child_flow_b -n dep-child-b -t sub-flows -t child -a
@flow(persist_result=True)
def child_flow_b(i={'i': 'upstream task'}, sim_failure_child_flow_b=False):
    print(f'i: {i}')
    if sim_failure_child_flow_b:
        raise Exception("This is a test exception")
    else:
        return {'b': 'child flow b'}

# prefect deployment build child_deployments.py:child_flow_d -n dep-child-d -t sub-flows -t child -a
@flow(persist_result=True)
def child_flow_d():
    return {'d': 'child flow d'}

# prefect deployment build child_deployments.py:child_flow_c -n dep-child-c -t sub-flows -t child -a
@flow(persist_result=True)
def child_flow_c():
    d = run_deployment(
        "child-flow-d/dep-child-d"
        )
    return {'d': d.state.result()}
