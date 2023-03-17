from prefect import flow, task
from prefect.deployments import run_deployment
from pydantic import BaseModel

import asyncio



@task()
async def upstream_task_h():
    print('upstream task')
    return {'h': 'upstream task'}

@task()
async def upstream_task_i():
    print('upstream task')
    return {'i': 'upstream task'}

@task()
async def downstream_task_j(a, c, sim_failure_downstream_task_j):
    if sim_failure_downstream_task_j:
        raise Exception("This is a test exception")
    else:
        print('downstream task')
        return {'j': 'downstream task'}

@task()
async def downstream_task_k(d):
    print('downstream task')
    return {'k': 'downstream task'}

# ---

class SimulatedFailure(BaseModel):
    child_flow_a: bool = False
    child_flow_b: bool = False
    downstream_task_j: bool = False

default_simulated_failure = SimulatedFailure(
    child_flow_a=False,
    child_flow_b=False,
    downstream_task_j=False
    )

# prefect deployment build asyncio_syntax.py:asyncio_syntax -n dep_asyncio -t sub-flows -t asyncio_syntax -a
@flow(persist_result=True)
async def asyncio_syntax(sim_failure: SimulatedFailure = default_simulated_failure):
    h = await upstream_task_h()
    i = await upstream_task_i()

    flow_run_a = await run_deployment(
        "child-flow-a/child-deployment-a",
        parameters={
        "i": i, 
        "sim_failure_child_flow_a": sim_failure.child_flow_a} 
        )
    a = flow_run_a.state.result()
    
    flow_run_b = await run_deployment(
        name="child-flow-b/child-deployment-b",
        parameters={
        "i": i,
        "sim_failure_child_flow_b": sim_failure.child_flow_b
        })
    b = flow_run_b.state.result()
    
    flow_run_c = await run_deployment(
        name="child-flow-c/child-deployment-c",
        )
    c = flow_run_c.state.result()

    j = await downstream_task_j(a, c, sim_failure.downstream_task_j)
    k = await downstream_task_k(b)

# ---

if __name__ == "__main__":
    asyncio.run(asyncio_syntax(
        sim_failure=SimulatedFailure(
        child_flow_a=False,
        child_flow_b=False, 
        downstream_task_j=False)
        ))