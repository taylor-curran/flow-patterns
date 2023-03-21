from prefect import flow, task
from prefect.deployments import run_deployment
from pydantic import BaseModel
import asyncio


@flow(persist_result=True)
async def child_flow_a(i, sim_failure_child_flow_a):
    print(f"i: {i}")
    if sim_failure_child_flow_a:
        raise Exception("This is a test exception")
    else:
        return {"a": "child flow a"}


@flow(persist_result=True)
async def child_flow_b(i={"i": "upstream task"}, sim_failure_child_flow_b=False):
    print(f"i: {i}")
    if sim_failure_child_flow_b:
        raise Exception("This is a test exception")
    else:
        return {"b": "child flow b"}


@flow(persist_result=True)
async def child_flow_d():
    return {"d": "child flow d"}


# -- Nested Child Flow --
@flow(persist_result=True)
async def child_flow_c():
    d = await child_flow_d()
    return {"c": d}


@task()
async def upstream_task_h():
    print("upstream task")
    return {"h": "upstream task"}


@task()
async def upstream_task_i():
    print("upstream task")
    return {"i": "upstream task"}


@task()
async def downstream_task_j(a, c, sim_failure_downstream_task_j):
    if sim_failure_downstream_task_j:
        raise Exception("This is a test exception")
    else:
        print("downstream task")
        return {"j": "downstream task"}


@task()
async def downstream_task_k(d):
    print("downstream task")
    return {"k": "downstream task"}


# ---


class SimulatedFailure(BaseModel):
    child_flow_a: bool = False
    child_flow_b: bool = False
    downstream_task_j: bool = False


default_simulated_failure = SimulatedFailure(
    child_flow_a=False, child_flow_b=False, downstream_task_j=False
)


# prefect deployment build async_python.py:async_python -n dep-async-py -t sub-flows -t async-py -t parent -a
@flow(persist_result=True)
async def async_python(sim_failure: SimulatedFailure = default_simulated_failure):

    first_round = await asyncio.gather(
        *[upstream_task_h(), upstream_task_i(), child_flow_c()]
    )
    h, i, c = first_round

    second_round = await asyncio.gather(
        *[
            child_flow_a(i, sim_failure.child_flow_a),
            child_flow_b(i, sim_failure.child_flow_b),
        ]
    )

    a, b = second_round

    third_round = await asyncio.gather(
        *[downstream_task_j(a, c, sim_failure.downstream_task_j), downstream_task_k(b)]
    )

    j, k = third_round

    return {"j": j, "k": k}


# ---

if __name__ == "__main__":
    asyncio.run(
        async_python(
            sim_failure=SimulatedFailure(
                child_flow_a=False, child_flow_b=False, downstream_task_j=False
            )
        )
    )


#  if timeout == 0:
#         return flow_run

#     with anyio.move_on_after(timeout):
#         while True:
#             flow_run = await client.read_flow_run(flow_run_id)
#             flow_state = flow_run.state
#             if flow_state and flow_state.is_final():
#                 return flow_run
#             await anyio.sleep(poll_interval)


# In [5]: async def async_fn():
#    ...:     return 10
#    ...:

# In [6]: async_fn()
# Out[6]: <coroutine object async_fn at 0x104ae2ce0>

# In [7]: await async_fn()
# Out[7]: 10

# In [8]:
