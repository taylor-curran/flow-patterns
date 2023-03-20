from prefect.deployments import Deployment
from child_deployments import child_flow_a, child_flow_b, child_flow_c, child_flow_d
from async_python import async_python
from task_wrapped_deployments import task_wrapped_deployments
from blocking_subflows import blocking_subflows


dep_child_a = Deployment.build_from_flow(
    flow=child_flow_a,
    name="dep-child-a",
    tags=["subflows", "child"]
)

dep_child_b = Deployment.build_from_flow(
    flow=child_flow_b,
    name="dep-child-b",
    tags=["subflows", "child"]
)

dep_child_c = Deployment.build_from_flow(
    flow=child_flow_c,
    name="dep-child-c",
    tags=["subflows", "child"]
)

dep_child_d = Deployment.build_from_flow(
    flow=child_flow_d,
    name="dep-child-d",
    tags=["subflows", "child"]
)

dep_task_wrapped_deployments = Deployment.build_from_flow(
    flow=task_wrapped_deployments,
    name="dep-task-wrapped-deployments",
    tags=["subflows", "task-wrapped", "parent"]
)

dep_asyncio = Deployment.build_from_flow(
    flow=async_python,
    name="dep-async-py",
    tags=["subflows", "async-py", "parent"]
)

if __name__ == "__main__":
    dep_child_a.apply()
    dep_child_b.apply()
    dep_child_c.apply()
    dep_child_d.apply()
    dep_task_wrapped_deployments.apply()
    dep_asyncio.apply()
