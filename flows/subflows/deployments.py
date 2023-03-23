from prefect.deployments import Deployment
from child_flows import child_flow_a, child_flow_b, child_flow_c, child_flow_d
from async_python_sub_deployments import async_python_sub_deployments
from async_python_sub_flows import async_python_sub_flows
from task_wrapped_deployments import task_wrapped_deployments
from blocking_subflows import blocking_subflows
from just_tasks import just_tasks


dep_child_a = Deployment.build_from_flow(
    flow=child_flow_a, name="dep-child-a", tags=["subflows", "child"]
)

dep_child_b = Deployment.build_from_flow(
    flow=child_flow_b, name="dep-child-b", tags=["subflows", "child"]
)

dep_child_c = Deployment.build_from_flow(
    flow=child_flow_c, name="dep-child-c", tags=["subflows", "child"]
)

dep_child_d = Deployment.build_from_flow(
    flow=child_flow_d, name="dep-child-d", tags=["subflows", "child"]
)

dep_task_wrapped_deployments = Deployment.build_from_flow(
    flow=task_wrapped_deployments,
    name="dep-task-wrapped-deployments",
    tags=["subflows", "task-wrapped", "parent"],
)

dep_async_py_sub_dep = Deployment.build_from_flow(
    flow=async_python_sub_deployments,
    name="dep-async-py-sub-dep",
    tags=["subflows", "async-py-sub-dep", "parent"],
)

dep_async_py_sub_flow = Deployment.build_from_flow(
    flow=async_python_sub_flows,
    name="dep-async-py-sub-flow",
    tags=["subflows", "async-py-sub-flow", "parent"],
)

dep_blocking_subflows = Deployment.build_from_flow(
    flow=blocking_subflows, name="dep-blocking", tags=["subflows", "blocking", "parent"]
)

dep_just_tasks = Deployment.build_from_flow(
    flow=just_tasks, name="dep-just-tasks", tags=["subflows", "just-tasks", "parent"]
)

if __name__ == "__main__":
    dep_child_a.apply()
    dep_child_b.apply()
    dep_child_c.apply()
    dep_child_d.apply()
    dep_task_wrapped_deployments.apply()
    dep_async_py_sub_dep.apply()
    dep_async_py_sub_flow.apply()
    dep_blocking_subflows.apply()
    dep_just_tasks.apply()
