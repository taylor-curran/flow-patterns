from prefect import get_client
from prefect.server.schemas.core import TaskRun
from prefect.server.schemas.filters import (
    TaskRunFilter,
    TaskRunFilterName,
    DeploymentFilter,
    DeploymentFilterName,
)


async def demo():
    async with get_client() as client:
        task_runs: list[TaskRun] = await client.read_task_runs(
            task_run_filter=TaskRunFilter(
                name=TaskRunFilterName(like_="taskNamePrefix")
            ),
            deployment_filter=DeploymentFilter(
                name=DeploymentFilterName(like_="deploymentNamePrefix")
            ),
        )

        print(len(task_runs))


if __name__ == "__main__":
    import asyncio

    asyncio.run(demo())
