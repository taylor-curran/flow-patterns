from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
import time


@task
def example_task():
    time.sleep(3)
    return {"result": "Example result"}


@flow(task_runner=ConcurrentTaskRunner)
def example_flow():
    a = example_task.submit()

    time.sleep(1)

    # get state of task at invocation
    print(a.get_state().type)
    # get value of result upon completion of task
    print(a.result())
    # get final state upon completion of task
    print(a.wait().type)


if __name__ == "__main__":
    example_flow()
