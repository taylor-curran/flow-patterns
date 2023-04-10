import prefect.runtime as runtime
from prefect import flow, task
from prefect.artifacts import create_link_artifact


@task
def artifact_task():
    # We expect EVERY task we write to do the following
    artifact_name = f'{runtime.task_run.name}-run 0'.replace(' ', '-').replace('_', '-').lower()
    return create_link_artifact(
        link="https://www.prefect.io/",
        link_text="Link to Some Log!",
        key=artifact_name
    )

@task
def artifact_task_2():
    return create_link_artifact(
        link="https://www.prefect.io/",
        link_text="Link to Some Log!",
        key='hi'
    )

@task
def artifact_task_3():
    return create_link_artifact(
        link="https://www.prefect.io/",
        link_text="Link!",
    )


@flow
def artifact_flow():
    artifact_task()
    artifact_task_2()
    artifact_task_3()


if __name__ == '__main__':
    artifact_flow()