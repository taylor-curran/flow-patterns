from prefect import flow, task
import random

#  If a state with an identical key is found, Prefect will use the cached state instead of running the task again.
# https://docs.prefect.io/concepts/tasks/#caching

def static_cache_key(context, parameters):
    task_name = context.task.name
    if task_name in parameters['skip_tasks']:
        return "static cache key"
    else:
        #TODO: Do I need to return this or will Prefect handle this for me?
        random_bits = random.getrandbits(128)
        hash = "%032x" % random_bits
        return hash

@task(cache_key_fn=static_cache_key)#, refresh_cache=True)
def task_a(name='task-a', skip_tasks: list = []):
    print('task a')
    return {'a': 'task a'}

@task(name='task-b', cache_key_fn=static_cache_key)#, refresh_cache=True)
def task_b(skip_tasks: list = []):
    print('task b')
    return {'b': 'task b'}

@task(name='task-c', cache_key_fn=static_cache_key)#, refresh_cache=True)
def task_c(skip_tasks: list = []):
    print('task c')
    return {'c': 'task c'}

@flow(persist_result=True)
def my_flow(skip_tasks: list = []):
    a = task_a(skip_tasks=skip_tasks)
    b = task_b(skip_tasks=skip_tasks)
    c = task_c(skip_tasks=skip_tasks)
    return [a, b, c]

if __name__ == '__main__':
    my_flow(skip_tasks=['task-b', 'task-c'])