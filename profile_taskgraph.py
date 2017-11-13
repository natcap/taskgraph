import os
import cProfile
import pstats
import taskgraph
import time
import random
import shutil

token_dir = 'token_dir'

def wait(sleep_time):
    time.sleep(sleep_time)

def main():
    if os.path.exists(token_dir):
        shutil.rmtree(token_dir)
    task_graph = taskgraph.TaskGraph(os.path.join(token_dir, 'test.db'), 0)
    task_set = set()
    for index in xrange(10000):
        task = task_graph.add_task(
            func=wait,
            args=(random.uniform(1e-7, 2e-7),),
            dependent_task_list=random.sample(task_set, min(len(task_set), 10)))
        task_set.add(task)
    task_graph.join()

if __name__ == '__main__':
    cProfile.run('main()', 'taskgraph_profile_stats')
    p = pstats.Stats('taskgraph_profile_stats')
    p.sort_stats('tottime').print_stats(10)
