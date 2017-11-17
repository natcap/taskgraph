import os
import cProfile
import pstats
import taskgraph
import time
import random
import shutil
import logging

logging.basicConfig(
    format='%(asctime)s %(name)-10s %(levelname)-8s %(message)s',
    level=logging.DEBUG, datefmt='%m/%d/%Y %H:%M:%S ')

taskgraph_cache_dir = 'taskgraph_cache_dir'

def wait():
    time.sleep(1e-9)

def main():
    task_graph = taskgraph.TaskGraph(taskgraph_cache_dir, 4)
    task_set = set()
    for index in xrange(100000):
        task = task_graph.add_task(
            func=wait,
            dependent_task_list=random.sample(
                task_set, min(len(task_set), 10)))
        task_set.add(task)
    task_graph.close()
    task_graph.join()

if __name__ == '__main__':
    #main()
    cProfile.run('main()', 'taskgraph_profile_stats')
    p = pstats.Stats('taskgraph_profile_stats')
    p.sort_stats('tottime').print_stats(10)
    p.sort_stats('cumtime').print_stats(10)
