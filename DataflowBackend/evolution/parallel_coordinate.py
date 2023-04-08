import pandas as pd
from matplotlib import pyplot as plt
from pandas.plotting import parallel_coordinates
from utils.util import *


def counter_parallel_coordinate(tasks):
    direct = []
    for task in tasks:
        if task["hv_type"] != "Map":
            continue
        tmp = {"hv_type": task["hv_type"], "duration": task["end_time"] - task["start_time"]}
        # del task["counter"]["FILE_BYTES_READ"]
        # del task["counter"]["FILE_BYTES_WRITTEN"]
        # del task["counter"]["HDFS_BYTES_READ"]
        # del task["counter"]["HDFS_BYTES_WRITTEN"]
        # del task["counter"]["PHYSICAL_MEMORY_BYTES"]
        # del task["counter"]["VIRTUAL_MEMORY_BYTES"]
        # del task["counter"]["COMMITTED_HEAP_BYTES"]

        tmp.update(task["counter"])
        direct.append(tmp)
    df = pd.DataFrame(direct)
    parallel_coordinates(df, class_column="hv_type")


def main():
    tasks_path = "../data4/Example24/output/FullTask.json"
    tasks = read_json_obj(tasks_path)["changed"]
    print(tasks[0]["counter"])
    plt.figure(figsize=(50, 5))
    counter_parallel_coordinate(tasks)
    plt.show()


if __name__ == '__main__':
    main()
