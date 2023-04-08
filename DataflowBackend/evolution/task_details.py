import math

import matplotlib.pyplot as plt
import numpy as np

from utils.import_config import *


def cal_w(t, k):
    return pow(math.e, -(t - k) ** 2 / k ** 2)


def cal_mean(cur_id, l, id_range, raw_info):
    w_list = [cal_w(raw_info[cur_id][0], raw_info[i][0]) for i in id_range]
    sum_val = sum(w_list)
    tmp = [(raw_info[i][1] * w_list[i - l]) for i in id_range]
    return sum(tmp) / sum_val


def cal_variance(cur_id, l, miu, id_range, raw_info):
    w_list = [cal_w(raw_info[cur_id][0], raw_info[i][0]) for i in id_range]
    sum_val = sum(w_list)

    tmp = [((raw_info[i][1] ** 2 - miu ** 2) * w_list[i - l]) for i in id_range]

    return (sum(tmp) / sum_val) ** 0.5


def draw_smooth_statics_plot(tasks, vertex):
    plt.figure(figsize=(20, 20))
    grid = plt.GridSpec(4, 1, wspace=0.2, hspace=0.2)
    ax_main = plt.subplot(grid[:2, 0])
    ax_input_var = plt.subplot(grid[2:3, 0])
    ax_process_var = plt.subplot(grid[3:4, 0])

    contain_set = {}
    cur_baseline = 1
    start_time = sys.maxsize
    end_time = -1
    for task in tasks:
        if task["vex_name"] != vertex:
            continue
        contain = task["container"]
        if contain not in contain_set:
            contain_set[contain] = cur_baseline
            cur_baseline += 2
        start_time = min(start_time, task["start_time"])
        end_time = max(end_time, task["end_time"])

    for task in tasks:
        input_time = []
        input_val = []
        process_time = []
        process_val = []

        if task["vex_name"] != vertex:
            continue
        # if task["machine"] != task["data_machine"]:
        #     continue
        # if contain_set[task["container"]] != 19:
        # if contain_set[task["container"]] != 7:
        # continue
        # print(task["machine"], task["data_machine"])
        for idx, _ in enumerate(task["input_speed"]):
            if not task["input_speed"][idx]["speed"] == 0:
                input_val.append(
                    1 / task["input_speed"][idx]["speed"])
                input_time.append(task["input_speed"][idx]["time"])
            if not task["process_speed"][idx]["speed"] == 0:
                process_val.append(
                    1 / task["process_speed"][idx]["speed"])
                process_time.append(task["process_speed"][idx]["time"])
        cur_baseline = contain_set[task["container"]]

        input_zip = list(zip(input_time, input_val))
        process_zip = list(zip(process_time, process_val))

        avg_size = len(input_zip) // 200
        # print("interval: ", avg_size)
        input_mean = []
        input_var = []
        process_mean = []
        process_var = []

        for i in range(len(input_zip)):
            l = max(0, i - avg_size)
            u = min(i + avg_size, len(input_zip))
            miu = cal_mean(i, l, range(l, u), input_zip)
            var = cal_variance(i, l, miu, range(l, u), input_zip)
            input_var.append(var)
            input_mean.append(miu)

        for i in range(len(process_zip)):
            l = max(0, i - avg_size)
            u = min(i + avg_size, len(process_zip))
            miu = cal_mean(i, l, range(l, u), process_zip)
            var = cal_variance(i, l, miu, range(l, u), process_zip)
            process_var.append(var)
            process_mean.append(miu)
        ax_main.fill_between(input_time, [x + cur_baseline for x in input_val],
                             y2=cur_baseline, color="red", linewidth=0)
        ax_main.fill_between(process_time, [-x + cur_baseline for x in process_val],
                             y2=cur_baseline, color="#3182bd", linewidth=0)
        ax_input_var.plot(input_time, input_var)
        ax_process_var.plot(process_time, process_var)

    # ax_main.set_xlim([start_time, end_time])
    # ax_main.set_ylim([10, 12])

    plt.show()


def draw_tasks_speed(tasks, vertex):
    plt.figure(figsize=(20, 20))
    contain_set = {}
    cur_baseline = 1
    start_time = sys.maxsize
    end_time = -1
    for task in tasks:
        # if task["vex_name"] != vertex:
        #     continue

        # if task["machine"] != "dbg04":
        #     continue
        contain = task["container"]
        if contain not in contain_set:
            contain_set[contain] = cur_baseline
            cur_baseline += 2
        start_time = min(start_time, task["start_time"])
        end_time = max(end_time, task["end_time"])
    vex_set = set()
    for task in tasks:
        if task["vex_name"] == vertex:
            continue
        contain = task["container"]
        if task["start_time"] < end_time and contain in contain_set:
            vex_set.add(task["vex_name"])

    for c in contain_set:
        plt.plot([start_time, end_time], [contain_set[c] + 1, contain_set[c] + 1], c="grey")

    for task in tasks:
        input_time = []
        input_val = []
        process_time = []
        process_val = []
        if task["container"] not in contain_set:
            continue
        if task["vex_name"] != vertex and task["vex_name"] not in vex_set:
            continue
        # if task["machine"] != "dbg04":
        #     continue
        # if task["machine"] != task["data_machine"]:
        #     continue
        # if contain_set[task["container"]] != 11:
        #     continue
        for idx, _ in enumerate(task["input_speed"]):
            # if task["input_speed"][idx]["speed"] * task["process_speed"][idx]["speed"] == 0:
            #     continue
            # input_val.append(1 / max(10000, task["input_speed"][idx]["speed"]))
            input_val.append(1 / task["input_speed"][idx]["speed"] if task["input_speed"][idx]["speed"] != 0 else 1)

            input_time.append(task["input_speed"][idx]["time"])
            # process_val.append(1 / max(10000, task["process_speed"][idx]["speed"]))
            process_val.append(
                1 / task["process_speed"][idx]["speed"] if task["process_speed"][idx]["speed"] != 0 else 1)

            process_time.append(task["process_speed"][idx]["time"])
        cur_baseline = contain_set[task["container"]]
        if task["vex_name"] == vertex:
            plt.fill_between(input_time, [x + cur_baseline for x in input_val],
                             y2=cur_baseline, color="red", linewidth=0)
            plt.fill_between(process_time, [-x + cur_baseline for x in process_val],
                             y2=cur_baseline, color="#3182bd", linewidth=0)
        elif "Redu" in task["vex_name"]:
            plt.fill_between([task["start_time"], task["end_time"]], [0.8 + cur_baseline, 0.8 + cur_baseline],
                             y2=cur_baseline, color="green", linewidth=0)
            plt.fill_between([task["start_time"], task["end_time"]], [-0.8 + cur_baseline, -0.8 + cur_baseline],
                             y2=cur_baseline, color="green", linewidth=0)
        else:
            plt.fill_between([task["start_time"], task["end_time"]], [0.8 + cur_baseline, 0.8 + cur_baseline],
                             y2=cur_baseline, color="grey", linewidth=0)
            plt.fill_between([task["start_time"], task["end_time"]], [-0.8 + cur_baseline, -0.8 + cur_baseline],
                             y2=cur_baseline, color="grey", linewidth=0)

    plt.xlim([start_time, end_time])
    plt.show()


def draw_process(tasks, container_map):
    start_time = sys.maxsize
    end_time = 0
    container_tasks = {}
    for task in tasks:
        container = task["container"]
        if container not in container_tasks:
            container_tasks.update({container: []})
        container_tasks[container].append(
            (task["step_info"][4], task["end_time"]))
        if task["step_info"][4] * task["end_time"] == 0:
            print(0)
        start_time = min(start_time, task["step_info"][4])
        end_time = max(end_time, task["end_time"])
    print(start_time, end_time, end_time - start_time)
    for container in container_map:
        id = container_map[container]["id"]
        times = container_tasks[container]
        x_vals = []
        y_vals = []
        for start_time, end_time in times:
            x_vals.append(start_time)
            x_vals.append(start_time)
            x_vals.append(end_time)
            x_vals.append(end_time)

            y_vals.append(id)
            y_vals.append(id + 1)
            y_vals.append(id + 1)
            y_vals.append(id)
        plt.fill_between(x_vals, y_vals, y2=id, color="blue", linewidth=0, alpha=0.5)
    return start_time, end_time


def draw_process_outline(tasks, container_map):
    start_time = sys.maxsize
    end_time = 0
    container_tasks = {}
    for task in tasks:
        container = task["container"]
        if container not in container_tasks:
            container_tasks.update({container: []})
        container_tasks[container].append(
            (task["start_time"], task["end_time"]))
        start_time = min(start_time, task["step_info"][4])
        end_time = max(end_time, task["end_time"])
    for container in container_map:
        id = container_map[container]["id"]
        times = container_tasks[container]
        times = sorted(times, key=lambda item: item[0])
        x_vals = []
        y_vals = []
        for start_time, end_time in times:
            x_vals.append(start_time)
            x_vals.append(start_time)
            x_vals.append(end_time)
            x_vals.append(end_time)

            y_vals.append(id)
            y_vals.append(id + 1)
            y_vals.append(id + 1)
            y_vals.append(id)
        plt.plot(x_vals, y_vals, color="grey", linewidth=0.5)
        plt.plot([times[0][0], times[-1][0]], [id, id], color="grey", linewidth=0.5)

    return start_time, end_time


def machine_scatter(tasks):
    color_lists = ["#a6cee3", "#1f78b4", "#b2df8a", "#33a02c", "#fb9a99", "#e31a1c", "#fdbf6f", "#ff7f00", "#cab2d6"]
    machine_map = {}
    for task in tasks:
        machine = task["machine"]
        if machine not in machine_map:
            machine_map[machine] = []
        machine_map[machine].append(task)
    print(machine_map.keys())
    idx = 0
    for m in machine_map:
        tasks_tmp = machine_map[m]
        plt.scatter([task["start_time"] for task in tasks_tmp],
                    [task["end_time"] - task["start_time"] for task in tasks_tmp], alpha=0.5)
        idx += 1
    plt.show()


def draw_pie(tasks, vertex):
    input_data = []
    print(tasks[0]["vec_name"])
    for task in tasks:
        if task["vec_name"] != vertex:
            continue
        # print(task["counter"])
        input_data.append(task["counter"]["FILE_BYTES_READ"])
    plt.pie(input_data)
    plt.show()


def draw_time_distribution(tasks, vertex):
    input_data = []
    for task in tasks:
        if task["vec_name"] != vertex:
            continue
        # print(task["counter"])
        input_data.append((task["start_time"], task["end_time"] - task["start_time"]))
    plt.scatter([task[0] for task in input_data], [task[1] for task in input_data])
    # plt.pie(input_data)
    plt.show()


def com_tasks_time(tasks, vertex):
    location_tasks = []

    for task in tasks:
        if task["vex_name"] != vertex:
            continue
        if task["data_machine"] != task["machine"]:
            location_tasks.append(task)
    diff = []
    durations = []
    input_times = []
    process_times = []
    bytes_list = []
    for task in location_tasks[:]:
        # if task["counter"]["HDFS_BYTES_READ"] + task["counter"]["FILE_BYTES_READ"] > 288439552:
        #     continue
        # if task["counter"]["HDFS_BYTES_READ"] + task["counter"]["FILE_BYTES_READ"] < 208439552:
        #     continue
        duration = task["end_time"] - task["start_time"]
        input_time = 0
        for t in task["input_speed"]:
            input_time += t["speed"]
            # if input_time > 1150:
            #     break
        input_times.append(input_time)
        process_time = 0
        for t in task["process_speed"]:
            process_time += t["speed"]
        process_times.append(process_time)
        diff.append(duration - process_time - input_time)
        durations.append(process_time + input_time)
        bytes_list.append(task["counter"]["HDFS_BYTES_READ"] + task["counter"]["FILE_BYTES_READ"])
        # print(task["counter"].keys())
        # bytes_list.append(task["counter"]["OUTPUT_BYTES"])
    # print(np.var(diff))
    plt.hist(diff)

    # plt.bar(range(len(input_times)), input_times, color="blue", label="InputTime")
    # plt.bar(range(len(process_times)), process_times, bottom=input_times, color="grey", label="ProcessTime")
    # plt.bar(range(len(diff)), diff, bottom=durations, color="red", label="OtherTime")
    # plt.legend(frameon=False)

    # bytes = (location_tasks[0]["counter"]["HDFS_BYTES_READ"])
    # bytes = bytes_list[0]
    # print(bytes_list[0])
    # print(bytes / input_times[0])
    # print(bytes / process_times[0])
    # print([input_times[0], process_times[0], diff[0]])
    # plt.pie([input_times[0], process_times[0], diff[0]])
    # plt.scatter(input_times, process_times)
    # plt.ylim([750, 4500])
    # p_speeds = [bytes_list[idx] / process_times[idx] for idx in range(len(process_times))]
    # i_speeds = [bytes_list[idx] / input_times[idx] for idx in range(len(process_times))]
    #
    # plt.hist(p_speeds)
    # print(np.var(p_speeds), np.std(p_speeds), np.std(p_speeds) / np.mean(p_speeds))
    # print(np.var(i_speeds), np.std(i_speeds), np.std(i_speeds) / np.mean(i_speeds))
    plt.ylim([0, 60])
    plt.show()


def tasks_stack_graph(tasks, vertex):
    min_duration = sys.maxsize
    max_duration = 0
    for task in tasks:
        if task["vex_name"] != vertex:
            continue
        min_duration = min(min_duration, task["end_time"] - task["start_time"])
        max_duration = max(max_duration, task["end_time"] - task["start_time"])
    gap = (max_duration - min_duration + 1) / 8
    lists = [[0, 0] for _ in range(8)]
    for task in tasks:
        if task["vex_name"] != vertex:
            continue
        duration = task["end_time"] - task["start_time"]
        is_location = task["machine"] != task["data_machine"]

        idx = int((duration - min_duration) // gap)
        if is_location:
            lists[idx][0] += 1
        else:
            lists[idx][1] += 1
    print(lists)
    idx = [(min_duration + gap * 0.5 + gap * i) for i in range(8)]
    plt.bar(idx, [item[1] for item in lists], width=100, color="g", label="WithoutLocation")
    plt.bar(idx, [item[0] for item in lists], width=100, bottom=[item[1] for item in lists], color="r",
            label="Location")
    plt.show()


def draw_single_task_speed(tasks, vertex):
    plt.figure(figsize=(20, 20))
    start_time = sys.maxsize
    end_time = -1

    for task in tasks:
        input_time = []
        input_val = []
        process_time = []
        process_val = []

        if task["vex_name"] != vertex:
            continue
        if task["machine"] == task["data_machine"]:
            continue
        if task["task_id"] != "attempt_1659945682372_0180_1_04_000086_0":
            continue

        for idx, _ in enumerate(task["input_speed"]):
            input_val.append(1 / task["input_speed"][idx]["speed"] if task["input_speed"][idx]["speed"] != 0 else 1)
            input_time.append(task["input_speed"][idx]["time"])
            # process_val.append(1 / max(10000, task["process_speed"][idx]["speed"]))
            process_val.append(
                1 / task["process_speed"][idx]["speed"] if task["process_speed"][idx]["speed"] != 0 else 1)

            process_time.append(task["process_speed"][idx]["time"])
        input_width = []
        process_width = []
        for i in range(1, len(input_time)):
            input_width.append(input_time[i] - input_time[i - 1])
            process_width.append(process_time[i] - process_time[i - 1])
        print(len([(input_time[i] + input_time[i - 1]) / 2 for i in range(1, len(input_time))]), len(input_width))
        plt.bar([(input_time[i] + input_time[i - 1]) / 2 for i in range(1, len(input_time))], input_val[1:],
                width=input_width, linewidth=0,
                color="red")
        plt.bar([(process_time[i] + process_time[i - 1]) / 2 for i in range(1, len(process_time))],
                [-x for x in process_val[1:]], width=process_width, linewidth=0,
                color="#3182bd")

        # plt.fill_between(input_time, [x + cur_baseline for x in input_val],
        #                  y2=cur_baseline, color="red", linewidth=1)
        # plt.fill_between(process_time, [-x + cur_baseline for x in process_val],
        #                  y2=cur_baseline, color="#3182bd", linewidth=1)
        print(min(process_val))
        break

    # plt.xlim([start_time, end_time])
    plt.show()


def verify_fetch(tasks):
    times = []
    for task in tasks:
        if task["fetches"]:
            for attempt in task["fetches"]:
                tmp = task["fetches"][attempt]
                if "time_taken" in tmp:
                    times.append(tmp["time_taken"])
    print(max(times), len(times))


def draw_fetch(container_map):
    # print(container_id)
    for container in container_map:
        id = container_map[container]["id"]
        times = container_map[container]["times"]
        # print(id, times)
        x_vals = []
        y_vals = []
        for end_time, duration in times:
            # duration = max(duration, 100)
            x_vals.append(end_time - duration)
            x_vals.append(end_time - duration)
            x_vals.append(end_time)
            x_vals.append(end_time)

            y_vals.append(id)
            y_vals.append(id + 1)
            y_vals.append(id + 1)
            y_vals.append(id)

        plt.fill_between(x_vals, y_vals, y2=id, color="red", linewidth=0)
        # break
    return container_map
    # plt.show()


def get_container_map(tasks):
    plt.figure(figsize=(20, 20))

    container_id = 0
    container_map = {}
    for task in tasks:
        container = task["container"]
        if container not in container_map:
            container_map.update({container: {"id": container_id, "times": []}})
            container_id += 2
    for task in tasks:
        container = task["container"]
        for attempt in task["fetches"]:
            tmp = task["fetches"][attempt]
            if "time_taken" in tmp:
                container_map[container]["times"].append(
                    (tmp["end_time"], tmp["time_taken"]))
                print(tmp)
                break

    return container_map


def draw_reducer_begin(tasks):
    start_time = sys.maxsize
    for task in tasks:
        if "Reducer 9" in task["vex_name"]:
            print(task.keys())
            print(task["input_bytes_info"])
            print(task["processed_bytes_info"])

            break
            start_time = min(start_time, task["step_info"][4])
    # plt.plot([start_time, start_time], [0, 96], c="black")


def reducer_test(tasks):
    for task in tasks:
        if task["vex_name"] == "Reducer 5":
            print(task)


def reducer_shuffle_sample(tasks):
    task_tar = tasks[0]
    for task in tasks:
        if task["vex_name"] == "Reducer 9":
            task_tar = task
            break
    print(task_tar["fetches"])
    shuffle_info = []
    for attempt in task_tar["fetches"]:
        tmp = task_tar["fetches"][attempt]
        if "time_taken" in tmp:
            shuffle_info.append(tmp)
    print(shuffle_info)


def main():
    tasks_file = "../data/log/query54/tasks.json"
    tasks_file = "../data/log/query29/tasks.json"

    # tasks_file = "../data/log/Example39/output/FullTask.json"  # data skew
    # tasks_file = "../data/log/query1/tasks.json"

    tasks = read_json_obj(tasks_file)
    reducer_shuffle_sample(tasks)
    # verify_fetch(tasks)
    # container_map = get_container_map(tasks)
    # draw_process_outline(tasks, container_map)
    # draw_process(tasks, container_map)
    # draw_fetch(container_map)
    # draw_reducer_begin(tasks)
    # plt.xlim([1661180661563.0,1661180804031.0]) #query54
    # plt.xlim([1658292495330, 1658292495330 + 73462])  # query29 long
    # plt.xlim([1658292648539.0, 1658292724350.0])
    # plt.show()
    # 42419.0 73462.0
    # draw_tasks_speed(tasks, "Map 1")
    # draw_single_task_speed(tasks, "Map 1")
    # com_tasks_time(tasks, "Map 1")
    # tasks_stack_graph(tasks, "Map 15")
    # vertex_info(tasks)
    # draw_smooth_statics_plot(tasks, "Map 1")
    # draw_pie(tasks, "Reducer 2")
    # draw_time_distribution(tasks, "Reducer 2")
    # machine_scatter(tasks)


if __name__ == '__main__':
    main()
