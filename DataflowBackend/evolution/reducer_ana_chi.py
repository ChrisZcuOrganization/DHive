import json
import math
import re
import sys
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import patches

tasks_1009 = "./chi_9.2/log/reducer_1009/tasks.json"
tasks_150 = "./chi_9.2/log/reducer_150/tasks.json"


def read_json_obj(filename: str):
    with open(filename, 'r', encoding='utf-8') as fp:
        json_obj = json.load(fp)
    return json_obj


def draw_mr(num, tasks):
    # vex_map = {}
    # for task in tasks:
    #     vex_name = task["vex_name"]
    #     if "Map" in vex_name:
    #         continue
    #     start_time = task["start_time"]
    #     min_start_time = min(min_start_time, start_time)
    #     end_time = task["end_time"]
    #     if vex_name not in vex_map:
    #         vex_map.update({
    #             vex_name: {
    #                 "start_time": [],
    #                 "duration": []
    #             }
    #         })
    #     vex_map[vex_name]["duration"].append(end_time - start_time)
    #     vex_map[vex_name]["start_time"].append(start_time)
    m_x_val = []
    m_y_val = []

    r_x_val = []
    r_y_val = []

    min_start_time = tasks[0]["start_time"]
    max_end_time = tasks[0]["end_time"]
    for task in tasks:
        if "Map" in task["vex_name"]:
            continue
        start_time = task["start_time"]
        end_time = task["end_time"]
        min_start_time = min(start_time, min_start_time)
        max_end_time = max(end_time, max_end_time)

        if "Map" in task["vex_name"]:
            m_x_val.append(start_time)
            m_y_val.append(end_time - start_time)
        else:
            r_x_val.append(start_time)
            r_y_val.append(end_time - start_time)
    plt.scatter([t - min_start_time for t in m_x_val], m_y_val, c="blue", alpha=0.5)
    plt.scatter([t - min_start_time for t in r_x_val], r_y_val, c="red", alpha=0.5)
    plt.xlim([0, 140000])
    plt.title(num)
    plt.show()


def reducer_time(num, tasks):
    inits = []
    shuffles = []
    processes = []
    sinks = []
    total_times = []
    for task in tasks:
        if "Map" in task["vex_name"]:
            continue
        step_info = task["step_info"]
        if (step_info[3] - step_info[2]) < 0:
            continue
        end_time = task["end_time"]
        start_time = task["start_time"]
        inits.append((step_info[1] - step_info[0]) / (end_time - start_time) / 1000)
        shuffles.append((step_info[3] - step_info[2]) / (end_time - start_time) / 1000)
        processes.append((step_info[5] - step_info[4]) / (end_time - start_time) / 1000)
        sinks.append((step_info[9] - step_info[8]) / (end_time - start_time) / 1000)
        total_times.append((end_time - start_time))
        # if (step_info[3] - step_info[2]) < 0:
        #     print(task)
    # plt.hist(inits)
    # plt.hist(shuffles)
    # plt.hist(processes)
    plt.hist(total_times)
    # plt.ylim([0, 1200])
    # plt.xlim([0, 1])
    plt.title(num)
    plt.show()


def reducer_total_ratio(tasks):
    inits = []
    shuffles = []
    processes = []
    sinks = []
    total_times = []
    num = 0
    for task in tasks:
        if "Map" in task["vex_name"]:
            continue
        num += 1
        step_info = task["step_info"]
        if (step_info[3] - step_info[2]) < 0:
            continue
        end_time = task["end_time"]
        start_time = task["start_time"]
        inits.append((step_info[1] - step_info[0]) / 1000)
        shuffles.append((step_info[3] - step_info[2]) / 1000)
        processes.append((step_info[5] - step_info[4]) / 1000)
        sinks.append((step_info[9] - step_info[8]) / 1000)
        total_times.append(end_time - start_time)

    print(sum(total_times) / 1000 / num, sum(inits) / 1000 / num, sum(shuffles) / 1000 / num,
          sum(processes) / 1000 / num)
    # print(sum(inits) / sum(total_times), sum(shuffles) / sum(total_times), sum(processes) / sum(total_times),
    #       sum(sinks) / sum(total_times))


def parallelism(tasks):
    cur_cnt = 0
    count = 0
    time_tups = []
    for task in tasks:
        if "Map" in task["vex_name"]:
            continue
        time_tups.append((task["start_time"], "start_time"))
        time_tups.append((task["end_time"], "end_time"))

    time_tups.sort()
    for t_t in time_tups:
        if t_t[1] == "start_time":
            cur_cnt += 1
            count = max(count, cur_cnt)
        else:
            cur_cnt -= 1
    return count


def total_time(tasks):
    min_start_time = sys.maxsize
    max_end_time = 0

    for task in tasks:
        min_start_time = min(min_start_time, task["start_time"])
        max_end_time = max(max_end_time, task["end_time"])

    return max_end_time - min_start_time


REDUCERS = ["Reducer 2", "Reducer 14",
            "Reducer 15", "Reducer 16", "Reducer 17",
            "Reducer 3",
            "Reducer 6", "Reducer 7"]


def total_red_time(tasks):
    min_start_time = sys.maxsize
    max_end_time = 0

    for task in tasks:
        if "Map" in task["vex_name"] or task["vex_name"] != REDUCERS[-1]:
            continue
        min_start_time = min(min_start_time, task["start_time"])
        max_end_time = max(max_end_time, task["end_time"])

    return max_end_time - min_start_time


def rd_vertex_hist(num, tasks):
    col = 4
    lines = math.ceil(len(REDUCERS) / col)
    vex_map = {}
    for task in tasks:
        vex_name = task["vex_name"]
        if "Map" in vex_map:
            continue
        start_time = task["start_time"]
        end_time = task["end_time"]
        if vex_name not in vex_map:
            vex_map.update({
                vex_name: []
            })
        vex_map[vex_name].append(end_time - start_time)

    fig = plt.figure(figsize=(10, 5))
    for idx, rd in enumerate(REDUCERS):
        ax = fig.add_subplot(lines, col, idx + 1)
        ax.hist(vex_map[rd])
        ax.set_title(str(num) + rd)
    plt.tight_layout()
    plt.show()


def rd_vertex_scatter(num, tasks):
    col = 4
    lines = math.ceil(len(REDUCERS) / col)
    vex_map = {}
    min_start_time = sys.maxsize
    for task in tasks:
        vex_name = task["vex_name"]
        start_time = task["start_time"]
        min_start_time = min(min_start_time, start_time)
        if "Map" in vex_name:
            continue
        start_time = task["start_time"]
        min_start_time = min(min_start_time, start_time)
        end_time = task["end_time"]
        if vex_name not in vex_map:
            vex_map.update({
                vex_name: {
                    "start_time": [],
                    "duration": []
                }
            })
        vex_map[vex_name]["duration"].append(end_time - start_time)
        vex_map[vex_name]["start_time"].append(start_time)

    fig = plt.figure(figsize=(10, 5))
    for idx, rd in enumerate(REDUCERS):
        ax = fig.add_subplot(lines, col, idx + 1)
        # for rd_2 in REDUCERS:
        #     if rd_2 == rd:
        #         continue
        #     ax.scatter(vex_map[rd]["start_time"], vex_map[rd]["duration"], alpha=0.5, c="blue")
        ax.scatter([(t - min_start_time) for t in vex_map[rd]["start_time"]], vex_map[rd]["duration"], c="red", alpha=1)
        ax.set_title(str(num) + rd)
        ax.set_xlim([0, 140000])
        ax.set_ylim([0, 10000])
    plt.tight_layout()
    plt.show()


def draw_rd_vertex_hl(num, tasks, vertex, fig, line, col, idx):
    x_val = []
    y_val = []
    min_start_time = sys.maxsize
    max_end_time = 0

    vex_start_time = sys.maxsize
    vex_end_time = 0

    h_x_val = []
    h_y_val = []
    for task in tasks:
        vex_name = task["vex_name"]
        machine_tmp = task["machine"]
        min_start_time = min(min_start_time, task["start_time"])
        max_end_time = max(max_end_time, task["end_time"])
        if "Map" in vex_name:
            continue
        tmp_x_val = x_val
        tmp_y_val = y_val
        if vex_name == vertex:
            vex_start_time = min(vex_start_time, task["start_time"])
            vex_end_time = max(vex_end_time, task["end_time"])

            tmp_x_val = h_x_val
            tmp_y_val = h_y_val
        start_time = task["start_time"]
        end_time = task["end_time"]
        step_info = task["step_info"]
        if step_info[3] - step_info[0] < 0:
            continue
        tmp_x_val.append(start_time / 1000)
        tmp_y_val.append((step_info[3] - step_info[0]) / 1000000)
        # tmp_y_val.append(end_time - start_time)
    ax = fig.add_subplot(line, col, idx + 1)
    ax.scatter([t - min_start_time / 1000 for t in x_val], y_val, c="blue", alpha=0.5)
    ax.scatter([t - min_start_time / 1000 for t in h_x_val], h_y_val, c="red", alpha=0.5)
    ax.set_title(f'{num},{vertex}')
    ax.set_xlim([0, 140])
    ax.set_ylim([0, 5])

    print(vex_start_time - min_start_time, end=", ")
    print(vex_end_time - min_start_time, end=", ")
    print(vex_end_time - vex_start_time)


def draw_rd_vertex(num, tasks, vertex, fig, line, col, idx):
    x_val = []
    y_val = []
    min_start_time = sys.maxsize
    max_end_time = 0
    for task in tasks:
        vex_name = task["vex_name"]
        machine_tmp = task["machine"]
        if vex_name != vertex:
            continue
        if machine != "all":
            if machine_tmp != machine:
                continue
        start_time = task["start_time"]
        end_time = task["end_time"]
        step_info = task["step_info"]
        min_start_time = min(min_start_time, start_time)
        max_end_time = max(max_end_time, end_time)
        x_val.append(start_time)
        # y_val.append((step_info[3] - step_info[0]) / 1000)
        y_val.append(end_time - start_time)
    ax = fig.add_subplot(line, col, idx + 1)
    ax.scatter([t - min_start_time for t in x_val], y_val, alpha=0.5)
    ax.set_title(f'{machine},{num},{vertex}')
    print(max_end_time - min_start_time)
    # ax.set_xlim([0, 26000])
    # ax.set_ylim([0, 8000])


machine = "all"


def draw_container(num, tasks, reducer, fig, line, col, idx):
    containers = {}
    min_start_time = sys.maxsize
    max_end_time = 0
    for task in tasks:
        vex_name = task["vex_name"]
        if vex_name != reducer:
            continue
        container = task["container"]
        if container not in containers:
            containers.update({
                container: []
            })
        min_start_time = min(min_start_time, task["start_time"])
        max_end_time = max(max_end_time, task["end_time"])
        containers[container].append((task["start_time"], task["end_time"]))

    ax = fig.add_subplot(line, col, idx + 1)
    # print(max_end_time - min_start_time)
    for container in containers:
        print(num, len(containers[container]))
    print()
    colors = ["red", "green", "blue"]
    for id_c, container in enumerate(containers.keys()):
        tasks_t = containers[container]
        tasks_t.sort()
        # print(tasks_t[-1][0] - min_start_time)
        # print([(t[0] - min_start_time, t[1] - min_start_time) for t in tasks_t])
        for id, tup in enumerate(tasks_t):
            # print((tup[0] - min_start_time) / 1000, id_c * 3 + 3, (tup[1] - tup[0]))
            ax.add_patch(patches.Rectangle(
                ((tup[0] - min_start_time), id_c * 3 + 3),
                (tup[1] - tup[0]),
                3,
                edgecolor='grey',
                facecolor=colors[id % 3],
                fill=colors[id % 3]
            ))
    # ax.set_xlim([0, 25000])
    ax.plot([0, 0], [0, 1])
    ax.set_title(f"{num}, {reducer}")


def draw_container_ipo(num, tasks, reducer, fig, line, col, idx):
    total_p = 0
    containers = {}
    min_start_time = sys.maxsize
    max_end_time = 0
    for task in tasks:
        vex_name = task["vex_name"]
        if vex_name != reducer:
            continue
        container = task["container"]
        if container not in containers:
            containers.update({
                container: []
            })
        min_start_time = min(min_start_time, task["step_info"][0])
        max_end_time = max(max_end_time, task["end_time"])
        containers[container].append((task["start_time"], task["end_time"], task["step_info"]))
    ax = fig.add_subplot(line, col, idx + 1)
    # print(max_end_time - min_start_time)
    colors = ["red", "green", "blue"]
    for id_c, container in enumerate(containers.keys()):
        tasks_t = containers[container]
        tasks_t.sort()
        # print(tasks_t[-1][0] - min_start_time)
        # print([(t[0] - min_start_time, t[1] - min_start_time) for t in tasks_t])
        min_start_time = tasks_t[0][2][0]
        for id, tup in enumerate(tasks_t):
            # print((tup[0] - min_start_time) / 1000, id_c * 3 + 3, (tup[1] - tup[0]))
            # print(tup[2][0] / 1000, min_start_time / 1000, tup[2][3] / 1000, tup[2][0] / 1000)
            ax.add_patch(patches.Rectangle(
                ((tup[2][0] / 1000 - min_start_time / 1000), id_c * 3 + 3),
                (tup[2][3] / 1000 - tup[2][0] / 1000),
                3,
                edgecolor='grey',
                facecolor='red',
                fill='red'
            ))
            total_p += (tup[2][5] / 1000 - tup[2][3] / 1000)
            ax.add_patch(patches.Rectangle(
                ((tup[2][4] / 1000 - min_start_time / 1000), id_c * 3 + 3),
                (tup[2][5] / 1000 - tup[2][4] / 1000),
                3,
                edgecolor='grey',
                facecolor='green',
                fill='green'
            ))
            ax.add_patch(patches.Rectangle(
                ((tup[2][5] / 1000 - min_start_time / 1000), id_c * 3 + 3),
                tup[1] - tup[0] - (tup[2][5] - tup[2][0]) / 1000,
                3,
                edgecolor='grey',
                facecolor='blue',
                fill='blue'
            ))
    ax.set_xlim([0, 25000])
    ax.plot([0, 0], [0, 1])
    ax.set_title(f"{num}, {reducer}")
    print(num, " ", total_p)


def shuffle_process_ol(num, tasks, vertex):
    ov_num = 0
    for task in tasks:
        step_info = task["step_info"]
        if task["vex_name"] != vertex:
            continue
        if step_info[3] > step_info[4]:
            ov_num += 1
    print(num, vertex, ov_num)


def machine_dis(num, tasks, vertex, fig, line, col, idx):
    machines = []
    for task in tasks:
        vex_name = task["vex_name"]
        if vex_name != vertex:
            continue
        machines.append(task["machine"])
    ax = fig.add_subplot(line, col, idx + 1)
    ax.hist(machines)
    ax.set_title(f"{num}{vertex}")
    ax.tick_params(axis="x", labelrotation=45)


def fetch_machine(num, tasks, vex_name, fig, line, col, idx):
    x_val = []
    y_val = []
    for task in tasks:
        total_fetch = 0
        o_m_num = 0
        t_m = task["machine"]
        vertex = task["vex_name"]
        if vertex != vex_name:
            continue
        fetches = task["fetches"]
        # print(t_m, fetches.keys())
        for m in fetches:
            if m != t_m:
                o_m_num += len(fetches[m])
            total_fetch += len(fetches[m])
        step_info = task["step_info"]
        # y_val.append(task["end_time"] - task["start_time"])
        x_val.append(step_info[3] / 1000 - step_info[0] / 1000)
        y_val.append(task["fetch_num"])
    ax = fig.add_subplot(line, col, idx + 1)
    ax.scatter(x_val, y_val, alpha=0.5)
    ax.set_title(f"{num},{vex_name}")


def fetch_bytes(num, tasks, vertex, fig, line, col, idx):
    x_val = []
    y_val = []
    machines = {}
    for task in tasks:
        total_size = 0
        if task["step_info"][3] - task["step_info"][0] < 0:
            continue
        if task["vex_name"] != vertex:
            continue
        machine = task["machine"]
        if machine not in machines:
            machines.update({machine: [[], [], [], []]})
        fetches_item = task["fetches_item"]
        task_fetch = 0
        for item in fetches_item:
            # print(item)
            total_size += int(re.search(r"(.*), dsize", item["csize"]).group(1)) / 1024 / 1024
            task_fetch += int(re.search(r"(.*), dsize", item["csize"]).group(1)) / 1024 / 1024
            # machines[machine][0].append(int(re.search(r"(.*), dsize", item["csize"]).group(1)) / 1024 / 1024)
        machines[machine][0].append(task_fetch)
        machines[machine][1].append((task["step_info"][3] - task["step_info"][0]) / 1000000)
        machines[machine][2].append((task["step_info"][5] - task["step_info"][4]) / 1000000)
        machines[machine][3].append((task["end_time"] - task["start_time"]) / 1000)
        x_val.append(task["end_time"] - task["start_time"])
        y_val.append(total_size)
    # ax = fig.add_subplot(line, col, idx + 1)
    # ax.scatter(x_val, y_val)
    # ax.set_title(f"{num},{vertex}")
    tmp_ = []
    for machine in machines:
        tmp_.append(
            (
                np.average(machines[machine][0]),
                np.average(machines[machine][1]),
                np.average(machines[machine][2]),
                np.average(machines[machine][3])
            )
        )
        # print(num, machine, [sum(t) for t in machines[machine]], [np.average(t) for t in machines[machine]])
    # print(tmp_)
    # print()
    return tmp_


def fetch_rate(num, tasks, vertex, s_machine, fig, line, col, idx):
    x_val = []
    machines = {}
    for task in tasks:
        if task["vex_name"] != vertex:
            continue
        fetches = task["fetches"]
        nums = 0
        bytes = 0
        for atp in fetches:
            item = fetches[atp]
            if "machine" in item:
                nums += 1
                bytes += float(item["rate"]) / 1024
        x_val.append(bytes)
        # for atp in fetches:
        #     item = fetches[atp]
        #     rate = float(item["rate"])
        #     machine = item["machine"]
        #     if machine != s_machine:
        #         continue
        #     if machine == task["machine"]:
        #         continue
        #     if machine not in machines:
        #         machines.update({
        #             machine: []
        #         })
        #     machines[machine].append(rate)
        #     x_val.append(rate)
    ax = fig.add_subplot(line, col, idx + 1)
    ax.hist(x_val)
    ax.set_title(f"{num},{s_machine},{vertex}")
    ax.tick_params(axis="x", labelrotation=45)
    print(num, sum(x_val))


def bytes_time_dis(x_val, s_y_val, p_y_val, t_y_val, fig):
    ax1 = fig.add_subplot(1, 3, 1)
    ax1.scatter(x_val, s_y_val)

    ax2 = fig.add_subplot(1, 3, 2)
    ax2.scatter(x_val, p_y_val)

    ax3 = fig.add_subplot(1, 3, 3)
    ax3.scatter(x_val, t_y_val)


def main():
    base_path = "./chi_9.2/log/reducer_{}/tasks.json"
    nums = [1009, 900, 800, 500, 450, 400, 350, 300, 250, 200, 150, 20]
    fig = plt.figure(figsize=(10, 5))

    x_val = []
    s_y_val = []
    p_y_val = []
    t_y_val = []

    for idx, num in enumerate(nums[:]):
        tasks = read_json_obj(base_path.format(num))
        # t_d = total_time(tasks)
        # t_r_d = total_red_time(tasks)
        # c_t = parallelism(tasks)
        # print(f"======================{num},{t_d / 1000}/{t_r_d / 1000}, {c_t}=======================")
        # reducer_total_ratio(tasks)
        # reducer_time(num, tasks)
        # draw_mr(num, tasks)
        # rd_vertex_hist(num, tasks)
        # rd_vertex_scatter(num, tasks)
        # draw_rd_vertex(num, tasks, "Reducer 2", fig, 4, 3, idx)
        # draw_container(num, tasks, "Reducer 3", fig, 4, 3, idx)
        # draw_container_ipo(num, tasks, "Reducer 14", fig, 4, 3, idx)
        # machine_dis(num, tasks, "Reducer 3", fig, 4, 3, idx)
        # draw_rd_vertex_hl(num, tasks, "Reducer 3", fig, 4, 3, idx)
        # fetch_machine(num, tasks, "Reducer 2", fig, 4, 3, idx)
        # shuffle_process_ol(num, tasks, "Reducer 14")
        tmp = fetch_bytes(num, tasks, "Reducer 3", fig, 4, 3, idx)
        for item in tmp:
            x_val.append(item[0])
            s_y_val.append(item[1])
            p_y_val.append(item[2])
            t_y_val.append(item[3])

    # fetch_rate(num, tasks, "Reducer 2", "dbg04", fig, 4, 3, idx)
    bytes_time_dis(x_val, s_y_val, p_y_val, t_y_val, fig)
    plt.tight_layout()
    # plt.savefig(f"chi_9.2/fig/reducer/reducer17.png")
    plt.show()


if __name__ == '__main__':
    main()
