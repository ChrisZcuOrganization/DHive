import matplotlib.pyplot as plt
import seaborn as sns
from utils.import_config import *


def draw_trace(tasks, fig):
    vertexes = []
    for task in tasks:
        vertex = task["vec_name"]
        if vertex not in vertexes:
            vertexes.append(vertex)
    print(len(vertexes))
    for idx, vertex in enumerate(vertexes):
        tmp = []
        for task in tasks:
            if task["vec_name"] != vertex:
                continue
            input_bytes = (0 if "HDFS_BYTES_READ" not in task["counter"] else task["counter"]["HDFS_BYTES_READ"])
            input_bytes += task["counter"]["FILE_BYTES_READ"]
            input_bytes += (0 if "SHUFFLE_BYTES" not in task["counter"] else task["counter"]["SHUFFLE_BYTES"])
            tmp.append(input_bytes)
        total_bytes = sum(tmp)
        if total_bytes == 0:
            continue
        ax = fig.add_subplot(math.ceil(len(vertexes) / 3), 3, idx + 1)
        for ls, item in enumerate(tmp):
            ax.plot([0, 2], [1 + ls * 0.2, 1 + ls * 0.2], color="red",
                    alpha=max(0.01, item / total_bytes))

        # ax.set_ylim([0, max(100, int(len(tmp) * 0.2))])
        ax.set_xticks([])
        ax.set_yticks([])
        ax.set_title(f"data_{idx}")


def draw_trace_input(tasks, fig):
    vertexes = []
    for task in tasks:
        vertex = task["vec_name"]
        if vertex not in vertexes:
            vertexes.append(vertex)
    sub_idx = 1
    for idx, vertex in enumerate(vertexes):
        tmp = []
        for task in tasks:
            if task["vec_name"] != vertex:
                continue
            input_time = task["step_info"][3] - task["step_info"][0]
            total_time = task["end_time"] - task["start_time"]
            tmp.append((input_time, total_time))
        if len(tmp) < 5:
            continue
        ax = fig.add_subplot(math.ceil(len(vertexes) / 3), 3, sub_idx)
        sub_idx += 1
        for ls, item in enumerate(tmp):
            break_points = 2 * (item[0] / item[1])
            ax.plot([0, break_points], [1 + ls * 0.2, 1 + ls * 0.2], color="red",
                    alpha=0.1)
            ax.plot([break_points, 2], [1 + ls * 0.2, 1 + ls * 0.2], color="blue",
                    alpha=0.1)
        # ax.set_ylim([0, max(100, int(len(tmp) * 0.2))])
        name = "tmp" if "Map" not in vertex else "raw"
        ax.set_xticks([])
        ax.set_yticks([])
        ax.set_title(f"data_{idx}_{name}")


def adjacent_matrix(tasks):
    machines = {

    }
    machine_set = []
    for task in tasks:
        if 'file_dir' not in task:
            continue
        if task["file_dir"] != "catalog_sales":
            continue
        src_machine = task["data_machine"]
        dst_machine = task["machine"]
        if src_machine not in machines:
            machines.update({src_machine: []})
        if src_machine not in machine_set:
            machine_set.append(src_machine)
        if dst_machine not in machine_set:
            machine_set.append(dst_machine)

        machines[src_machine].append(
            (dst_machine, task["read_bytes"],
             task["end_time"] - task["start_time"]))
    print("{:^7}".format(""), end=" ")

    for machine in machine_set:
        print("{:^7}".format(machine), end=" ")
    print()
    for machine_src in machine_set:
        if machine_src not in machines:
            continue
        print("{:^7}".format(machine_src), end=" ")
        tmp = {}
        for item in machines[machine_src]:
            machine_dst = item[0]
            if machine_dst not in tmp:
                tmp.update({
                    machine_dst: []
                })
            tmp[machine_dst].append(item[2])

        for machine_tmp in machine_set:
            print("{:^7}".format(0 if machine_tmp not in tmp else int(np.average(tmp[machine_tmp]))), end=" ")
        print()


def edge_sim(tasks, fig):
    y_val1 = [0]
    y_val2 = [0]
    items = []
    items_left = []
    start_time = sys.maxsize

    SPEED = False

    for task in tasks:
        if task["vec_name"] != "Map 12":
            continue
        start_time = min(start_time, task["step_info"][0])
        read_bytes = sum([0 if "READ" not in key or "BYTES" not in key else task["counter"][key]
                          for key in task["counter"]])

        if SPEED:
            items.append((task["step_info"][0], "start", "read",
                          read_bytes / (task["step_info"][3] - task["step_info"][0])))
            items.append((task["step_info"][3], "end", "read",
                          read_bytes / (task["step_info"][3] - task["step_info"][0])))
            items.append(
                (task["step_info"][4], "start", "process",
                 read_bytes / (task["step_info"][5] - task["step_info"][4])))
            items.append(
                (task["step_info"][5], "end", "process",
                 read_bytes / (task["step_info"][5] - task["step_info"][4])))
        else:
            items.append(
                (task["step_info"][0], "start", "read", read_bytes))
            # items.append((task["step_info"][3], "end", "read",
            #               read_bytes))
            items.append(
                (task["step_info"][4], "start", "process",
                 read_bytes))
            # items.append(
            #     (task["step_info"][5], "end", "process",
            #      read_bytes))

        # items_left.append(
        #     (task["step_info"][3], "cur_bytes_in", read_bytes)
        # )
        # items_left.append(
        #     (task["step_info"][5], "cur_bytes_pro", read_bytes)
        # )
    items.sort()
    items_left.sort()
    x_val1 = [start_time]
    x_val2 = [start_time]
    x_val3 = [start_time]
    y_val3 = [0]
    for item in items_left:
        x_val3.append(item[0])
        if "in" in item[1]:
            y_val3.append(y_val3[-1] + item[2])
        else:
            y_val3.append(y_val3[-1] - item[2])
    x_val = [start_time]
    y_val_1_1 = [0]
    y_val_2_2 = [0]
    y_val_1_2 = [0]
    for item in items:
        x_val.append(item[0])
        if item[2] == "read":
            x_val1.append(item[0])
        else:
            x_val2.append(item[0])

        if item[1] == "start":
            if item[2] == "read":
                y_val1.append(y_val1[-1] + item[3])
                y_val_1_1.append(y_val_1_1[-1] + item[3])
                y_val_2_2.append(y_val_2_2[-1])
                y_val_1_2.append(y_val2[-1])
            else:
                y_val2.append(y_val2[-1] + item[3])
                y_val_2_2.append(y_val_2_2[-1] + item[3])
                y_val_1_1.append(y_val_1_1[-1])
        else:
            if item[2] == "read":
                y_val1.append(y_val1[-1] - item[3])
                y_val_1_1.append(y_val_1_1[-1] - item[3])
                y_val_2_2.append(y_val_2_2[-1])
            else:
                y_val2.append(y_val2[-1] - item[3])
                y_val_2_2.append(y_val_2_2[-1] - item[3])
                y_val_1_1.append(y_val_1_1[-1])

    x_val1.append(x_val2[-1])
    y_val1.append(y_val2[-1])

    print([((t - start_time) / 1000) for t in x_val1])
    print([(b / 1000000) for b in y_val1])

    print([((t - start_time) / 1000) for t in x_val2])
    print([(b / 1000000) for b in y_val2])

    # plt.plot([((t - start_time) / 1000) for t in x_val1], [(b / 1000000) for b in y_val1])
    # plt.plot([((t - start_time) / 1000) for t in x_val2], [(-b / 1000000) for b in y_val2])
    # plt.plot([((t - start_time) / 1000) for t in x_val1], [((b - y_val_1_2[idx]) / 1000000) for idx, b in enumerate(y_val1)])

    # plt.plot([((t - start_time) / 1000) for t in x_val3], [(b / 1000000) for b in y_val3])
    plt.stackplot(x_val, y_val_2_2, y_val_1_1, [y_val_1_1[idx] - y_val_2_2[idx] for idx in range(len(x_val))])

    # plt.ylim([0, 16000])


def tasks_encoding(tasks, vertex):
    input_items = []
    processor_items = []
    start_time = sys.maxsize

    for task in tasks:
        if task["vex_name"] != vertex:
            continue
        start_time = min(start_time, task["start_time"])
        for idx, _ in enumerate(task["input_info"]):
            if task["input_info"][idx]["speed"] * task["processor_info"][idx]["speed"] == 0:
                continue
            input_items.append((task["input_info"][idx]["time"],
                                "begin", 1 / task["input_info"][idx]["speed"]))
            input_items.append((task["input_info"][idx]["time"] + task["input_info"][idx]["speed"],
                                "end", 1 / task["input_info"][idx]["speed"]))
            processor_items.append((task["processor_info"][idx]["time"],
                                    "begin", 1 / task["processor_info"][idx]["speed"]))
            processor_items.append((task["processor_info"][idx]["time"] + task["processor_info"][idx]["speed"],
                                    "end", 1 / task["processor_info"][idx]["speed"]))
    input_items.sort()
    processor_items.sort()

    input_time = [start_time]
    input_val = [0]

    processor_time = [start_time]
    processor_val = [0]

    for idx, _ in enumerate(input_items):
        input_time.append(input_items[idx][0])
        if input_items[idx][1] == "begin":
            input_val.append(input_val[-1] + input_items[idx][2])
        else:
            input_val.append(input_val[-1] - input_items[idx][2])

        processor_time.append(processor_items[idx][2])
        if processor_items[idx][1] == "begin":
            processor_val.append(processor_val[-1] + processor_items[idx][2])
        else:
            processor_val.append(processor_val[-1] - processor_items[idx][2])

    plt.fill_between(input_time, input_val, y2=0)
    # print(processor_val[-1])
    # plt.plot(processor_time[:100], [-x for x in processor_val[:100]])
    # plt.xlim([input_time[0], input_time[10]])


def tasks_flow(tasks, vertex):
    contain_set = {}
    cur_baseline = 1
    max_val = -1
    start_time = sys.maxsize
    end_time = -1
    for task in tasks:
        input_val = []
        process_val = []
        if task["vex_name"] != vertex:
            continue
        # if "Map" not in task["vex_name"]:
        #     continue
        contain = task["container"]
        if contain not in contain_set:
            contain_set[contain] = cur_baseline
            cur_baseline += 2
        start_time = min(start_time, task["start_time"])
        end_time = max(end_time, task["end_time"])
        input_val.append(0)
        process_val.append(0)
        for idx, _ in enumerate(task["input_info"]):
            if task["input_info"][idx]["speed"] * task["processor_info"][idx]["speed"] == 0:
                continue
            input_val.append(1 / max(0, task["input_info"][idx]["speed"]))
            process_val.append(1 / max(0, task["processor_info"][idx]["speed"]))
        max_val = max(max_val, max(max(process_val, input_val)))
    cur_baseline = 1

    tasks_flows = []

    for task in tasks[:]:
        input_time = []
        input_val = []
        process_time = []
        process_val = []

        if task["vex_name"] != vertex:
            continue
        # if "Map" not in task["vex_name"]:
        #     continue

        if task["machine"] != task["data_machine"]:
            continue
        # input_time.append(start_time)
        # process_time.append(start_time)
        # input_val.append(0.5)
        # process_val.append(0.5)
        for idx, _ in enumerate(task["input_info"]):
            if task["input_info"][idx]["speed"] * task["processor_info"][idx]["speed"] == 0:
                continue
            # input_time.append(task["input_info"][idx]["time"] - task["input_info"][idx]["speed"])
            # input_val.append(0)
            # input_time.append(task["input_info"][idx]["time"] - task["input_info"][idx]["speed"])
            # input_val.append(1 / max(0, task["input_info"][idx]["speed"]))
            input_val.append(1 / max(0, task["input_info"][idx]["speed"]))
            # if len(input_time) > 0:
            #     input_val.append((input_val[-1] + 1 / max(0, task["input_info"][idx]["speed"])) / 2)
            # else:
            #     input_val.append(1 / max(0, task["input_info"][idx]["speed"]))
            input_time.append(task["input_info"][idx]["time"])
            # input_time.append(task["input_info"][idx]["time"])
            # input_val.append(0)

            # process_time.append(task["processor_info"][idx]["time"] - task["processor_info"][idx]["speed"])
            # process_val.append(0)
            # process_time.append(task["processor_info"][idx]["time"] - task["processor_info"][idx]["speed"])
            # process_val.append(1 / max(0, task["processor_info"][idx]["speed"]))
            process_val.append(1 / max(0, task["processor_info"][idx]["speed"]))
            # if len(process_val) > 0:
            #     process_val.append((process_val[-1] + 1 / max(0, task["processor_info"][idx]["speed"])) / 2)
            # else:
            #     process_val.append(1 / max(0, task["processor_info"][idx]["speed"]))
            process_time.append(task["processor_info"][idx]["time"])
            # process_time.append(task["processor_info"][idx]["time"])
            # process_val.append(0)
        # input_time.append(end_time)
        # input_val.append(input_val[-1])
        # process_time.append(end_time)
        # process_val.append(process_val[-1])
        cur_baseline = contain_set[task["container"]]
        plt.fill_between(input_time, [x + cur_baseline for x in input_val], y2=cur_baseline)
        plt.fill_between(process_time, [-x + cur_baseline for x in process_val],
                         y2=cur_baseline)
        # plt.plot([start_time, end_time], [cur_baseline, cur_baseline])
        # cur_baseline += 2
        # print(process_time)
        tasks_flows.append({"process_time": process_time,
                            "process_val": process_val,
                            "input_val": input_val,
                            "input_time": input_time})
        # return
    plt.xlim([start_time, end_time])
    return tasks_flows


def get_max_input_bytes(tasks, vertex):
    max_input = 0
    for task in tasks:
        if task["vex_name"] != vertex:
            continue
        if len(task["input_bytes_info"]) < 2:
            continue
        max_input = max(max_input, int(task["input_bytes_info"][-1]["input_bytes"]) + (
                int(task["input_bytes_info"][-1]["input_bytes"]) - int(
            task["input_bytes_info"][-2]["input_bytes"])))
    return max_input


def tasks_bytes_flow(tasks, vertex, max_input):
    cur_base = 0
    start_time = sys.maxsize
    end_time = 0
    for task in tasks:
        if task["vex_name"] != vertex:
            continue
        start_time = min(start_time, task["step_info"][4])
        end_time = max(end_time, task["step_info"][5])

    for task in tasks[:]:
        if task["vex_name"] != vertex:
            continue
        # if cur_base > 40:
        #     return
        # if task["machine"] == task["data_machine"]:
        #     continue
        # if "Map" not in task["vex_name"]:
        #     continue
        if len(task["input_bytes_info"]) < 2:
            continue
        # print(task["input_bytes_info"])
        input_bytes = int(task["input_bytes_info"][-1]["input_bytes"]) + (
                int(task["input_bytes_info"][-1]["input_bytes"]) - int(task["input_bytes_info"][-2]["input_bytes"]))
        # left_bytes = [input_bytes]
        # left_times = [task["start_time"]]

        # processed_bytes = []
        # processed_time = []
        items = [(task["step_info"][4], 0, "input_left", input_bytes)]
        last_bytes = input_bytes
        for i_b in task["input_bytes_info"]:
            items.append((int(i_b["time"]), 0, "input_left", last_bytes - int(i_b["input_bytes"])))
            # last_bytes = int(i_b["input_bytes"])
            # print(items[-2][3] - items[-1][3])
        for p_b in task["processed_bytes_info"]:
            # print(int(p_b["total_bytes"]))
            items.append((int(p_b["time"]), 1, "processed_bytes", int(p_b["total_bytes"])))
            # print(items[-1][3] - items[-2][3])

        items.sort()
        last_bytes = items[0][3]
        # x_val = [start_time]
        x_val = [task["step_info"][4]]
        y_val_processed = [0]
        y_val_total = [input_bytes / max_input]
        y_left = [input_bytes / max_input]
        tmp_bytes = input_bytes
        input_bytes = max_input

        for item in items:
            if item[2] == "input_left":
                last_bytes = item[3]
                y_val_processed.append(y_val_processed[-1])
                y_val_total.append(y_val_processed[-1] + last_bytes / input_bytes)
                y_left.append(last_bytes / input_bytes)
                x_val.append(item[0])
            else:
                x_val.append(item[0])
                y_val_processed.append(item[3] / input_bytes)
                y_val_total.append((float(item[3]) + last_bytes) / input_bytes)
                y_left.append(last_bytes / input_bytes)
        x_val.append(end_time)
        y_val_processed.append(y_val_processed[-1])
        y_val_total.append(y_val_processed[-1])
        plt.fill_between([start_time, x_val[0]], [y_val_total[0] + cur_base, y_val_total[0] + cur_base], y2=cur_base,
                         color="green", linewidth=0)
        plt.fill_between(x_val, [i + cur_base for i in y_val_processed], y2=cur_base, color="blue", linewidth=0)
        plt.fill_between(x_val, [i + cur_base for i in y_val_total], y2=[i + cur_base for i in y_val_processed],
                         color="red", linewidth=0)
        cur_base += (tmp_bytes / max_input * 1.2)


def sum_left_bytes(ip_map):
    res = 0
    for key in ip_map:
        res += ip_map[key]["left_bytes"]
    return res


def sum_processed_bytes(ip_map):
    res = 0
    for key in ip_map:
        res += ip_map[key]["processed_bytes"]
    return res


def task_stack(tasks, vertex, if_save=False, save_path=""):
    start_time = sys.maxsize
    end_time = -1
    y_scale = 0
    for task in tasks[:]:
        if task["vex_name"] != vertex:
            continue
        y_scale += int(task["input_bytes_info"][-1]["input_bytes"])
        start_time = min(start_time, task["start_time"])
        end_time = max(end_time, task["end_time"])

    time_series = [start_time + i * (end_time - start_time) / 5000 for i in range(5000)]
    stack_lists = []
    stack_colors = []
    for task in tasks[:]:
        if task["vex_name"] != vertex:
            continue
        if len(task["input_bytes_info"]) < 2:
            continue
        # input_bytes = int(task["input_bytes_info"][-1]["input_bytes"]) + (
        #         int(task["input_bytes_info"][-1]["input_bytes"]) - int(task["input_bytes_info"][-2]["input_bytes"]))
        input_bytes = int(task["input_bytes_info"][-1]["input_bytes"])
        left_bytes = [(task["step_info"][4], 0, input_bytes)]
        processed_bytes = [(task["step_info"][4], 0, 0)]
        last_bytes = input_bytes
        for i_b in task["input_bytes_info"]:
            left_bytes.append((int(i_b["time"]), 0, last_bytes - int(i_b["input_bytes"])))
        for p_b in task["processed_bytes_info"]:
            processed_bytes.append((int(p_b["time"]), 0, int(p_b["total_bytes"])))
        stack_before_pro_bytes = []
        for time in time_series:
            left_bytes.append((time, 1, -1))
            processed_bytes.append((time, 1, -1))
            if time < task["step_info"][4]:
                stack_before_pro_bytes.append(input_bytes)
            else:
                stack_before_pro_bytes.append(0)
        left_bytes.sort()
        processed_bytes.sort()
        stack_left_ary = []
        stack_pro_ary = []
        last_bytes = input_bytes
        for item in left_bytes:
            if item[2] == -1:
                if item[0] < task["step_info"][4]:
                    # if item[0] < task["input_bytes_info"][0]["time"]:
                    stack_left_ary.append(0)
                else:
                    stack_left_ary.append(last_bytes)
            else:
                last_bytes = item[2]
        last_bytes = 0
        for item in processed_bytes:
            if item[2] == -1:
                stack_pro_ary.append(last_bytes)
            else:
                last_bytes = item[2]
        # y_scale = 1
        max_val = 40
        stack_lists.append(copy.deepcopy([(item / y_scale) * max_val for item in stack_pro_ary]))
        stack_lists.append(copy.deepcopy([(item / y_scale) * max_val for item in stack_left_ary]))
        stack_lists.append(copy.deepcopy([(item / y_scale) * max_val for item in stack_before_pro_bytes]))
        stack_colors.append("blue")
        stack_colors.append("green")
        # location = task["machine"] != task["data_machine"]
        # stack_colors.append("grey" if location else "green")
        stack_colors.append("red")
    plt.figure()
    plt.stackplot(time_series, np.array(stack_lists), colors=stack_colors, linewidth=0)
    plt.title(vertex + f", {len(stack_colors) / 3}")
    # plt.xlim([1658292648539.0, 1658292724350.0])
    if if_save:
        plt.savefig(save_path)
    else:
        pass
    # plt.show()


def vertex_data(tasks, vertex, base_line, global_max_bytes):
    items = []
    initial_bytes = 0
    start_time = sys.maxsize
    ip_tasks_map = {}

    output_bytes = 0
    for task in tasks:
        if task["vex_name"] != vertex:
            continue
        if len(task["input_bytes_info"]) < 2:
            continue
        last_bytes = int(task["input_bytes_info"][-1]["input_bytes"]) + (
                int(task["input_bytes_info"][-1]["input_bytes"]) - int(task["input_bytes_info"][-2]["input_bytes"]))
        # print(task["counter"])
        if "OUTPUT_BYTES" in task["counter"]:
            output_bytes += task["counter"]["OUTPUT_BYTES"]
        if task["task_id"] not in ip_tasks_map:
            ip_tasks_map[task["task_id"]] = {"left_bytes": last_bytes, "processed_bytes": 0}
        initial_bytes += last_bytes
        start_time = min(start_time, task["step_info"][4])
        for i_b in task["input_bytes_info"]:
            items.append((int(i_b["time"]), 0, "input_left", last_bytes - int(i_b["input_bytes"]),
                          task["task_id"]))
        for p_b in task["processed_bytes_info"]:
            items.append((int(p_b["time"]), 1, "processed_bytes", int(p_b["total_bytes"]),
                          task["task_id"]))
    x_val = [start_time]
    y_val_total = [initial_bytes]
    y_val_processed = [0]
    y_left = [initial_bytes]
    items.sort()
    last_bytes = initial_bytes

    for item in items:
        if item[2] == "input_left":
            ip_tasks_map[item[4]]["left_bytes"] = item[3]
            y_val_total.append(y_val_processed[-1] + sum_left_bytes(ip_tasks_map))
            y_left.append(sum_left_bytes(ip_tasks_map))
            y_val_processed.append(y_val_processed[-1])
            x_val.append(item[0])
        else:
            ip_tasks_map[item[4]]["processed_bytes"] = item[3]
            y_val_total.append(sum_processed_bytes(ip_tasks_map) + sum_left_bytes(ip_tasks_map))
            y_left.append(sum_left_bytes(ip_tasks_map))
            y_val_processed.append(sum_processed_bytes(ip_tasks_map))
            x_val.append(item[0])
    tmp = 1
    if global_max_bytes != -1:
        tmp = global_max_bytes
        if initial_bytes / global_max_bytes < 0.01:
            tmp = 10 * initial_bytes
    plt.fill_between(x_val, [i / tmp + base_line for i in y_val_processed], y2=base_line, color="red")
    plt.fill_between(x_val, [i / tmp + base_line for i in y_val_total],
                     y2=[i / tmp + base_line for i in y_val_processed],
                     color="blue")
    print(vertex, initial_bytes, y_val_processed[-1], output_bytes, initial_bytes / tmp)
    return initial_bytes
    # plt.stackplot(x_val, [y_val_processed, y_left], baseline="sym")


def reducer_proc_time(tasks, vertex):
    start_time = sys.maxsize
    for task in tasks:
        if task["vex_name"] != vertex:
            continue
        start_time = min(start_time, task["step_info"][4])
    plt.plot([start_time, start_time], [0, 3], c="black", linewidth=5)


def reducer_fetch_time(tasks, vertex):
    start_fetch = sys.maxsize
    end_time = -1
    for task in tasks:
        if task["vex_name"] != vertex:
            continue
        for item in task["fetches"]:
            start_tmp = task["fetches"][item]["end_time"] - task["fetches"][item]["time_taken"]
            if start_fetch > start_tmp:
                start_fetch = start_tmp
                end_time = task["fetches"][item]["end_time"]
    plt.plot([start_fetch, start_fetch], [0, 2], linewidth=10, c="black")
    plt.plot([end_time, end_time], [0, 2], linewidth=10, c="green")


def data_skew(tasks, vertex):
    y_val = []
    x_val = []
    for task in tasks:
        if task["vex_name"] != vertex:
            continue
        x_val.append(task["start_time"])
        y_val.append(task["counter"]["SHUFFLE_BYTES"])
    plt.scatter(x_val, y_val)


def fetch_flow(tasks, vertex, task_set):
    items = []
    start_time = sys.maxsize
    for task in tasks:
        if task["vex_name"] != vertex:
            continue
        tmp = 0
        for task_id in task["fetches"]:
            if task_id[:-6] not in task_set:
                continue
            fetch = task["fetches"][task_id]
            items.append((fetch["end_time"], 0, fetch["csize"]))
            start_time = min(start_time, fetch["end_time"] - fetch["time_taken"])
            tmp += fetch["csize"]
        items.append((task["step_info"][4], 1, tmp))
    items.sort()
    x_val = [start_time]
    y_val = [0]
    for item in items:
        x_val.append(item[0])
        if item[1] == 0:
            y_val.append(item[2] + y_val[-1])
        else:
            y_val.append(y_val[-1] - item[2])

    plt.fill_between(x_val, [-i for i in y_val], y2=0, color="green")


def get_task_set(tasks, vertex):
    task_set = set()
    for task in tasks:
        if task["vex_name"] != vertex:
            continue
        task_set.add(task["task_id"])
    return task_set


def order_tasks(tasks, vertexes):
    # machine order here
    items = []
    random.shuffle(tasks)
    for idx, task in enumerate(tasks):
        if task["vex_name"] not in vertexes:
            continue
        items.append((task["machine"], idx, task))

    items.sort()
    return [item[-1] for item in items]


def get_vertex(tasks):
    vertex = set()
    for task in tasks:
        vex = task["vex_name"]
        if vex not in vertex:
            vertex.add(vex)
    return vertex


def save_query_vertex(tasks, path):
    vertexes = get_vertex(tasks)
    os.makedirs(path, exist_ok=True)
    for vertex in vertexes:
        print(vertex)
        tasks_tmp = order_tasks(tasks, vertex)
        task_stack(tasks_tmp, vertex, True, f"{path}/{vertex}")


def overall_flow(tasks):
    before_bytes, left_bytes, processed_bytes, start_time, end_time = generate_overall_stack(tasks)
    time_length = len(before_bytes)
    time_series = [start_time + i * (end_time - start_time) / (time_length - 1) for i in range(time_length)]
    plt.stackplot(time_series, np.array([before_bytes, left_bytes, processed_bytes]), colors=["red", "green", "blue"],
                  linewidth=0)


def parallelism(tasks, name):
    cur_cnt = 0
    count = 0
    time_tups = []
    x_val = []
    y_val = []
    for task in tasks:
        time_tups.append((task["start_time"], "start_time"))
        time_tups.append((task["end_time"], "end_time"))

    time_tups.sort()
    for t_t in time_tups:
        x_val.append(t_t[0])
        if t_t[1] == "start_time":
            cur_cnt += 1
            count = max(count, cur_cnt)
        else:
            cur_cnt -= 1
        y_val.append(cur_cnt)
    plt.plot(x_val, y_val, c="grey")
    plt.ylim([0, 70])
    # plt.xlim([1658292648539.0, 1658292724350.0])
    plt.title(name)
    return count


def draw_container(tasks, fig, line, col, idx):
    containers = {}
    min_start_time = sys.maxsize
    max_end_time = 0
    c_t = []
    for task in tasks:
        container = task["container"]
        if container not in containers:
            containers.update({
                container: []
            })
            c_t.append((task["machine"], container))
        min_start_time = min(min_start_time, task["start_time"])
        max_end_time = max(max_end_time, task["end_time"])
        # containers[container].append((task["input_bytes_info"][0]["time"], task["end_time"], task["vex_name"]))
        containers[container].append((task["start_time"], task["end_time"], task["vex_name"]))
        # containers[container].append((task["step_info"][4], task["end_time"], task["vex_name"]))
    c_t.sort()
    ax = fig.add_subplot(line, col, idx)
    colors = {
        "Map 7": "red",
        "Map 3": "blue",
        "Map 10": "blue",
        "Map 1": "green"
    }
    # for id_c, container in enumerate(containers.keys()):
    for id_c, container in enumerate(c_t):
        tasks_t = containers[container[1]]
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
                facecolor=colors[tup[2]],
                # fill=colors[id % 3]
            ))
    # ax.set_xlim([0, 25000])
    ax.plot([0, 0], [0, 1])


def draw_scatter_plot(tasks, vertex):
    x_val = []
    y_val = []
    for task in tasks:
        x_val.append(task["start_time"])
        y_val.append(task["end_time"])
    plt.scatter(x_val, y_val, c="red", alpha=0.2)
    # plt.xlim([1658292648539.0, 1658292724350.0])
    plt.title(vertex)


def cal_machine(tasks):
    m_t = []
    for task in tasks:
        m_t.append((task["end_time"] - task["start_time"], task["machine"]))
    m_t.sort(reverse=True)
    for i in range(0, 6):
        print(m_t[i])


def machine_dis(tasks):
    y_val = []
    for task in tasks:
        if task["machine"] == "dbg11":
            y_val.append(task["end_time"] - task["start_time"])
    plt.hist(y_val)


def draw_time_distribution(tasks, vertex):
    x_val = []
    y_val = []
    for task in tasks:
        x_val.append(task["step_info"][0])
        y_val.append(task["end_time"])
    plt.scatter(x_val, [60 for _ in range(len(x_val))], c="red", alpha=0.2)
    plt.scatter(y_val, [65 for _ in range(len(x_val))], c="blue", alpha=0.2)
    for idx in range(len(x_val)):
        plt.plot([x_val[idx], y_val[idx]], [60, 65], c='blue', linewidth='0.2')
    # plt.xlim([1658292648539.0, 1658292724350.0])


def data_layout(tasks):
    machine_map = {}
    for task in tasks:
        data_machine = task["data_machine"]
        # data_machine = task["machine"]
        if data_machine not in machine_map:
            machine_map[data_machine] = 0
        machine_map[data_machine] += int(task["input_bytes_info"][-1]["input_bytes"])
        # machine_map[data_machine] += task["counter"]["OUTPUT_BYTES"]
        # machine_map[data_machine] += 1
    m = list(machine_map.keys())
    m = sorted(m)

    print(m)
    print(machine_map)

    plt.bar(m, [machine_map[k] for k in m])
    total = sum([machine_map[k] for k in m])
    # plt.imshow([[machine_map[k] for k in m]], vmin=total / (len(m) * 2), vmax=total / (len(m)) * 1.2, cmap="Reds")
    plt.figure(figsize=(10, 2))
    sns.heatmap([[machine_map[k] for k in m]], vmin=total / (len(m) * 2), vmax=total / (len(m)) * 1.3, xticklabels=m,
                cmap="YlOrBr")


def main():
    # case3 = "../data4/Example39/output/FullTask.json"  # data skew
    # case3 = "../data4/Query54/output/FullTask.json"
    # case3 = "../data_chi_9.2/log/reducer_150/tasks.json"  # task number
    # case3 = "../data_chi_9.2/log/tasks_machine.json"  # data locality
    case3 = "../data/log/query1/tasks.json"
    # case3 = "../data/log/query62/resource_comp/tasks.json"

    tasks = read_json_obj(case3)
    # data_skew(tasks, "Reducer 3")
    vertex = "Map 7"
    # tasks_tmp = order_tasks(tasks, [vertex, "Map 1", "Map 10"])
    tasks_tmp = order_tasks(tasks, [vertex])
    tasks_encoding(tasks_tmp, vertex)
    draw_container(tasks_tmp, plt.figure(), 1, 1, 1)
    # cal_machine(tasks_tmp)
    # machine_dis(tasks)
    # draw_scatter_plot(tasks_tmp, "Map 1")
    # task_stack(tasks_tmp, vertex)
    data_layout(tasks_tmp)
    # parallelism(tasks_tmp, "query62" + vertex)
    # draw_time_distribution(tasks_tmp, vertex)
    # overall_flow(tasks)
    plt.tight_layout()
    plt.show()


if __name__ == '__main__':
    main()
