import copy
import json
import math
import random
import sys


def order_tasks(tasks):
    # machine order here
    items = []
    random.shuffle(tasks)
    return tasks


def read_json_obj(filename: str):
    with open(filename, 'r', encoding='utf-8') as fp:
        json_obj = json.load(fp)
    return json_obj


def write_to_file(tasks_list, out_path):
    task_str = json.dumps(tasks_list)

    with open(out_path, "w") as f:
        f.writelines(task_str)


def get_start_end_time(tasks):
    start_time = sys.maxsize
    end_time = -1
    for task in tasks:
        start_time = min(start_time, task["start_time"])
        end_time = max(end_time, task["end_time"])
    return start_time, end_time


def get_vertex(tasks):
    vertex = set()
    for task in tasks:
        vex = task["vex_name"]
        if vex not in vertex:
            vertex.add(vex)
    return vertex


def generate_overall_stack(tasks):
    start_time = sys.maxsize
    end_time = -1

    for task in tasks:
        start_time = min(start_time, int(task["start_time"]))
        end_time = max(end_time, int(task["end_time"]))

    time_series = [start_time + i * (end_time - start_time) / 500 for i in range(501)]
    overall_before_flow = [0 for _ in range(501)]
    overall_left_flow = [0 for _ in range(501)]
    overall_processed_flow = [0 for _ in range(501)]

    vertexes = get_vertex(tasks)
    for vertex in vertexes:
        tasks_flow, vertex_start, vertex_end, _, _, _, _, _, _ = generate_task_stack(tasks, vertex)
        vertex_before_flow, vertex_left_flow, vertex_processed_flow = get_vertex_flow(tasks_flow)
        cal_len = len(vertex_before_flow)
        tmp_time_series = [vertex_start + i * (vertex_end - vertex_start) / (cal_len - 1) for i in range(cal_len)]
        tmp_before_flows = []
        tmp_left_flow = []
        tmp_processed_flow = []
        for idx in range(len(vertex_before_flow)):
            tmp_before_flows.append((tmp_time_series[idx], 0, vertex_before_flow[idx], -1))
            tmp_left_flow.append((tmp_time_series[idx], 0, vertex_left_flow[idx], -1))
            tmp_processed_flow.append((tmp_time_series[idx], 0, vertex_processed_flow[idx], -1))
        for idx in range(len(time_series)):
            tmp_before_flows.append((time_series[idx], 1, 0, idx))
            tmp_left_flow.append((time_series[idx], 1, 0, idx))
            tmp_processed_flow.append((time_series[idx], 1, 0, idx))
        tmp_before_flows.sort()
        tmp_left_flow.sort()
        tmp_processed_flow.sort()
        cur_before = 0
        cur_left = 0
        cur_processed = 0
        for idx in range(len(tmp_left_flow)):
            tmp_time = tmp_left_flow[idx][0]
            if tmp_time > vertex_start:
                if tmp_time > vertex_end:
                    break
                item = tmp_left_flow[idx]
                flag = item[1]
                if flag == 1:
                    overall_before_flow[item[3]] += cur_before
                    overall_left_flow[item[3]] += cur_left
                    overall_processed_flow[item[3]] += cur_processed
                else:
                    cur_before = tmp_before_flows[idx][2]
                    cur_left = tmp_left_flow[idx][2]
                    cur_processed = tmp_processed_flow[idx][2]
    return overall_before_flow, overall_left_flow, overall_processed_flow, start_time, end_time


def get_vertex_flow(tasks_flow) -> [int]:
    vertex_before_flow = [0 for _ in range(len(tasks_flow[0][0]))]
    vertex_left_flow = [0 for _ in range(len(tasks_flow[0][0]))]
    vertex_processed_flow = [0 for _ in range(len(tasks_flow[0][0]))]

    for task_flow in tasks_flow:
        for idx in range(len(task_flow[0])):
            vertex_before_flow[idx] += task_flow[0][idx]
            vertex_left_flow[idx] += task_flow[1][idx]
            vertex_processed_flow[idx] += task_flow[2][idx]
    return vertex_before_flow, vertex_left_flow, vertex_processed_flow


def log_rate_transform(tasks_flow):
    for idx, _ in enumerate(tasks_flow[0][0]):
        cur_total_bytes_before = 1
        cur_total_bytes_left = 1
        cur_total_bytes_pro = 1

        cur_total_bytes = 1
        for flow in tasks_flow:
            cur_total_bytes_before += flow[0][idx]
            cur_total_bytes_left += flow[1][idx]
            cur_total_bytes_pro += flow[2][idx]

        cur_total_bytes += (cur_total_bytes_before + cur_total_bytes_left + cur_total_bytes_pro)

        for flow in tasks_flow:
            flow[0][idx] = flow[0][idx] / cur_total_bytes * math.log2(cur_total_bytes)
            flow[1][idx] = flow[1][idx] / cur_total_bytes * math.log2(cur_total_bytes)
            flow[2][idx] = flow[2][idx] / cur_total_bytes * math.log2(cur_total_bytes)


def get_task_machine_map(tasks):
    task_map = {}
    for task in tasks:
        task_map[task["task_id"]] = task["machine"]
    return task_map


def generate_data_dis(tasks, vertex, task_machine_map):
    src_map = {}
    cal_data_map = {}
    for task in tasks:
        if task["vex_name"] != vertex:
            continue

        machine = task["machine"]
        if machine not in cal_data_map:
            cal_data_map.update({machine: 0})
        cal_data_map[machine] += int(task["input_bytes_info"][-1]["input_bytes"])

        if "data_machine" in task and task["data_machine"] is not None:
            data_machine = task["data_machine"]
            if int(task["input_bytes_info"][-1]["input_bytes"]) == 0:
                continue
            if data_machine not in src_map:
                src_map.update({data_machine: {"total": 0}})
            src_map[data_machine]["total"] += int(task["input_bytes_info"][-1]["input_bytes"])
            if machine not in src_map[data_machine]:
                src_map[data_machine][machine] = 0
            src_map[data_machine][machine] += int(task["input_bytes_info"][-1]["input_bytes"])

        if len(task["fetches"]) > 0:
            for attempt in task["fetches"]:
                # if task["vex_name"] == "Reducer 2":
                # print(task["fetches"][attempt]["end_time"] - task["step_info"][4],
                #       task["step_info"][4] - task["start_time"])
                data_machine = task_machine_map[attempt[:-6]]
                if "dsize" not in task["fetches"][attempt] or task["fetches"][attempt]["dsize"] == 0:
                    continue
                if data_machine not in src_map:
                    src_map.update({data_machine: {"total": 0}})
                if machine not in src_map[data_machine]:
                    src_map[data_machine][machine] = 0
                if "dsize" in task["fetches"][attempt]:
                    src_map[data_machine]["total"] += task["fetches"][attempt]["dsize"]
                    src_map[data_machine][machine] += task["fetches"][attempt]["dsize"]
    return src_map, cal_data_map


def get_tasks_list(tasks, vertex):
    tasks_list = []
    for task in tasks:
        if task["vex_name"] != vertex:
            continue
        tmp = {}
        for c in task["counter"]:
            if "RECORDS_OUT_OPERATOR" in c:
                continue
            tmp[c] = task["counter"][c]
        tasks_list.append({
            # "task_id": 0,
            "task_id": task["task_id"],
            "task_start_time": task["start_time"],
            "task_end_time": task["end_time"],
            "counter": tmp
        })
    return tasks_list


def generate_task_stack(tasks, vertex):
    start_time = sys.maxsize
    total_start_time = sys.maxsize
    input_end_time = -1
    process_start_time = sys.maxsize
    process_end_time = -1
    out_start_time = sys.maxsize
    out_end_time = -1

    total_end_time = -1
    end_time = -1
    init_bytes = 0
    for task in tasks[:]:
        if task["vex_name"] != vertex:
            continue
        if "Reducer" in vertex:
            start_time = min(start_time, task["step_info"][4])
        else:
            start_time = min(start_time, task["start_time"])
        process_start_time = min(process_start_time, task["step_info"][4])
        total_start_time = min(total_start_time, task["start_time"])
        out_start_time = min(out_start_time, task["step_info"][5])
        init_bytes += int(task["input_bytes_info"][-1]["input_bytes"])
        end_time = max(end_time, task["step_info"][5])
        input_end_time = max(input_end_time, task["step_info"][3])
        process_end_time = max(process_end_time, task["step_info"][5])
        total_end_time = max(total_end_time, task["end_time"])

    time_series = [start_time + i * (end_time - start_time) / 100 for i in range(101)]
    tasks_flow_list = []
    for task in tasks[:]:
        if task["vex_name"] != vertex:
            continue
        input_bytes = int(task["input_bytes_info"][-1]["input_bytes"])
        left_bytes = [(task["step_info"][0], 0, input_bytes)]
        processed_bytes = [(task["step_info"][0], 0, 0)]
        last_bytes = input_bytes

        if "OUTPUT_BYTES" in task["counter"]:  # todo
            task["processed_bytes_info"][-1]["total_bytes"] = max(task["processed_bytes_info"][-1]["total_bytes"],
                                                                  task["counter"]["OUTPUT_BYTES"])

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
        # scale = math.pow(2, math.log(init_bytes, 5))
        scale = 1
        tasks_flow_list.append(
            (copy.deepcopy([item for item in stack_before_pro_bytes]),
             copy.deepcopy([item for item in stack_left_ary]),
             copy.deepcopy([item for item in stack_pro_ary])
             ))
    return tasks_flow_list, start_time, end_time, total_start_time, total_end_time, process_start_time, process_end_time, input_end_time, out_start_time


def load_connection(file_path):
    con = {}
    with open(file_path, "r") as fp:
        items = fp.readlines()
    for item in items:
        if "Stage-" in item:
            break
        else:
            if "Reducer" in item or "Map" in item:
                tmp = item.split("<-")
                to = tmp[0].strip()
                frms = tmp[1].split(",")
                for frm in frms:
                    frm_tmp = frm.split("(")[0].strip()
                    if frm_tmp not in con:
                        con.update({
                            frm_tmp: []
                        })
                    con[frm_tmp].append(to)
    return con


def reducer_input_script(tasks_path):
    tasks = read_json_obj(tasks_path)

    for task in tasks:
        if "Reduc" in task["vex_name"]:
            tmp = task["processed_bytes_info"]
            task["processed_bytes_info"] = task["input_bytes_info"]
            task["input_bytes_info"] = task["processed_bytes_info"][:1]
    write_to_file(tasks, "../../data/log/query29/tasks.json")


def get_vertex_ops(dag_path):
    dag_info = read_json_obj(dag_path)
    vex_ops = {}
    for vex in dag_info:
        tmp = []
        steps = dag_info[vex]
        for step in steps:
            tmp.append(step['attr_dict']['@Label'].split('_')[0])
        vex_ops[vex] = tmp
    return vex_ops


if __name__ == '__main__':
    dag_path = "../../data/log/query29/output/DagUpdate.json"
    get_vertex_ops(dag_path)
