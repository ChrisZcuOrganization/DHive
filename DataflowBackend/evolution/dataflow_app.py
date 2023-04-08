import matplotlib.pyplot as plt
import numpy as np
from flask_cors import CORS
from flask import Flask, request, jsonify
from flask import render_template, Response, json, make_response
import os
from utils.util import *

FILE_ABS_PATH = os.path.dirname(__file__)
SERVER_PATH = os.path.join(FILE_ABS_PATH, '../')

DATA_DIR = os.path.join(SERVER_PATH, 'data/log')

app = Flask(__name__)
CORS(app)


def get_vertex_flow(tasks, vertex):
    tasks_flows = []

    for task in tasks:
        input_time = []
        input_val = []
        process_time = []
        process_val = []

        if task["vex_name"] != vertex:
            continue
        for idx, _ in enumerate(task["input_info"][1:]):
            if task["input_info"][idx]["speed"] * task["processor_info"][idx]["speed"] == 0:
                continue
            input_time.append(task["input_info"][idx]["time"])
            input_val.append(4096000 / max(0, task["input_info"][idx]["speed"]))

            process_time.append(task["processor_info"][idx]["time"])
            process_val.append(4096000 / max(0, task["processor_info"][idx]["speed"]))

        tasks_flows.append({"process_time": process_time,
                            "process_val": process_val,
                            "input_val": input_val,
                            "input_time": input_time})
    return tasks_flows


# @app.route('/api/test/', methods=['POST', 'GET'])
# def tasks_flow():
#     test_flow = get_vertex_flow(tasks, "Map 1")
#
#     return jsonify({"test_flow": test_flow}), 200, {"Content-Type": "application/json"}
#

@app.route('/api/tasks_flow/', methods=['POST', 'GET'])
def get_tasks_flow():
    params = request.json
    app_name = params['app']

    flow_path = os.path.join(os.path.join(DATA_DIR, app_name), 'flow_info.json')
    if os.path.exists(flow_path):
        info = read_flow_from_file(flow_path)
        return jsonify(info), 200, {"Content-Type": "application/json"}

    input_path = os.path.join(os.path.join(DATA_DIR, app_name), 'tasks.json')
    plan_path = os.path.join(os.path.join(DATA_DIR, app_name), 'plan.plan')
    dag_path = os.path.join(os.path.join(os.path.join(DATA_DIR, app_name), 'output'), "DagUpdate.json")

    connect_pair = load_connection(plan_path)
    tasks = read_json_obj(input_path)
    vertexes = get_vertex(tasks)
    flows = {}

    print("calculating" + f" {app_name}......")
    tasks_machine_map = get_task_machine_map(tasks)
    ops = get_vertex_ops(dag_path)
    for vertex in vertexes:
        flow, start_t, end_t, total_s_t, total_e_t, pro_s_t, pro_e_t, in_e_t, out_s_t = generate_task_stack(tasks,
                                                                                                            vertex)
        tasks_list = get_tasks_list(tasks, vertex)
        src_data_map, cal_data_map = generate_data_dis(tasks, vertex, tasks_machine_map)
        # log_rate_transform(flow)
        flows[vertex] = {
            "tasks_flow": flow,
            "tasks_list": tasks_list,
            "start_time": start_t,
            "total_start_time": total_s_t,
            "process_start_time": pro_s_t,
            "output_start_time": out_s_t,
            "input_end_time": in_e_t,
            "process_end_time": pro_e_t,
            "end_time": end_t,
            "total_end_time": total_e_t,
            "src_data_dis": src_data_map,
            "cal_data_dis": cal_data_map,
            "operators": ops[vertex]
        }
    g_start, g_end = get_start_end_time(tasks)
    before_bytes, left_bytes, processed_bytes, start_t, end_time = generate_overall_stack(tasks)

    print("calculate " + f"{app_name} done")
    write_to_file({"overall_flow": {
        "before_flow": before_bytes,
        "left_flow": left_bytes,
        "processed_flow": processed_bytes
    }, "vertexes_flow": flows, "vertexes": list(vertexes),
        "start_time": g_start,
        "end_time": g_end,
        "connect": connect_pair}, os.path.join(os.path.join(DATA_DIR, app_name), 'flow_info.json'))
    return jsonify(
        {"overall_flow": {
            "before_flow": before_bytes,
            "left_flow": left_bytes,
            "processed_flow": processed_bytes
        },
            "vertexes_flow": flows, "vertexes": list(vertexes),
            "start_time": g_start,
            "end_time": g_end,
            "connect": connect_pair}
    ), 200, {"Content-Type": "application/json"}


@app.route('/api/tasks_details/', methods=['POST', 'GET'])
def get_tasks_details():
    params = request.json
    app_name = params['app']

    details_path = os.path.join(os.path.join(DATA_DIR, app_name), 'tasks_details.json')
    if os.path.exists(details_path):
        info = read_flow_from_file(details_path)
        return jsonify(info), 200, {"Content-Type": "application/json"}

    print(app_name, "calculating tasks details...")
    input_path = os.path.join(os.path.join(DATA_DIR, app_name), 'tasks.json')
    tasks = read_json_obj(input_path)
    tasks_flow = tasks_details(tasks)
    write_to_file(tasks_flow, details_path)
    return jsonify(tasks_flow), 200, {"Content-Type": "application/json"}


def map_task_details(task, num):
    input_time = []
    input_val = []
    process_time = []
    process_val = []
    length = len(task["input_speed"])
    gap = 1 if length < 20 else (length // 20)
    # totalLen = 1 if length < 10 else (length // 10)
    for idx in range(0, length, gap):
        # for idx in range(length):
        if task["input_speed"][idx]["speed"] * task["process_speed"][idx]["speed"] == 0:
            continue
        input_val.append(1 / max(0, task["input_speed"][idx]["speed"]))
        input_time.append(task["input_speed"][idx]["time"])
        process_val.append(1 / max(0, task["process_speed"][idx]["speed"]))
        process_time.append(task["process_speed"][idx]["time"])
    return {"container": task["container"][-6:],
            "machine": task["machine"],
            "task_id": task["task_id"],
            "data_machine": task["data_machine"],
            "vertex": task["vex_name"],
            "start_time": task["start_time"],
            "end_time": task["end_time"],
            "process_time": process_time,
            "process_val": process_val,
            "input_val": input_val,
            "input_time": input_time}


def reduce_task_details(task, num):
    # TODO add shuffle information, for map tasks too
    shuffle_info = []
    for attempt in task["fetches"]:
        tmp = task["fetches"][attempt]
        if "time_taken" in tmp:
            shuffle_info.append(tmp)

    processed_bytes = task["input_bytes_info"]
    processed_speed_full = []
    for idx in range(1, len(processed_bytes)):
        if processed_bytes[idx]["time"] - processed_bytes[idx - 1]["time"] == 0:
            continue
        processed_speed_full.append((processed_bytes[idx]["total_bytes"] - processed_bytes[idx - 1]["total_bytes"]) / (
                processed_bytes[idx]["time"] - processed_bytes[idx - 1]["time"]))
    process_time = []
    process_val = []
    length = len(processed_speed_full)
    gap = 1 if length < 20 else (length // 20)
    max_val = 0.0001
    for idx in range(0, len(processed_speed_full)):
        max_val = max(max_val, processed_speed_full[idx])
    for idx in range(0, length, gap):
        process_val.append(processed_speed_full[idx] / max_val)
        process_time.append(processed_bytes[idx]["time"])
    return {"container": task["container"][-6:],
            "machine": task["machine"],
            # "task_id": num,
            "task_id": task["task_id"],
            "vertex": task["vex_name"],
            "start_time": task["start_time"],
            "end_time": task["end_time"],
            "process_begin_time": task["step_info"][4],
            "process_time": process_time,
            "process_val": process_val,
            "shuffle_info": shuffle_info}


def tasks_details(tasks):
    vertex_map = {}
    container_map = {}
    num = 0
    for task in tasks[:]:
        if task["vex_name"] not in vertex_map:
            vertex_map.update({
                task["vex_name"]: []
            })
        container_id = task["machine"][-2:] + "-" + task["container"][-2:]
        if container_id not in container_map:
            container_map.update({
                container_id: {"map": [], "reducer": []}
            })
        if "Map" not in task["vex_name"]:
            details = reduce_task_details(task, num)
            container_map[container_id]["reducer"].append(details)
        else:
            details = map_task_details(task, num)
            container_map[container_id]["map"].append(details)
        num += 1

    return container_map


def read_flow_from_file(input_path):
    return read_json_obj(input_path)


def main():
    app.run(debug=True, host="0.0.0.0")


if __name__ == '__main__':
    main()
