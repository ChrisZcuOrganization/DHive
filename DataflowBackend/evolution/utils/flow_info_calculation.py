import os
from util import *

FILE_ABS_PATH = os.path.dirname(__file__)
SERVER_PATH = os.path.join(FILE_ABS_PATH, '../../')

DATA_DIR = os.path.join(SERVER_PATH, 'data_chi_9.2/log')


def flow_generate(app_name):
    input_path = os.path.join(os.path.join(DATA_DIR, app_name), 'tasks.json')
    plan_path = os.path.join(os.path.join(DATA_DIR, app_name), 'plan.plan')
    connect_pair = load_connection(plan_path)
    tasks = read_json_obj(input_path)
    vertexes = get_vertex(tasks)
    flows = {}

    print("calculating" + f" {app_name}......")
    for vertex in vertexes:
        flow, start_time, end_time = generate_task_stack(tasks, vertex)
        flows[vertex] = {
            "tasks_flow": flow,
            "start_time": start_time,
            "end_time": end_time
        }
    g_start, g_end = get_start_end_time(tasks)

    print("calculate " + f"{app_name} done")
    write_to_file({"overall_flow": [], "vertexes_flow": flows, "vertexes": list(vertexes),
                   "start_time": g_start,
                   "end_time": g_end,
                   "connect": connect_pair}, os.path.join(os.path.join(DATA_DIR, app_name), 'flow_info.json'))


def main():
    base_dir = "../../data_chi_9.2/log/query{}"
    for i in range(5, 99):
        query_path = base_dir.format(i)
        if not os.path.exists(query_path):
            continue
        if os.path.exists(f"{query_path}/flow_info.json"):
            continue
        flow_generate(f"query{i}")


if __name__ == '__main__':
    main()
