import json

import matplotlib.pyplot as plt


def read_json_obj(filename: str):
    with open(filename, 'r', encoding='utf-8') as fp:
        json_obj = json.load(fp)
    return json_obj


def draw_ovall(s_tasks, l_tasks, fig):
    start_time = s_tasks[0]["start_time"]
    end_time = 0
    x_val = []
    y_val = []

    x_val_r = []
    y_val_r = []

    for task in s_tasks:
        start_time = min(start_time, task["start_time"])
        end_time = max(end_time, task["end_time"])
        if task["vex_name"] not in ["Map 1"]:
            continue
        if "Map" not in task["vex_name"]:
            x_val_r.append(task["start_time"])
            y_val_r.append(task["end_time"] - task["start_time"])
            continue
        x_val.append(task["start_time"])
        y_val.append(task["end_time"] - task["start_time"])

    ax = fig.add_subplot(1, 2, 1)
    ax.scatter([x - x_val[0] for x in x_val], [y / 1000 for y in y_val], alpha=0.5)
    ax.scatter([x - x_val[0] for x in x_val_r], [y / 1000 for y in y_val_r], c="r", alpha=0.5)
    ax.set_xlim([0, end_time - start_time])
    ax.set_xticks([])

    x_val = []
    y_val = []
    x_val_r = []
    y_val_r = []

    for task in l_tasks:
        if task["vex_name"] != "Map 1":
            continue
        if "Map" not in task["vex_name"]:
            x_val_r.append(task["start_time"])
            y_val_r.append(task["end_time"] - task["start_time"])
            continue
        x_val.append(task["start_time"])
        y_val.append(task["end_time"] - task["start_time"])
    ax2 = fig.add_subplot(1, 2, 2)
    ax2.scatter([x - x_val[0] for x in x_val], [y / 1000 for y in y_val], alpha=0.5)
    ax2.scatter([x - x_val[0] for x in x_val_r], [y / 1000 for y in y_val_r], c="r", alpha=0.5)
    ax2.set_xlim([0, end_time - start_time])
    ax2.set_xticks([])


def vertex_machine(tasks):
    machines = []
    for task in tasks:
        if task["vex_name"] not in ["Map 7"]:
            continue
        machines.append(task["machine"])
    plt.hist(machines)
    plt.show()


def machine_vertex(tasks):
    vertexs = []
    for task in tasks:
        if "Map" not in task["vex_name"]:
            continue
        vertexs.append(task["vex_name"])
    plt.hist(vertexs)
    plt.show()


def main():
    small_cs_file = "chi_9.2/log/query29/query29_small/tasks.json"
    large_cs_file = "chi_9.2/log/query29/query29_large/tasks.json"
    s_tasks = read_json_obj(small_cs_file)
    l_tasks = read_json_obj(large_cs_file)

    fig = plt.figure(figsize=(10, 5))
    draw_ovall(s_tasks, l_tasks, fig)
    plt.show()
    # vertex_machine(l_tasks)


if __name__ == '__main__':
    main()
