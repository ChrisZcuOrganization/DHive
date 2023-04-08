import re

from models.data_edge import Edge
from models.data_node import Node


def useful_lines(line: str) -> bool:
    """the data flow from execution plan"""
    return "      " == line[0:6]


def load_logic_dag(path: str):
    lines = []
    with open(path, 'r') as fp:
        while True:
            line = fp.readline()
            if "Stage" in line:
                break
            if "<-" in line:
                lines.append(line)
    return lines


def generate_logic_dag(lines):
    dags = {}

    for line in lines:
        dst = re.match("(.*)<-(.*)", line).group(1).strip()
        if dst not in dags:
            dags.update({
                dst: {
                    "dst_nodes": [],
                    "src_nodes": [],
                    "in_degree": 0
                }
            })
        srcs = line.split("<- ")[1].split(", ")
        for src in srcs:
            vex = src.split("(")[0].strip()
            if vex not in dags:
                dags.update({
                    vex: {
                        "dst_nodes": [],
                        "src_nodes": [],
                        "in_degree": 0
                    }
                })
            dags[vex]["dst_nodes"].append(dst)
            dags[dst]["src_nodes"].append(vex)
    for vex in dags:
        dags[vex]["in_degree"] = len(dags[vex]["src_nodes"])
    return dags


def generate_logic_dags(path: str):
    lines = load_logic_dag(path)
    dags = generate_logic_dag(lines)
    return dags


def load_exe_plan(path: str, filter_rules=(lambda line: True)) -> [str]:
    lines = []
    with open(path, 'r') as fp:
        while True:
            line = fp.readline()
            if not line:
                break
            if filter_rules(line):
                lines.append(line.replace("\n", ""))
    return lines


def generate_data_dag(logs, dags):
    pros_lines = []
    nodes = {}
    cur_layers = [-1]
    node_id = 0
    edges = []
    nodes_layer = [(-1, -1)]

    for idx in range(len(logs)):
        line = logs[idx]
        is_last_line = False
        if idx == len(logs) - 1:
            pros_lines.append(line)
            is_last_line = True
        if re.match(r"(.*)<-", line) or is_last_line:
            layers = len(re.match(r"(.*)<-", line).group(1)) if not is_last_line else cur_layers[-1]
            if layers < cur_layers[-1] or is_last_line:
                src_layer = cur_layers[-1]
                while layers <= cur_layers[-1] or (is_last_line and len(cur_layers) > 1):
                    # print(cur_layers)
                    processing_lines = []
                    while True:
                        last_line = pros_lines.pop()
                        processing_lines.append(last_line)
                        if "<-" in last_line:
                            cur_layers.pop()
                            break
                    ori_cols = []
                    out_cols = []
                    data_name = "tmp"
                    pred = ""
                    head_line = processing_lines[-1]
                    # print("*****************************")
                    # print(processing_lines[-1])
                    for item in processing_lines:
                        if "@" in item:
                            tmp = re.match(r'(.*)Output:\[(.*)]', item).group(2)
                            ori_cols = tmp.replace("\"", "").split(",")
                            data_name = re.match(r"(.*)@(.*)Tbl(.*)", item).group(2).split(",")[0]
                        elif "Please refer to" in item:
                            data_name = "previous"
                        elif "predicate" in item:
                            pred = (item.strip())
                        elif "Output:" in item:
                            tmp = re.match(r'(.*)Output:\[(.*)]', item).group(2)
                            out_cols = tmp.replace("\"", "").split(",")
                    node = Node(node_id, data_name, processing_lines[-1], ori_cols, out_cols)
                    node_id += 1
                    nodes[node_id - 1] = node

                    vex = re.match(r"(.*)<-(.*) \[", head_line).group(2)
                    cur_node_layer = len(re.match(r"(.*)<-(.*)", head_line).group(1))
                    if "Map" in vex or "Reducer" in vex:
                        node_out = Node(node_id, f"data from {vex}", f"data from {vex}", [], [])
                        node_id += 1
                        if cur_node_layer < nodes_layer[-1][1]:
                            while cur_node_layer < nodes_layer[-1][1]:
                                edges.append(Edge(nodes[nodes_layer[-1][0]], node_out, vex))
                                edges[-1].print()
                                nodes_layer.pop()
                        else:  # must be map here
                            edges.append(Edge(node, node_out, vex))
                            edges[-1].print()
                        nodes[node_id - 1] = node_out
                        nodes_layer.append((node_id - 1, cur_node_layer))
                    else:
                        # if "Merge Join" not in head_line:
                        # continue
                        # print(head_line)
                        nodes_layer.append((node_id - 1, cur_node_layer))
            cur_layers.append(layers)
        pros_lines.append(line.replace("\n", ""))


def dataflow_from_dag(logs, dags):
    pass


def gen_data(lines, begin_line, end_line, vex=None):
    # print(begin_line, end_line)
    vex_name = ""
    if "<-" in lines[begin_line]:
        if "vectorized" in lines[begin_line]:
            vex_name = re.match(r"(.*)<-(.*) vectorized", lines[begin_line]).group(2)
        elif any(p in lines[begin_line] for p in ["Map", "Reducer"]):
            vex_name = re.match(r"(.*)<-(.*) \[", lines[begin_line]).group(2)
        else:
            vex_name = vex
    children = []

    print(f"data from {vex_name}")
    node = Node(f"data from {vex_name}", vex_name, children)
    cur_layer = len(re.match(r"(.*)<-", lines[begin_line]).group(1))
    next_node_line = begin_line + 1
    while True:
        if next_node_line == end_line:
            break
        if "<-" in lines[next_node_line]:
            break
        else:
            next_node_line += 1

    if next_node_line == end_line:
        return node

    tmp_idx = next_node_line + 1
    next_layer = len(re.match(r"(.*)<-", lines[next_node_line]).group(1))
    while tmp_idx < end_line:
        if "<-" in lines[tmp_idx]:
            if len(re.match(r"(.*)<-", lines[tmp_idx]).group(1)) <= next_layer:
                children.append(gen_data(lines, next_node_line, tmp_idx, vex_name))
                next_node_line = tmp_idx
        tmp_idx += 1
    children.append(gen_data(lines, next_node_line, end_line, vex_name))

    return node


def main():
    log_path = "../data4/Query29/query29.plan"
    dags = generate_logic_dags(log_path)
    # for item in dags:
    #     print(item, dags[item])
    logs = load_exe_plan(log_path, useful_lines)
    logs[0] = logs[0][:4] + "<-" + logs[0][6:]
    gen_data(logs, 0, len(logs), " ")
    # generate_data_dag(logs, "")
    dataflow_from_dag(logs, dags)


if __name__ == '__main__':
    main()
