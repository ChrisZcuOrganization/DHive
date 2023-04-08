import copy
import json
import os

import requests
import re
from matplotlib import pyplot as plt


def read_lines_cond(filename: str, line_filter=(lambda line: True)):
    """ Read all line that satisfies the filter """
    lines = []
    with open(filename, 'r', encoding='utf-8') as fp:
        while True:
            line = fp.readline()
            if not line:
                break
            if line_filter(line):
                lines.append(line)
    return lines


def is_useful_line(line: str) -> bool:
    return " Profiling: Tez" in line \
           or "Container: container_" in line \
           or " Profiling: Hadoop" in line \
           or " Final Counters for " in line \
           or " Completed fetch for attempt" in line \
           or "|HttpConnection.url|: for url=" in line \
           or "Initializing task, taskAttemptId" in line


def log_parser(input_path, out_path, is_write):
    lines = read_lines_cond(input_path, is_useful_line)

    task_list = []
    step_info = [0.0 for _ in range(10)]
    task_id = 1
    cur_task = {}
    fetches = {}
    fetches_item = []
    fetch_num = 0
    counter = {}
    input_speed = []
    process_speed = []

    input_bytes_info = []
    processed_bytes_info = []

    for line in lines:
        if "Container: container_" in line:
            if re.search(" on (.*):", line):
                machine = re.search(" on (.*):", line).group(1)
            else:
                machine = re.search(" on (.*)_", line).group(1)
            container = re.search("Container: container_(.*) on", line).group(1)
            cur_task.update({
                "machine": machine,
                "container": container
            })

        # task start
        elif "Container task starting" in line:
            start_time = re.search(r'starting at time (.*) ms', line).group(1)
            cur_task.update({
                "start_time": float(start_time)
            })
        # task info
        elif "Container task info" in line:
            vex_name = re.search(r'VectorName: (.*) VertexParallelism:', line).group(1)
            cur_task.update({
                "vex_name": vex_name,
                "task_id": task_id
            })
            task_id += 1
        elif "Initializing task, taskAttemptId" in line:
            task_id_tmp = re.search(r"Initializing task, taskAttemptId=(.*)", line).group(1)
            cur_task["task_id"] = task_id_tmp
        elif "Hadoop read file path is" in line:
            file_dir = re.match(r"(.*)/100/(.*)/(.*)", line).group(2)
            file_name = re.match(r"(.*)/100/(.*)/(.*)", line).group(3)
            cur_task.update({
                "file_dir": file_dir,
                "file_name": file_name
            })
        elif "'Initialization'" in line:
            if "starting" in line:
                start_time = re.search(r'starting at time (.*) ms', line).group(1)
                step_info[0] = float(start_time)
            elif "ending" in line:
                end_time = re.search(r'ending at time (.*) ms', line).group(1)
                step_info[1] = float(end_time)
        elif "'Input' on" in line or "'Shuffle' on" in line:
            if "starting" in line:
                start_time = re.search(r'starting at time (.*) ms', line).group(1)
                step_info[2] = float(start_time)
            elif "ending" in line:
                # print(line)
                end_time = re.search(r'ending at time (.*) ms', line).group(1)
                step_info[3] = float(end_time)
        elif "'Processor'" in line:
            if "starting" in line:
                start_time = re.search(r'starting at time (.*) ms', line).group(1)
                step_info[4] = float(start_time)
            elif "ending" in line:
                end_time = re.search(r'ending at time (.*) ms', line).group(1)
                step_info[5] = float(end_time)
        elif "'Sink'" in line:
            if "start" in line:
                start_time = re.search(r'start at time (.*) ms', line).group(1)
                step_info[6] = float(start_time)
            elif "end" in line:
                end_time = re.search(r'end at time (.*) ms', line).group(1)
                step_info[7] = float(end_time)
        elif "'Spill'" in line:
            if "starting" in line:
                continue
                start_time = re.search(r'starting at time (.*) ms', line).group(1)
                step_info[8] = float(start_time)
            elif "ending" in line:
                if 'ms' in line:
                    end_time = re.search(r'ending at time (.*) ms', line).group(1)
                else:
                    end_time = re.search(r'ending at time (.*) ns', line).group(1)
                step_info[9] = float(end_time)
        elif "read time is" in line:
            groups = re.match(r"(.*)read time is (.*) at time (.*)", line)
            time_usage = int(groups.group(2))
            time_stamp = int(groups.group(3))
            input_speed.append({"speed": time_usage, "time": time_stamp})
        elif "processor time is" in line:
            groups = re.match(r"(.*)processor time is (.*) at time (.*)", line)
            time_usage = int(groups.group(2))
            time_stamp = int(groups.group(3))
            process_speed.append({"speed": time_usage, "time": time_stamp})
        elif "|HttpConnection.url|: for url=" in line:
            machine = re.search(r"http://(.*):", line).group(1)
            src_ids = re.search(r"&map=(.*) sent", line).group(1)
            # if machine not in fetches:
            #     fetches.update({
            #         machine: {}
            #     })
            src_id = src_ids.split(",")
            for id_tmp in src_id:
                fetches.update({
                    id_tmp: {"machine": machine}
                })
        elif "processor input  bytes" in line:
            # print(line)
            input_bytes = re.match(r"(.*)bytes is (.*)at time (.*)", line).group(2)
            time = re.match(r"(.*)bytes is (.*)at time (.*)", line).group(3)
            input_bytes_info.append({"time": int(time), "input_bytes": int(input_bytes)})
        elif "Tez processed bytes is" in line:
            bytes = re.match(r"(.*)total\): (.*) at time (.*)", line).group(2).split(", ")
            time = re.match(r"(.*)total\): (.*) at time (.*)", line).group(3)
            processed_bytes_info.append({
                "key_bytes": int(bytes[0]),
                "value_bytes": int(bytes[1]),
                "total_bytes": int(bytes[2]),
                "time": int(time)
            })
        # elif "collect keyWritable" in line:
        #     key_bytes = re.match(r"(.*)collect keyWritable (.*), value length is (.*), total bytes is (.*)at time (.*)",
        #                          line).group(2)
        #     val_bytes = re.match(r"(.*)collect keyWritable (.*), value length is (.*), total bytes is (.*)at time (.*)",
        #                          line).group(3)
        #     total_bytes = re.match(
        #         r"(.*)collect keyWritable (.*), value length is (.*), total bytes is (.*)at time (.*)",
        #         line).group(3)
        #     time = re.match(r"(.*)collect keyWritable (.*), value length is (.*), total bytes is (.*)at time (.*)",
        #                     line).group(5)
        #     processed_bytes_info.append({
        #         "key_bytes": key_bytes,
        #         "val_bytes": val_bytes,
        #         "total_bytes": total_bytes,
        #         "time": time
        #     })
        elif "|ShuffleScheduler.fetch|: Completed fetch for attempt:" in line:
            fetch_num += 1
            id_tmp = re.search("attempt_(.*)}", line).group(1)
            id_tmp = "attempt_" + id_tmp
            dist = re.search("} to (.*), csize", line).group(1)
            csize = re.search("csize=(.*), dsize", line).group(1)
            dsize = re.search("dsize=(.*), End", line).group(1)
            if re.search("Rate=(.*) MB", line) is None:
                print(line)
                continue
            rate = re.search("Rate=(.*) MB", line).group(1)
            end_time = re.match(r"(.*)EndTime=(.*), Time", line).group(2)
            time_taken = re.match(r"(.*)TimeTaken=(.*), Rate", line).group(2)
            if id_tmp not in fetches:
                fetches.update({id_tmp: {}})
            fetches[id_tmp].update({
                "dist": dist,
                "csize": int(csize),
                "dsize": int(dsize),
                "rate": float(rate),
                "end_time": int(end_time),
                "time_taken": int(time_taken)
            })
            # fetches_item.append({
            #     "dist": dist,
            #     "csize": csize,
            #     "dsize": dsize,
            #     "rate": rate
            # })
        elif "reducer input key size" in line:
            key_bytes = re.match(
                r"(.*)key size is (.*), value length is (.*), num is (.*), total bytes is (.*) at time (.*)",
                line).group(2)
            value_bytes = re.match(
                r"(.*)key size is (.*), value length is (.*), num is (.*), total bytes is (.*) at time (.*)",
                line).group(3)
            tag_bytes = re.match(
                r"(.*)key size is (.*), value length is (.*), num is (.*), total bytes is (.*) at time (.*)",
                line).group(4)
            total_bytes = re.match(
                r"(.*)key size is (.*), value length is (.*), num is (.*), total bytes is (.*) at time (.*)",
                line).group(5)
            time = re.match(
                r"(.*)key size is (.*), value length is (.*), num is (.*), total bytes is (.*) at time (.*)",
                line).group(6)
            input_bytes_info.append({
                "time": int(time),
                "key_bytes": int(key_bytes),
                "value_bytes": int(value_bytes),
                "tag_bytes": int(tag_bytes),
                "total_bytes": int(total_bytes) + int(tag_bytes),
                "input_bytes": int(total_bytes) + int(tag_bytes) + int(tag_bytes)
            })
        elif "alIOProcessorRuntimeTask|: Final Counters" in line:
            file_counter = re.search(r"(.*)File System Counters (.*)]\[org", line).group(2)
            # print(file_counter)
            task_counter = re.search(r"(.*)org.apache.tez.common.counters.TaskCounter (.*)]\[HIVE", line).group(2)
            # print(line)
            hive_counter = (
                re.match(r"(.*)HIVE (.*)]]", line) if "Shuffle Error" not in line else re.match(
                    r"(.*)HIVE (.*)]\[Shuffle", line)).group(2)

            file_c_list = file_counter.split(", ")
            task_counter_list = task_counter.split(", ")
            hive_counter_list = hive_counter.split(", ")

            for item in file_c_list:
                counter[item.split("=")[0]] = int(item.split("=")[1])
            for item in task_counter_list:
                counter[item.split("=")[0]] = int(item.split("=")[1])
            for item in hive_counter_list:
                counter[item.split("=")[0]] = int(item.split("=")[1].split("][")[0])
            if "OUTPUT_BYTES" in counter:
                processed_bytes_info.append({
                    "total_bytes": int(counter["OUTPUT_BYTES"]),
                    "time": int(step_info[5])
                })

        elif "Container task ending at" in line:
            end_time = re.search(r'Container task ending at time (.*) ms', line).group(1)
            cur_task.update({
                "end_time": float(end_time),
                "step_info": step_info,
                "fetches": fetches,
                "fetch_num": fetch_num,
                "fetches_item": fetches_item,
                "counter": counter,
                "input_speed": input_speed,
                "process_speed": process_speed,
                "input_bytes_info": input_bytes_info,
                "processed_bytes_info": processed_bytes_info
            })
            task_list.append(copy.deepcopy(cur_task))
            cur_task = {}
            counter = {}
            step_info = [0.0 for _ in range(10)]
            fetches = {}
            fetch_num = 0
            fetches_item = []
            input_speed = []
            process_speed = []
            input_bytes_info = []
            processed_bytes_info = []

    if not is_write:
        return task_list

    task_str = json.dumps(task_list)

    with open(out_path, "w") as f:
        f.writelines(task_str)

    return task_list


def get_loc(dir_name, file_name):
    log_url = "http://10.20.96.60:8888/api/location"
    header = {"Content-Type": "application/json;charset=UTF-8"}
    r = requests.get(url=log_url, params={"dir": dir_name, "file_name": file_name})
    res = r.text
    re_res = re.search(r'DatanodeInfoWithStorage\[(.*)]]', res).group(1)
    res = re.search(r'(.*):', re_res.split(",")[0]).group(1)
    #     print(res)
    return res


ip_to_name = {
    "10.20.96.231": "dbg02",
    "10.20.96.60": "dbg03",
    "10.20.5.110": "dbg04",
    "10.20.56.88": "dbg05",
    "10.20.108.214": "dbg06",
    "10.20.90.215": "dbg07",
    "10.20.119.244": "dbg08",
    "10.20.40.108": "dbg09",
    "10.20.51.221": "dbg10",
    "10.20.41.108": "dbg11",
    "10.20.4.104": "dbg12",
    "10.20.5.190": "dbg13",
    "10.20.100.145": "dbg14"

}


def read_json_obj(filename: str):
    with open(filename, 'r', encoding='utf-8') as fp:
        json_obj = json.load(fp)
    return json_obj


def update_tasks(input_path):
    tasks = read_json_obj(input_path)
    for task in tasks:
        if "file_dir" not in task:
            continue
        ip = get_loc(task["file_dir"], task["file_name"])
        machine = (ip_to_name[ip])
        task.update(
            {"data_machine": machine}
        )
    write_tasks(tasks, input_path)


def write_tasks(task_list, out_path):
    task_str = json.dumps(task_list)

    with open(out_path, "w") as f:
        f.writelines(task_str)


def update_tasks_input(tasks_info, tasks, out_path):
    info_map = {}
    for task in tasks_info:
        info_map[task["task_id"]] = task["input_bytes_info"]
    for task in tasks:
        task["input_bytes_info"] = info_map[task["task_id"]]
    write_tasks(tasks, out_path)


def log_parse_batch():
    base_dir = "../data_chi_9.2/log/query{}"
    for i in range(1, 100):
        query_path = base_dir.format(i)
        if not os.path.exists(query_path):
            continue
        if os.path.exists("{0}/tasks.json".format(query_path)):
            continue
        else:
            print("query{} parsing...".format(i))
            tasks_path = "{0}/tasks.json".format(query_path)
            log_path = "{0}/log.log".format(query_path)
            log_parser(log_path, tasks_path, True)


def ip_add_batch():
    base_dir = "../data_chi_9.2/log/query{}"
    for i in range(1, 100):
        query_path = base_dir.format(i)
        if not os.path.exists(query_path):
            continue
        else:
            print("query{} parsing...".format(i))
            tasks_path = "{0}/tasks.json".format(query_path)
            update_tasks(tasks_path)


def main():
    # ip_add_batch()
    # return
    # log_parse_batch()
    # return
    # reducer_150 = f"../data_chi_9.2/log/reducer_{num}/reducer_{num}.log"
    # log_path = "../data_chi_9.2/log/query29/query29_speed/log.log"
    log_path = "../data/log/query62/"
    # log_path = "../data/log/query62/resource_comp/"
    task_list = log_parser(f"{log_path}log.log", f"{log_path}tasks.json", True)
    # update_tasks_input(task_list, old_tasks, "../data_chi_9.2/log/query29/query29_speed/tasks.json")
    # update_tasks(f"{log_path}tasks.json")
    # print(num)


if __name__ == '__main__':
    main()
