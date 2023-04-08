import copy
import json
import os

import requests
import re
from matplotlib import pyplot as plt


def get_log(key, app_id):
    log_url = "http://10.20.96.60:8888/api/log"
    header = {"Content-Type": "application/json;charset=UTF-8"}
    r = requests.get(url=log_url, params={"app_id": app_id})

    dir_path = f"log/io_logs/{key}"
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)

    with open(f"log/io_logs/{key}/{app_id}.log", "wb") as p:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                p.write(chunk)
    print(f"{app_id} update from server log done")


def get_loc(dir, file_name):
    log_url = "http://10.20.96.60:5000/api/location"
    header = {"Content-Type": "application/json;charset=UTF-8"}
    r = requests.get(url=log_url, params={"dir": dir, "file_name": file_name})
    res = r.text
    re_res = re.search(r'DatanodeInfoWithStorage\[(.*)]]', res).group(1)
    print(re.search(r'(.*):', re_res.split(",")[0]).group(1))
    # print(r.text)


def get_tmp():
    log_url = "http://10.20.96.60:8888/api/tmp"
    header = {"Content-Type": "application/json;charset=UTF-8"}
    r = requests.get(url=log_url, params={"query": "tmp.log"})
    # path = f"chi_9.2/log/reducer_{num}"
    path = "../data/log/query62"
    # path = "../data/log/query62/resource_comp"

    if not os.path.exists(path):
        os.mkdir(path)
    with open(f"{path}/log.log", "wb") as p:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                p.write(chunk)
    print("tmp update from server log done")


if __name__ == "__main__":
    get_tmp()
