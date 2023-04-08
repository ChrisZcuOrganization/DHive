import os
import re

from sphinx.util import requests


def read_lines_cond(file_name, line_filter=(lambda line: True)):
    lines = []
    with open(file_name, 'r', encoding="utf-8") as fp:
        while True:
            line = fp.readline()
            if not line:
                break
            if line_filter(line):
                lines.append(line)
    return lines


def is_useful_line(line):
    return re.search(r"(.*)query[0-9](.*).sql", line) is not None or "App id application" in line


def parse_query_id(lines):
    query_appid_map = {}
    cur_query = ""
    for idx in range(len(lines)):
        if "query" in lines[idx]:
            cur_query = re.search(r"./(.*).sql", lines[idx]).group(1)
        else:
            app_id = re.search(r"(.*) App id (.*)\)", lines[idx]).group(2)
            query_appid_map.update({
                cur_query: app_id
            })
    return query_appid_map


def fetch_log(out_path, app_id):
    fetch_url = "http://10.20.96.60:8888/api/log/"
    r = requests.get(url=fetch_url, params={"app_id": app_id})
    with open(f"{out_path}/log.log", "wb") as p:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                p.write(chunk)


def fetch_raw_log(file_name):
    row_lines = read_lines_cond(file_name, is_useful_line)
    query_map = parse_query_id(row_lines)

    base_dir = "../../data_chi_9.2/log/{}"
    for query in query_map:
        out_path = base_dir.format(query)
        if os.path.exists(out_path):
            continue
        else:
            os.mkdir(out_path)
        print(query, " ", query_map[query], "......")
        fetch_log(out_path, query_map[query])


def fetch_plan(file_name):
    row_lines = read_lines_cond(file_name, is_useful_line)
    query_map = parse_query_id(row_lines)

    base_dir = "../../data_chi_9.2/log/{}"
    for query in query_map:
        out_path = base_dir.format(query)
        if not os.path.exists(out_path):
            continue
        print(query, "......")
        fetch_url = "http://10.20.96.60:8888/api/plan/"
        r = requests.get(url=fetch_url, params={"query_id": query})
        with open(f"{out_path}/plan.plan", "wb") as p:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    p.write(chunk)


def fetch_sql(file_name):
    row_lines = read_lines_cond(file_name, is_useful_line)
    query_map = parse_query_id(row_lines)

    base_dir = "../../data_chi_9.2/log/{}"
    for query in query_map:
        out_path = base_dir.format(query)
        if not os.path.exists(out_path):
            continue
        print(query, "......")
        fetch_url = "http://10.20.96.60:8888/api/sql/"
        r = requests.get(url=fetch_url, params={"query_id": query})
        with open(f"{out_path}/{query}.sql", "wb") as p:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    p.write(chunk)


def main(file_name):
    # fetch_sql(file_name)
    fetch_log("../../data_chi_9.2/log/query1", "application_1659945682372_0048")


if __name__ == '__main__':
    file = "../../data_chi_9.2/tmp/tpcds_run_script.txt"
    main(file)
