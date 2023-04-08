import json
import os

import matplotlib.pyplot as plt


def mons_merge(in_dir, out_path):
    full_mons = {}
    for file in os.listdir(in_dir):
        if "dbg" not in file:
            continue
        file_path = in_dir + "/" + file
        with open(file_path, 'r') as fp:
            items = fp.readlines()
        tmp = []
        for item in items:
            item = item.replace("\n", "")
            tmp.append(json.loads(item))
        full_mons[file[:5]] = tmp

    mons_str = json.dumps(full_mons)
    with open(out_path, 'w') as fp:
        fp.writelines(mons_str)


def read_json_obj(filename: str):
    with open(filename, 'r', encoding='utf-8') as fp:
        json_obj = json.load(fp)
    return json_obj


def mem_com(mons_obj, fig):
    idx = 1
    for key in keys:
        for machine in machines:
            mons_tar = mons_obj[key]
            ax = fig.add_subplot(9, 2, idx)
            mons_items = mons_tar[machine]
            x_val = []
            y_val = []
            for index, item in enumerate(mons_items[:]):
                x_val.append(item["timestamp"] / 1000)
                y_val.append(item["mem"]["used"] / 1000000000)

            # ax.plot([t - x_val[0] for t in x_val[1:]],
            #         [(y_val[i] - y_val[i - 1]) / (x_val[i] - x_val[i - 1]) for i in range(1, len(y_val))])
            ax.plot([x - x_val[0] for x in x_val], [y - y_val[0] for y in y_val])
            ax.set_ylim([0, 13])
            ax.set_xticks([])
            idx += 1


keys = ["small", "large"]
machines = ["dbg03", "dbg04", "dbg05", "dbg08",
            "dbg09", "dbg10", "dbg11", "dbg12",
            "dbg14"]


def main():
    # r_num_file = "chi_9.2/log/query29/query29_rnum/full_mons.json"
    small_cs_file = "chi_9.2/log/query29/query29_small/full_mons.json"
    large_cs_file = "chi_9.2/log/query29/query29_large/full_mons.json"
    mons_obj = {
        # "r_num": read_json_obj(r_num_file),
        "small": read_json_obj(small_cs_file),
        "large": read_json_obj(large_cs_file)
    }
    fig = plt.figure(figsize=(5, 10))
    mem_com(mons_obj, fig)
    plt.show()
    plt.tight_layout()
    # mons_merge("chi_9.2/log/query29/query29_rnum", "chi_9.2/log/query29/query29_rnum/full_mons.json")


if __name__ == '__main__':
    # mons_merge("chi_9.2/log/query29/query29_small", "chi_9.2/log/query29/query29_small/full_mons.json")
    main()
