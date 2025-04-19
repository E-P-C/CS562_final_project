import subprocess
from pathlib import Path


def parse_phi_from_file(filepath):
    phi = {
        "attributes": [],
        "gv_count": 0,
        "gv": [],
        "agg_func": [],
        "gv_predicates": [],
        "having": ""
    }

    key_map = {
        "S": "attributes",
        "n": "gv_count",
        "V": "gv",
        "F": "agg_func",
        "sig": "gv_predicates",
        "G": "having"
    }

    with open(filepath, "r") as f:
        lines = f.read().strip().split("\n")

    for line in lines:
        if not line.strip():
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()

        mapped_key = key_map.get(key)
        if not mapped_key:
            raise ValueError(f"Unknown key: {key}")

        if mapped_key == "gv_count":
            phi[mapped_key] = int(value)
        elif mapped_key == "having":
            phi[mapped_key] = value
        elif mapped_key == "gv_predicates":
            predicates = [v.strip() for v in value.split(",") if v.strip()]
            parsed_preds = []
            for pred in predicates:
                if "." in pred:
                    scan_prefix, rest = pred.split(".", 1)
                    if "==" not in rest:
                        rest = rest.replace("=", "==", 1)
                    attr, val = rest.split("==", 1)
                    attr = attr.strip()
                    val = val.strip()
                    if not val.startswith("'"):
                        val = f"'{val}'"
                    parsed = f"(x['{attr}']=={val})"
                    parsed_preds.append(parsed)
            phi[mapped_key] = parsed_preds
        else:
            phi[mapped_key] = [v.strip() for v in value.split(",")]

    return phi


def main():
    file = "mf_input.txt"
    filePath = Path(file).with_name(file)
    print(filePath)

    phi = parse_phi_from_file(filePath)

    gv_attrs = phi["gv"]
    agg_attrs = phi["agg_func"]

    key_tuple_expr = ", ".join([f"row['{g}']" for g in gv_attrs])
    gv_dict_expr = ", ".join([f"'{g}': row['{g}']" for g in gv_attrs])

    agg_init_lines = ""
    for agg in agg_attrs:
        if "min" in agg:
            agg_init_lines += f"h_row['{agg}'] = float('inf')\n            "
        elif "max" in agg:
            agg_init_lines += f"h_row['{agg}'] = float('-inf')\n            "
        else:
            agg_init_lines += f"h_row['{agg}'] = 0\n            "

    agg_logic_lines = """
    for sc in range(1, %d + 1):
        predicate = %s[sc - 1]
        print(f"Evaluating predicate for scan {sc}: {predicate}")
        for row in rows:
            row = dict(row)
            for h_row in _global:
                try:
                    x = row
                    if eval(predicate) and all(h_row[g] == row[g] for g in %s):
                        for agg in %s:
                            if agg.startswith(f"sum_{sc}") and 'quant' in row:
                                h_row[agg] += row['quant']
                            elif agg.startswith(f"count_{sc}"):
                                h_row[agg] += 1
                            elif agg.startswith(f"min_{sc}") and 'quant' in row:
                                h_row[agg] = min(h_row[agg], row['quant'])
                            elif agg.startswith(f"max_{sc}") and 'quant' in row:
                                h_row[agg] = max(h_row[agg], row['quant'])
                except Exception as e:
                    print("Eval error:", e)
                    continue
    """ % (phi['gv_count'], phi['gv_predicates'], phi['gv'], phi['agg_func'])

    # Post-processing: calculate avg_{sc}_quant = sum_{sc}_quant / count_{sc}
    avg_logic_lines = """
    for h_row in _global:
        for agg in %s:
            if agg.startswith("avg_"):
                parts = agg.split("_")
                sc = parts[1]
                base = "_".join(parts[2:])
                sum_key = f"sum_{sc}_{base}"
                count_key = f"count_{sc}"
                if sum_key in h_row and count_key in h_row and h_row[count_key] != 0:
                    h_row[agg] = h_row[sum_key] / h_row[count_key]
    """ % phi['agg_func']

    body = f"""
    rows = cur.fetchall()
    keys_seen = set()
    for row in rows:
        row = dict(row)
        key = ({key_tuple_expr})
        if key not in keys_seen:
            keys_seen.add(key)
            h_row = {{{gv_dict_expr}}}
            {agg_init_lines}
            _global.append(h_row)

    {agg_logic_lines}
    {avg_logic_lines}

    print("MF Structure After Aggregation:")
    for row in _global:
        print(row)

    # print("\\nAggregate Summary:")
    # agg_keys = [k for k in _global[0].keys() if any(a in k for a in ['sum', 'count', 'avg', 'min', 'max'])]
    # for key in agg_keys:
    #     values = [row[key] for row in _global if isinstance(row[key], (int, float))]
    #     if values:
    #         print(f"{{key}}: min = {{min(values)}}, max = {{max(values)}}")
    """

    tmp = f'''
import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# DO NOT EDIT THIS FILE, IT IS GENERATED BY generator.py

def query():
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute("SELECT * FROM sales")
    
    _global = []
    {body}

def main():
    query()
    
if __name__ == "__main__":
    main()
'''

    with open("_generated.py", "w") as f:
        f.write(tmp)

    subprocess.run(["python", "_generated.py"])


if __name__ == "__main__":
    main()
