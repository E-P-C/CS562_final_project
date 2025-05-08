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
            if not value:
                value = ",".join([f"{i+1}.1=1" for i in range(phi["gv_count"])])

            predicates = [v.strip() for v in value.split(",") if v.strip()]
            parsed_preds = []
            for pred in predicates:
                if "." in pred:
                    _, rest = pred.split(".", 1)
                    if "==" not in rest:
                        rest = rest.replace("=", "==", 1)

                    attr, val = rest.split("==", 1)
                    attr = attr.strip()
                    val = val.strip()

                    if attr.isdigit() or attr in ["True", "False"]:
                        parsed_preds.append(f"({attr}=={val})")
                    else:
                        if not val.startswith(("'", '"')) and not val.isdigit():
                            val = f"'{val}'"
                        parsed_preds.append(f"(x['{attr}']=={val})")
            phi[mapped_key] = parsed_preds

        else:
            phi[mapped_key] = [v.strip() for v in value.split(",")]

    return phi

def main():
    file = "mf_input.txt"
    filePath = Path(file).with_name(file)
    print(f"Using input file: {filePath}")

    phi = parse_phi_from_file(filePath)

    updated_agg_funcs = phi["agg_func"].copy()
    for agg in phi["agg_func"]:
        if agg.startswith("avg_"):
            parts = agg.split("_")
            sc = parts[1]
            base = "_".join(parts[2:])
            sum_key = f"sum_{sc}_{base}"
            count_key = f"count_{sc}"
            if sum_key not in updated_agg_funcs:
                updated_agg_funcs.append(sum_key)
            if count_key not in updated_agg_funcs:
                updated_agg_funcs.append(count_key)
    phi["agg_func"] = updated_agg_funcs

    gv_attrs = phi["gv"]
    agg_attrs = phi["agg_func"]
    sel_attrs = phi["attributes"]

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

    agg_logic_lines = f"""
    for sc in range(1, {phi['gv_count']} + 1):
        predicate = {phi['gv_predicates']}[sc - 1]
        print(f"Evaluating scan {{sc}}: {{predicate}}")

        for row in rows:
            row = dict(row)
            x = row
            if eval(predicate):
                for h_row in _global:
                    if all(h_row[g] == row[g] for g in {phi['gv']}):
                        for agg in {phi['agg_func']}:
                            if agg.startswith(f"sum_{{sc}}") and 'quant' in row:
                                h_row[agg] += row['quant']
                            elif agg.startswith(f"count_{{sc}}"):
                                h_row[agg] += 1
                            elif agg.startswith(f"min_{{sc}}") and 'quant' in row:
                                h_row[agg] = min(h_row[agg], row['quant'])
                            elif agg.startswith(f"max_{{sc}}") and 'quant' in row:
                                h_row[agg] = max(h_row[agg], row['quant'])
    """

    avg_logic_lines = f"""
    for h_row in _global:
        for agg in {phi['agg_func']}:
            if agg.startswith("avg_"):
                parts = agg.split("_")
                sc = parts[1]
                base = "_".join(parts[2:])
                sum_key = f"sum_{{sc}}_{{base}}"
                count_key = f"count_{{sc}}"
                if sum_key in h_row and count_key in h_row and h_row[count_key] != 0:
                    h_row[agg] = h_row[sum_key] / h_row[count_key]
    """

    # Final result and save output.csv
    body = f"""
    import csv
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

    print("Full MF Structure After Aggregation:")
    for row in _global:
        print(row)

    print("\\nFinal Output (Based on S:)")
    filtered_rows = []
    for row in _global:
        filtered = {{k: row[k] for k in {sel_attrs} if k in row}}
        print(filtered)
        filtered_rows.append(filtered)

    # Save to output.csv
    with open("output.csv", "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames={sel_attrs})
        writer.writeheader()
        writer.writerows(filtered_rows)
    print("Output saved to output.csv")
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

    conn = psycopg2.connect("dbname=" + dbname + " user=" + user + " password=" + password,
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
