import subprocess
from pathlib import Path

# Function to parse the mf_input.txt file and extract all configuration inputs
def parse_phi_from_file(filepath):
    phi = {
        "attributes": [],
        "gv_count": 0,
        "gv": [],
        "agg_func": [],
        "gv_predicates": [],
        "having": ""
    }

    # Mapping short keys to actual dictionary keys
    key_map = {
        "S": "attributes",
        "n": "gv_count",
        "V": "gv",
        "F": "agg_func",
        "sig": "gv_predicates",
        "G": "having"
    }

    # Read all lines from the config file
    with open(filepath, "r") as f:
        lines = f.read().strip().split("\n")

    for line in lines:
        if not line.strip():
            continue  # Skip blank lines
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
            # If no predicate is given, inject always-true predicate like 1==1, 2==2,...
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

                    # Literal check: if the predicate is like (1==1), just keep it
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

# Main function that parses input and generates _generated.py
def main():
    file = "mf_input.txt"
    filePath = Path(file).with_name(file)
    print(f"Using input file: {filePath}")

    # Parse all inputs
    phi = parse_phi_from_file(filePath)

    # Automatically add sum_* and count_* if avg_* is specified
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

    key_tuple_expr = ", ".join([f"row['{g}']" for g in gv_attrs])
    gv_dict_expr = ", ".join([f"'{g}': row['{g}']" for g in gv_attrs])

    # Initialization logic for aggregates
    agg_init_lines = ""
    for agg in agg_attrs:
        if "min" in agg:
            agg_init_lines += f"h_row['{agg}'] = float('inf')\n            "
        elif "max" in agg:
            agg_init_lines += f"h_row['{agg}'] = float('-inf')\n            "
        else:
            agg_init_lines += f"h_row['{agg}'] = 0\n            "

    # Main aggregation logic by scan
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

    # Post-processing logic to calculate avg_*
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

    # Combine it all into the script body
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
    """

    # Final full script to be written into _generated.py
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

    # Write the generated script and execute it
    with open("_generated.py", "w") as f:
        f.write(tmp)

    subprocess.run(["python", "_generated.py"])

# Start here
if __name__ == "__main__":
    main()
