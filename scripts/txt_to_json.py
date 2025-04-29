import ast, json, numpy as np

infile = "../test.txt"
outfile = "../test.jsonl"

with open(infile) as fin, open(outfile, "w") as fout:
    for line in fin:
        obj = ast.literal_eval(line)
        for k, v in obj.items():
            if isinstance(v, np.ndarray):
                obj[k] = v.tolist()
        json.dump(obj, fout)
        fout.write("\n")
print("finish convertion ", outfile)