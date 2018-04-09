import numpy as np
import sys

def compute_metrics(x, name):
    print("Average", name, "time:", np.mean(x))
    print("STD of", name, "time:", np.std(x, ddof=1))
    print(name,x)

def parse_times_file(filename):
    load, encode, summary = [], [], []
    with open(filename, "r") as f:
        for line in f:
            if "Loading" in line:
                load.append(int(line.split()[-1]))
            elif "Encoding" in line:
                encode.append(int(line.split()[-1]))
            else:
                summary.append(int(line.split()[-1]))
    load, encode, summary = np.array(load), np.array(encode), np.array(summary)
    compute_metrics(load, "Loading")
    compute_metrics(encode, "Encoding")
    compute_metrics(summary, "Summarization")

def reorder_output(filename):
    with open(filename, "r") as f:
        lines = [line for line in f]
    lines.sort()
    with open(filename, "w") as f:
        for line in lines:
            f.write(line)

def parse_dump(filename):
    with open(filename, "r") as f:
        for line in f:
            if "Loading time:" in line:
                line_split = line.split()
                print("Loading time:", line_split[line_split.index("time:") + 1])
            elif "Encoded in:" in line:
                line_split = line.split()
                print("Encoding time:", line_split[line_split.index("in:") + 1])
            elif "Summarization time:" in line:
                line_split = line.split()
                print("Summary time:", line_split[line_split.index("time:") + 1])

def main():
    if sys.argv[1] == "dump":
        parse_dump(sys.argv[2])
    else:
        parse_times_file(sys.argv[2])
        reorder_output(sys.argv[2])

if __name__ == "__main__":
	main()
