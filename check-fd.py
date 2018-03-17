import pandas as pd
import sys

def go(filename, col1, col2):
    cnt1, cnt2 = set(), set()
    df = pd.read_csv(filename)
    for idx, row in df.iterrows():
        cnt1.add(row[col1])
        cnt2.add(row[col2])
    print(col1, "cardinality:", len(cnt1))
    print(col2, "cardinality:", len(cnt2))

def main():
    go(sys.argv[1], sys.argv[2], sys.argv[3])

if __name__ == "__main__":
	main()
