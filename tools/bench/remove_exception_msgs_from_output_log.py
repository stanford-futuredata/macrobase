import sys

if (len(sys.argv) < 2):
    print "Incorrect usage: python %s <results_file>" % sys.argv[0]
    exit(-1)

with open(sys.argv[1], 'r') as f:
    lines = f.read().split('\n')
    i = 0
    while (i < len(lines)):
        line = lines[i]
        if "Exception" in line:
            index = line.find("Exception")
            line = line[:index]
            i += 1
            while (lines[i].startswith('\t') or "Exception" in lines[i]):
                i += 1
            line += lines[i]
        print line
        i += 1
