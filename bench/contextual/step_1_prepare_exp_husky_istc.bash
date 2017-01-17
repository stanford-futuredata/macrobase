#!/bin/bash


ssh -t x4chu@husky-big.cs.uwaterloo.ca  "cd /home/x4chu/ContextualOutliers/macrobase/bench/contextual ; rm -rf *" 
rsync  -r /Users/xuchu/Documents/Github/macrobase/bench/contextual/ x4chu@husky-big.cs.uwaterloo.ca:/home/x4chu/ContextualOutliers/macrobase/bench/contextual/


ssh -t x4chu@istc3.csail.mit.edu  "cd /data/x4chu/macrobase/bench/contextual ; rm -rf *" 
rsync  -r /Users/xuchu/Documents/Github/macrobase/bench/contextual/ x4chu@istc3.csail.mit.edu:/data/x4chu/macrobase/bench/contextual/
