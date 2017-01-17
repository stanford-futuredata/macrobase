#!/bin/bash


echo "Start fetching result on husky"
ssh -t x4chu@husky-big.cs.uwaterloo.ca  "cd /home/x4chu/ContextualOutliers/macrobase/bench/contextual ; python contextual_plots.py" 
scp x4chu@husky-big.cs.uwaterloo.ca:/home/x4chu/ContextualOutliers/macrobase/bench/contextual/*.csv /Users/xuchu/Documents/Github/macrobase/bench/contextual/SIGMOD_Exp/
echo "Done fetching result on husky"


echo "Start fetching result on istc"
ssh -t x4chu@istc3.csail.mit.edu  "cd /data/x4chu/macrobase/bench/contextual ; python contextual_plots.py" 
scp x4chu@istc3.csail.mit.edu:/data/x4chu/macrobase/bench/contextual/*.csv /Users/xuchu/Documents/Github/macrobase/bench/contextual/SIGMOD_Exp/
echo "Done fetching result on istc"
