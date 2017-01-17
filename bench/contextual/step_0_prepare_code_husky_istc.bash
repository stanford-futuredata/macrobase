#!/bin/bash

rsync -r /Users/xuchu/Documents/Github/macrobase/src/ x4chu@husky-big.cs.uwaterloo.ca:/home/x4chu/ContextualOutliers/macrobase/src/
echo "Start compiling on husky-big"
ssh -t x4chu@husky-big.cs.uwaterloo.ca  "cd /home/x4chu/ContextualOutliers/macrobase; mvn package" 
echo "Finish compiling on husky-big"



rsync -r /Users/xuchu/Documents/Github/macrobase/src/ x4chu@istc3.csail.mit.edu:/data/x4chu/macrobase/src/
echo "Start compiling on istc3"
ssh -t x4chu@istc3.csail.mit.edu  "cd /data/x4chu/macrobase ; mvn package" 
echo "Finish compiling on istc3"



