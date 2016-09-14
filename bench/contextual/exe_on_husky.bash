#!/bin/bash

ssh -t x4chu@husky-big.cs.uwaterloo.ca ssh husky-01 './ContextualExecute.bash' &
ssh -t x4chu@husky-big.cs.uwaterloo.ca ssh husky-02 './ContextualExecute.bash' &
ssh -t x4chu@husky-big.cs.uwaterloo.ca ssh husky-03 './ContextualExecute.bash' &
ssh -t x4chu@husky-big.cs.uwaterloo.ca ssh husky-04 './ContextualExecute.bash' &
