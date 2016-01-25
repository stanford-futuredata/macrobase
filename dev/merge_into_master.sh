#!/bin/sh

if [ $(expr $(git status --porcelain | wc -l)) != 0 ]; then
  echo "Please make sure your repository is clean before proceeding."
  exit 1
fi

working_branch=$(git rev-parse --abbrev-ref HEAD)

if [ "$working_branch" = "master" ]; then
  echo "You have to on a branch other than master to proceed."
  exit 1
fi

set -e
set -x

git fetch origin
git rebase -i origin/master
git checkout master
git merge --ff-only ${working_branch}
git push origin master
