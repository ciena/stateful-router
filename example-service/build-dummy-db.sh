#!/bin/bash
set -e

# project-specific information
NAME="dummy-db"
REPO=""

# gather working tree information
GIT_BRANCH="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo git-not-initialized)"
GIT_COMMIT_NUM="$(git rev-list --count HEAD 2>/dev/null || echo 0)"
GIT_COMMIT="$(git log --format='%H' -n 1 2>/dev/null || echo git-not-initialized)"
if [[ "$(git ls-files --others --modified --exclude-standard 2>/dev/null || echo git-not-initialized)" != "" ]]; then
  GIT_UNCOMMITTED_CHANGES="true"
fi

if [[ "$GIT_COMMIT" == "git-not-initialized" ]]; then
  echo "warning: git not initialized"
fi

# build docker image
docker build -t "$REPO$NAME" \
  --build-arg "GIT_BRANCH=$GIT_BRANCH" \
  --build-arg "GIT_COMMIT_NUM=$GIT_COMMIT_NUM" \
  --build-arg "GIT_COMMIT=$GIT_COMMIT" \
  --build-arg "GIT_UNCOMMITTED_CHANGES=$GIT_UNCOMMITTED_CHANGES" -f ./dummy-db/Dockerfile .

# if working tree is clean, tag based on git version information
if [[ "$GIT_UNCOMMITTED_CHANGES" != "true" ]]; then
  if [[ "$GIT_BRANCH" == "master" ]]; then
    # remove existing
    docker rmi "$REPO$NAME:0.$GIT_COMMIT_NUM" > /dev/null 2>&1 || true
    docker tag "$REPO$NAME" "$REPO$NAME:0.$GIT_COMMIT_NUM" && echo "Successfully tagged $REPO$NAME:0.$GIT_COMMIT_NUM"
  else
    docker rmi "$REPO$NAME:$(echo "$GIT_BRANCH" | sed -e 's/\//_/g')-0.$GIT_COMMIT_NUM" > /dev/null 2>&1 || true
    docker tag "$REPO$NAME" "$REPO$NAME:$(echo "$GIT_BRANCH" | sed -e 's/\//_/g')-0.$GIT_COMMIT_NUM" && echo "Successfully tagged $REPO$NAME:$(echo "$GIT_BRANCH" | sed -e 's/\//_/g')-0.$GIT_COMMIT_NUM"
  fi
fi
