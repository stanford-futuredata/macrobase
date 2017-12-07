#!/usr/bin/env bash
curl -H "Accept: application/json" -H "Content-type: application/json" \
-X POST -d "@core/demo/batch.json" 0.0.0.0:4567/query; echo