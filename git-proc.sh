#!/bin/bash

ADDITIONAL_COMMENT="$1"
git add .
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
if [ -n "$ADDITIONAL_COMMENT" ]; then
    git commit -m "$TIMESTAMP" -m "Updated: $ADDITIONAL_COMMENT"
else
    git commit -m "$TIMESTAMP" -m "Updated"
fi
git push