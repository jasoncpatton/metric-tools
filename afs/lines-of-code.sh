#!/bin/sh

if [ "$#" -ne "1" ]; then
    echo "$0 requires a path to a source code directory as its lone argument" 1>&2
    exit 1
fi

SOURCE_DIR="$1"

for dir in $(find "$SOURCE_DIR" -maxdepth 1 -type d -not -path '*.git'); do
    dirname="$(basename $dir)"
    if [ "$SOURCE_DIR" = "$dir" ]; then
	loc=$(find "$dir" -maxdepth 1 -type f -exec \
	    wc -l {} \; | awk '{total += $1} END {print total}')
    else
	loc=$(find "$dir" -type f -exec \
	    wc -l {} \; | awk '{total += $1} END {print total}')
    fi
    echo "$dirname: $loc"
done
