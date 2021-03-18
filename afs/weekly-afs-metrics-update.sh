#!/bin/bash
set -e

#####
# The following should be set in the config file ~/.weekly-afs-metrics.config.sh
# RT_USER              - A readonly RT service account username
# RT_PASSWORD_FILE     - Path to a file containing the password for RT_USER
# RT_API_URI           - The base URI of the RT REST API (ends in REST/1.0)
# CONDOR_SRC_REPO_DIR  - Path to up-to-date copy of the CONDOR_SRC repo pointed
#     at the master branch. **The onus is on you to keep this repo up to date!**
CONFIG_FILE="$HOME/.weekly-afs-metrics-config.sh"
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Could not find $CONFIG_FILE, exiting..." 1>&2
    exit 1
fi
. $CONFIG_FILE
if [ -z "$RT_USER" ] || \
   [ -z "$RT_PASSWORD_FILE" ] || \
   [ -z "$RT_API_URI" ] || \
   [ -z "$CONDOR_SRC_REPO_DIR" ]; then
    echo "One or more required variables are missing from $CONFIG_FILE" 1>&2
    exit 1
fi
#####

cd "$(dirname "$0")"

# venv setup (requests is needed for RT REST API queries)

python3 -m venv venv 1>&2
. venv/bin/activate
pip install -r requirements.txt 1>&2

# run metrics

OUTDIR="data-$(date +%F)"
START_DATE=$(date -d "-1 week 12 AM" +%s)
END_DATE=$(date -d "today 12 AM" +%s)

if [ ! -d "$OUTDIR" ]; then
   mkdir -p "$OUTDIR"
fi

echo "Running extract_download_data.py" 1>&2
python extract_download_data.py \
       --outdir="$OUTDIR" --start=$START_DATE --end=$END_DATE

echo "Running extract_git_data.py" 1>&2
python extract_git_data.py \
       --outdir="$OUTDIR" --start=$START_DATE --end=$END_DATE \
       --repo_dir="$CONDOR_SRC_REPO_DIR"

echo "Running extract_htcondor_users_data.py" 1>&2
python extract_htcondor_users_data.py \
       --outdir="$OUTDIR" --start=$START_DATE --end=$END_DATE

echo "Running extract_rt_stats.py" 1>&2
python extract_rt_stats.py \
       --outdir="$OUTDIR" --start=$START_DATE --end=$END_DATE \
       --queue="htcondor-admin" \
       --api_uri="$RT_API_URI" --username="$RT_USER" --password_file="$RT_PASSWORD_FILE"

echo "Running extract_version_history.py" 1>&2
python extract_version_history.py \
       --outdir="$OUTDIR" \
       --repo_dir="$CONDOR_SRC_REPO_DIR"
echo 1>&2

# print metrics

echo "==========================="
echo "=== Download statistics ==="
echo "==========================="
echo
cat "$OUTDIR/download_data.txt"
echo
echo "==========================="
echo "=== Codebase statistics ==="
echo "==========================="
echo
cat "$OUTDIR/git_data.txt"
echo
echo "================================="
echo "=== HTCondor-Users statistics ==="
echo "================================="
echo
cat "$OUTDIR/htcondor-users_data.txt"
echo
echo "================================="
echo "=== HTCondor-Admin statistics ==="
echo "================================="
echo
cat "$OUTDIR/rt_data_htcondor-admin.txt"
echo
echo "=================================="
echo "=== Version History statistics ==="
echo "=================================="
echo
cat "$OUTDIR/version_history_data.txt"
