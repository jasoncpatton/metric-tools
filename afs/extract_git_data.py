import sys
import logging
import argparse
import subprocess
import shlex
import shutil
import time
import re
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

OUTDIR = Path.cwd()
START_TS = int(time.time()) - 3600*24*7
END_TS = int(time.time())

def get_git_logs(args):
    after = datetime.fromtimestamp(args.start).strftime("%B.%d.%Y")
    before = datetime.fromtimestamp(args.end).strftime("%B.%d.%Y")
    cmd = f'git --git-dir="{args.repo_dir}/.git" log --all --numstat --pretty="%ae" --after {{{after}}} --before {{{before}}}'
    logging.info(f"Running {cmd}")
    result = subprocess.run(shlex.split(cmd), stdout=subprocess.PIPE, timeout=180)
    try:
        result.check_returncode()
    except subprocess.CalledProcessError:
        logging.exception(f"Error while trying to run {cmd}")
        sys.exit(1)
    return result.stdout.decode("utf-8")

def get_total_loc(args):
    cmd = f'./lines-of-code.sh "{args.repo_dir}"'
    logging.info(f"Running {cmd}")
    result = subprocess.run(shlex.split(cmd), stdout=subprocess.PIPE, timeout=180)
    try:
        result.check_returncode()
    except subprocess.CalledProcessError:
        logging.exception(f"Error while trying to run {cmd}")
        sys.exit(1)

    loc = 0
    for line in result.stdout.decode("utf-8").split("\n"):
        if line.split(":")[0] in ["src", "bindings"]:
            # only count files in "src/" or "bindings"
            loc += int(line.split()[1])

    return loc

def parse_logs_and_write_output(git_logs, total_loc, args):
    commits = 0
    authors = set()
    lines_added = 0
    lines_removed = 0
    files = 0
    unique_files = set()

    previous_line = ""
    new_commit = True
    for line in git_logs.split("\n"):
        cols = line.split()
        if len(cols) == 0 or cols[0] == "":
            pass
        elif len(cols) == 1:
            # author line(s)
            if new_commit:
                commits += 1
                new_commit = False
            # don't add automated github authors
            if not ("github" in cols[0]):
                authors.add(cols[0])
        else:
            # should be a file line, try parsing it
            new_commit = True
            try:
                if cols[2].split("/")[0] in ["src", "bindings"]:
                    # only count files in "src/" or "bindings/"
                    lines_added += int(cols[0])
                    lines_removed += int(cols[1])
                    unique_files.add(cols[2])
                    files += 1
            except ValueError:
                pass

        previous_line = line

    with open(args.outdir / "git_data.txt", "w") as f:
        f.write(f"From {datetime.fromtimestamp(args.start)} to {datetime.fromtimestamp(args.end)}:\n")
        f.write(f"{len(authors)} contributors\n")
        f.write(f"made {commits} source code commits,\n")
        f.write(f"consisting of {files} ({len(unique_files)} unique) file modifications\n")
        f.write(f"adding {lines_added} lines of code (LOC)\n")
        f.write(f"and removing {lines_removed} LOC.\n")
        f.write(f"Total LOC stands at {total_loc}.\n")

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", default=OUTDIR, metavar="PATH", type=Path, help="Output directory, defaults to CWD (%(default)s)")
    parser.add_argument("--start", default=START_TS, metavar="TIMESTAMP", type=int, help="Starting timestamp, defaults to one week ago (%(default)d)")
    parser.add_argument("--end", default=END_TS, metavar="TIMESTAMP", type=int, help="Ending timestamp, defaults to now (%(default)d)")
    parser.add_argument("--log_level", metavar="LEVEL", default="WARNING", help="Log level, defaults to %(default)s")
    parser.add_argument("--repo_dir", metavar="PATH", type=Path, required=True,
        help="Path to existing CONDOR_SRC repository. Must be updated and set to correct branch.")
    args = parser.parse_args()
    return args

def main():
    args = parse_args()
    logging.getLogger().setLevel(getattr(logging, args.log_level.upper(), logging.WARNING))
    git_logs = get_git_logs(args)
    total_loc = get_total_loc(args)
    parse_logs_and_write_output(git_logs, total_loc, args)

if __name__ == "__main__":
    main()
