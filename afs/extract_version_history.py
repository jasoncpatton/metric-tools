import sys
import logging
import argparse
import subprocess
import shlex
import shutil
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

OUTDIR = Path.cwd()

def get_latest_version_history_files(doc_dir):
    devel_files = doc_dir.glob("development-release-series-*.rst")
    stable_files = doc_dir.glob("stable-release-series-*.rst")

    devel_versions = [f.stem.split("-")[-1] for f in devel_files]
    stable_versions = [f.stem.split("-")[-1] for f in stable_files]

    # Assume for now that bigger numbers are newer...
    devel_versions.sort(key=int, reverse=True)
    stable_versions.sort(key=int, reverse=True)

    latest_devel_file = doc_dir / f"development-release-series-{devel_versions[0]}.rst"
    latest_stable_file = doc_dir / f"stable-release-series-{stable_versions[0]}.rst"

    return [latest_devel_file, latest_stable_file]

def get_version_history_files(doc_dir, versions):
    version_history_files = []
    for version in versions:
        major_ver_nodot = "".join(version.split(".")[:2])
        glob = f"*-release-series-{major_ver_nodot}.rst"
        matched_files = list(doc_dir.glob(glob))
        if len(matched_files) == 0:
            logging.error(f"Found no version history file for {version} (matching: {glob})")
            logging.error(f"Skipping version {version}")
            continue
        elif len(matched_files) > 1:
            logging.error(f"Found multiple version history files for {version} (matching: {glob})")
            logging.error(f"Skipping version {version}")
            continue
        else:
            version_history_files += matched_files
    return version_history_files

def read_docs(args):
    doc_dir = args.repo_dir / "docs" / "version-history"

    # Get a list of history files to parse
    if args.versions is None:
        version_history_files = get_latest_version_history_files(doc_dir)
    else:
        version_history_files = get_version_history_files(doc_dir, args.versions)
    if len(version_history_files) == 0:
        logging.error("Could not find any version history files!")
        sys.exit(1)

    # data is two level defaultdict with int data:
    # 1. Version tuple
    # 2. Section (release notes, new features, bugs fixed)
    data = defaultdict(lambda: defaultdict(int))
    doc_type = {
        "release notes:": "notes",
        "new features:": "features",
        "bugs fixed:": "bugfixes",
    }

    # Make a list of version tuples from requested versions (if they exist)
    store_versions = None
    if args.versions is not None:
        store_versions = [tuple([int(x) for x in v.split(".")]) for v in args.versions]

    for version_history_file in version_history_files:
        with open(version_history_file) as f:
            n = 0
            version = None
            section = None
            for line in f:
                n += 1

                # Skip empty lines
                if line.rstrip() == "":
                    continue

                # Skip until we get a version
                if line.startswith("Version "):
                    version = tuple([int(x) for x in line.rstrip().split()[1].split(".")])
                    logging.info(f"Got {line.rstrip()} {version}")
                    section = None
                    continue
                elif version is None:
                    continue

                # Skip versions that aren't requested to be stored
                if store_versions is not None and version not in store_versions:
                    continue

                # Skip until we get a section
                if line.rstrip().lower() in doc_type:
                    section = doc_type[line.rstrip().lower()]
                    logging.info(f"Got section {line.rstrip()}")
                    continue
                elif section is None:
                    continue

                # Count all entries
                if line.startswith("- "):
                    if line.rstrip() == "- None.":
                        continue
                    data[version][section] += 1

        logging.info(f"Read {n} lines from {version_history_file}")

    return data

def write_csv(data, args):
    versions = list(data.keys())
    versions.sort(reverse=True)

    # Get version strings
    version_strs = [".".join([str(x) for x in v]) for v in versions]

    cols = ["bugfixes", "features"]

    with open(args.outdir / "documented_changes_by_version.csv", "w") as f:
        f.write(f"version,{','.join(cols)}\n")
        for version, version_str in zip(versions, version_strs):
            f.write(f"{version_str},{','.join([str(data[version][col]) for col in cols])}\n")

    with open(args.outdir / "version_history_data.txt", "w") as f:
        f.write(f"As of {datetime.now()}:\n")
        for version, version_str in zip(versions, version_strs):
            f.write(f"Version {version_str} had\n")
            f.write(f"\t{data[version]['features']} documented enhancements and\n")
            f.write(f"\t{data[version]['bugfixes']} documented bug fixes.\n")

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", default=OUTDIR, metavar="PATH", type=Path, help="Output directory, defaults to CWD (%(default)s)")
    parser.add_argument("--log_level", metavar="LEVEL", default="WARNING", help="Log level, defaults to %(default)s")
    parser.add_argument("--version", metavar="VERSION", action="append", dest="versions",
        help=("Version to output stats for, can be specified multiple times. "
            "Defaults to outputting all versions in latest stable and devel series if not specified"))
    parser.add_argument("--repo_dir", metavar="PATH", type=Path, required=True,
        help="Path to existing CONDOR_SRC repository. Must be updated and set to correct branch.")
    args = parser.parse_args()
    return args

def main():
    args = parse_args()
    logging.getLogger().setLevel(getattr(logging, args.log_level.upper(), logging.WARNING))
    data = read_docs(args)
    write_csv(data, args)

if __name__ == "__main__":
    main()
