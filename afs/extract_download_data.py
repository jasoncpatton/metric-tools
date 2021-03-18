import logging
import re
import argparse
import time
from collections import OrderedDict, defaultdict
from pathlib import Path
from datetime import datetime

logging.basicConfig(level=logging.WARNING)

# important paths
DL_LOGS_PATH = Path("/p/condor/public/license")
NATIVE_FILE = DL_LOGS_PATH / "native-packages.mbox"

# defaults
OUTDIR = Path.cwd()
START_TS = int(time.time()) - 3600*24*7
END_TS = int(time.time())

[WINDOWS, LINUX, MACOS, OTHER] = ["Windows", "Linux", "MacOS", "Other"]
OS_MAP = [
    ("winnt",   WINDOWS),
    ("windows", WINDOWS),
    ("winn50",  WINDOWS),
    (r"\.deb$", LINUX),
    ("_deb_",   LINUX),
    (r"\.rpm$", LINUX),
    ("linux",   LINUX),
    ("rhel",    LINUX),
    ("rhap",    LINUX),
    ("rhas",    LINUX),
    ("fedora",  LINUX),
    ("redhat",  LINUX),
    ("centos",  LINUX),
    ("ubuntu",  LINUX),
    ("debian",  LINUX),
    (r"mac ?os ?x?", MACOS),
    ("irix",    OTHER),
    ("dux",     OTHER),
    ("solaris", OTHER),
    ("sun4u_sol_",  OTHER),
    ("aix",     OTHER),
    ("hpux",    OTHER),
    ("bsd",     OTHER),
    ("vax-openvms", OTHER),
    ("ydl3",    LINUX),
    ("yd5",     LINUX),
    ("all",     "All"),
    ("_sl_",    LINUX),
    ("sles",    LINUX),
    ("_sol_",   LINUX),
    (r"\.orig\.tar\.[gx]z", LINUX),
    (r"-src\.tar\.[gbx]z2?", "All"),
]
os_map_temp = OrderedDict()
for (os_re, os_str) in OS_MAP:
    os_map_temp[re.compile(os_re, re.IGNORECASE)] = os_str
OS_MAP = os_map_temp

[X86, X86_64, OTHER] = ["x86", "x86-64", "Other"]
ARCH_MAP = [
    (r"linux-x86-g?libc", X86),
    ("linux-x86.tar",     X86),
    ("linux-x86-redhat",  X86),
    ("linux-x86-rhel",    X86),
    ("linux-x86-debian",  X86),
    ("winnt-x86-5.0",     X86),
    ("amd64",  X86_64),
    ("x86_64", X86_64),
    ("i386",   X86),
    ("i686",   X86),
    ("x64",    X86_64),
    ("ia64",   OTHER),
    ("ppc64",  OTHER),
    ("sun4u",  OTHER),
    ("sparc",  OTHER),
    ("alpha",  OTHER),
    ("-ppc-",  OTHER),
    ("ppc_aix", OTHER),
    ("hppar",  OTHER),
    ("sgi",    OTHER),
    ("powerpc", OTHER),
    ("vax-openvms", OTHER),
    (r"aix.*aix", OTHER),
    ("x86-dynamic", X86),
    (r"x86\.exe",   X86),
    (r"x86\.zip",   X86),
    (r"x86\.msi",   X86),
    (r"x86\.tar",   X86),
    ("-all-all", "All"),
    ("-x86_rhap",   X86),
    ("-x86_rhas",   X86),
    ("-x86_redhat", X86),
    ("-x86_deb",    X86),
    ("-x86_sl",     X86),
    ("-x86_freebsd", X86),
    ("-x86_macos",  X86),
    ("-x86_centos", X86),
    (r"\.src\.",  "All"),
    (r"^all$",    "All"),
    (r"\.(orig|debian)\.tar\.[gx]z", "All"),
    (r"-src\.tar\.[gbx]z2?", "All"),
]
arch_map_temp = OrderedDict()
for (arch_re, arch_str) in ARCH_MAP:
    arch_map_temp[re.compile(arch_re, re.IGNORECASE)] = arch_str
ARCH_MAP = arch_map_temp

def get_os(filename):
    for os_re, os_str in OS_MAP.items():
        match = os_re.search(filename)
        if match:
            return os_str
    return "Unknown"

def get_arch(filename):
    for arch_re, arch_str in ARCH_MAP.items():
        match = arch_re.search(filename)
        if match:
            return arch_str
    return "Unknown"

def get_log_files(log_path):
    sendfile_re = re.compile(r"sendfile-v\d+\.\d+")
    log_files = []
    for log_file in log_path.glob("sendfile-v*"):
        if sendfile_re.match(log_file.name):
            log_files.append(log_file)
    return log_files

def read_log_file(log_file, data_out, args):
    # 1551455077	END	condor-8.9.0-462330-Windows-x64.zip	...
    re_binary = re.compile(r"h?t?condor-([\d.]+\d)(?:_preview)?[-.](.*)")
    re_source = re.compile(r"h?t?condor_src-([\d.]+\d)(?:_preview)?[-.](.*)")
    re_binary_deb = re.compile(r"h?t?condor_([\d.]+\d)-(.*\.deb)")
    headers = ["timestamp", "status", "filename"]

    logging.info(f"Opening {log_file.name}")
    with open(log_file) as f:
        n = 0
        stored = 0
        for line in f:
            n += 1
            cols = line.rstrip().split()
            data = dict(zip(headers, cols[:len(headers)]))
            logging.debug(f"Got line {data}")

            if int(data["timestamp"]) < args.start or int(data["timestamp"]) > args.end:
                logging.debug(f"Skipping date {data['timestamp']}")
                continue

            if data["status"] != "END":
                logging.debug(f"Skipping status {data['status']}")
                continue
            if "sha256sum" in data["filename"]:
                logging.debug(f"Skipping file {data['filename']}")
                continue
            if data["filename"].startswith("condordebugsyms"):
                logging.debug(f"Skipping file {data['filename']}")
                continue
            if data["filename"].startswith("condor-drone-"):
                logging.debug(f"Skipping file {data['filename']}")
                continue

            if data["filename"].startswith("condor-") or data["filename"].startswith("htcondor-"):
                contents = "binary"
                match = re_binary.match(data["filename"])
                logging.debug(f"{data['filename']} is a binary file")
            elif data["filename"].startswith("condor_src-") or data["filename"].startswith("htcondor_src-"):
                contents = "source"
                match = re_source.match(data["filename"])
                logging.debug(f"{data['filename']} is a source file")
            elif re_binary_deb.match(data["filename"]):
                contents = "binary"
                match = re_binary_deb.match(data["filename"])
                logging.debug(f"{data['filename']} is a binary deb file")
            else:
                logging.error(f"Unparseable filename at {log_file.name}:{n}: {data['filename']}")
                logging.debug(f"Skipping file {data['filename']}")
                continue
            if match is None:
                logging.error(f"Unparseable {contents} filename at {log_file.name}:{n}: {data['filename']}")
                logging.debug(f"Skipping file {data['filename']}")
                continue

            version = match.group(1)
            logging.debug(f"Got version {version}")

            version_major = ".".join(version.split(".")[0:2])
            logging.debug(f"Got major version {version_major}")
            if "." not in version_major or len(version_major) < 3:
                logging.error(f"Weird version {version} at {log_file.name}:{n}: {data['filename']}")
                logging.debug(f"Skipping file {data['filename']}")
                continue

            os = get_os(data["filename"])
            logging.debug(f"Got OS {os}")
            if os == "Unknown":
                logging.warning(f"Dubious OS found at {log_file.name}:{n}: {data['filename']}")

            arch = get_arch(data["filename"])
            logging.debug(f"Got Arch {arch}")
            if arch == "Unknown":
                logging.warning(f"Dubious Arch found at {log_file.name}:{n}: {data['filename']}")

            # Get a datetime with only the date
            dt = datetime.fromtimestamp(int(data["timestamp"]))
            date = datetime(dt.year, dt.month, dt.day)

            # Store data
            data_out[date]["version"][version] += 1
            data_out[date]["version_major"][version_major] += 1
            data_out[date]["osarch"][f"{os}/{arch}"] += 1
            data_out[date]["os"][os] += 1
            stored += 1

    logging.info(f"Read {n} lines from {log_file.name}")
    logging.info(f"Stored {stored} lines from {log_file.name}")
    return

def read_native_file(native_file, args):
    # data is three level defaultdict with int data:
    # 1. date (datetime.datetime)
    # 2. bin type ("version", "version_major", "osarch", "os")
    # 3. bin name ("8.9.12", "8.9", "Linux/x86_64", "Linux")
    # e.g. 42 downloads on 2021-03-10:
    # data[datetime.datetime(2021, 03, 10)]["version"]["8.9.12"] = 42
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    re_rpm = re.compile(r"/.*/h?t?condor-([\d.]+\d)-.*\.(x86_64|i386|i686|ia64)\.rpm \((\d+) hits?\)")
    re_src_rpm = re.compile(r"/.*/h?t?condor-([\d.]+\d)-(.*)\.src\.rpm \((\d+) hits?\)")

    re_deb = re.compile(r"/.*/h?t?condor[_-]([\d.]+\d)-.*_(amd64|i386|all)\.deb \((\d+) hits?\)")
    re_src_deb = re.compile(r"/.*/h?t?condor[_-]([\d.]+\d)-.*\.(orig|debian)\.tar\.[xg]z \((\d+) hits?\)")

    re_tarball = re.compile(r"/.*/tarball/.*/(h?t?condor[-_][\d.]+\d[-.][^/ ]+) \((\d+) hits?\)")
    re_src_tarball = re.compile(r"/.*/tarball/.*/(h?t?condor_src[-_][\d.]+\d[-.][^/ ]+) \((\d+) hits?\)")
    re_binary = re.compile(r"h?t?condor-([\d.]+\d)(?:_preview)?[-.](.*)")
    re_source = re.compile(r"h?t?condor_src-([\d.]+\d)(?:_preview)?[-.](.*)")
    re_binary_deb = re.compile(r"h?t?condor_([\d.]+\d)-(.*\.deb)")
    re_source_deb = re.compile(r"h?t?condor_([\d.]+\d)\..*(orig|debian)\.tar\.[xg]z")

    date = None
    in_header = True
    with open(native_file) as f:
        n = 0
        emails = 0
        paths = 0
        stored = 0
        for line in f:
            n += 1
            if len(line) >= 5 and line[:5] == "From ":
                logging.info(f"Got new email {line.rstrip()}")
                emails += 1
                date = None
                date_warning = False
                in_header = True
                continue
            elif "From " in line:
                # If "From " occurs elsewhere, it could be a sign of interleaved reports
                logging.error(f"Possible corruption at {native_file.name}:{n}")
                continue
            elif len(line) >= 5 and line[:5] == "Date:":
                if (date is not None) or not in_header:
                    logging.warning(f'Got unexpected "Date:" header at {native_file.name}:{n}')
                try:
                    # Date: Sat, 06 Mar 2021 02:30:32 -0600 (CST)
                    date = datetime.strptime(line.split(":")[1].strip(), "%a, %d %b %Y %H")
                except ValueError:
                    logging.error(f"Could not parse date at {native_file.name}:{n}: {line.rstrip()}")
                date = datetime(date.year, date.month, date.day)
                continue
            elif in_header and line.strip() == "":
                if date is None:
                    logging.warning(f"Did not find date in header at {native_file.name}:{n}")
                in_header = False
                continue
            elif in_header:
                continue
            elif date.timestamp() < args.start or date.timestamp() > args.end:
                if not date_warning:
                    logging.debug(f"Skipping email due to date")
                    date_warning = True
                continue

            # Only consider lines that are paths
            if line.startswith('/') and line.split('/')[1] in ["s", "condor", "htcondor"]:
                paths += 1
                logging.debug(f"Attempting match against {line.rstrip()}")
            else:
                continue

            # Try matching binary packages first
            if re_rpm.match(line.rstrip()) or re_deb.match(line.rstrip()):
                match = re_rpm.match(line.rstrip()) or re_deb.match(line.rstrip())
                logging.debug(f"Got binary package match: {match.groups()}")

                (version, arch_str, hits) = match.groups()
                version_major = ".".join(version.split(".")[0:2])

                os = "Linux"
                arch = get_arch(arch_str)
                if arch == "Unknown":
                    logging.warning(f"Dubious Arch found at {native_file.name}:{n}: {line.rstrip()}")

            # Try matching source packages second
            elif re_src_rpm.match(line.rstrip()) or re_src_deb.match(line.rstrip()):
                match = re_src_rpm.match(line.rstrip()) or re_src_deb.match(line.rstrip())
                logging.debug(f"Got source package match: {match.groups()}")

                (version, dummy, hits) = match.groups()
                version_major = ".".join(version.split(".")[0:2])

                os = "Linux"
                arch = "All"

            # Try matching tarballs last
            elif re_tarball.match(line.rstrip()) or re_src_tarball.match(line.rstrip()):
                match = re_tarball.match(line.rstrip()) or re_src_tarball.match(line.rstrip())
                logging.debug(f"Got tarball match: {match.groups()}")

                (filename, hits) = match.groups()

                if "sha256sum" in filename:
                    logging.debug(f"Skipping file {filename}")
                    continue

                if filename.startswith("condor-") or filename.startswith("htcondor-"):
                    contents = "binary"
                    match = re_binary.match(filename)
                elif filename.startswith("condor_src-") or filename.startswith("htcondor_src-"):
                    contents = "source"
                    match = re_source.match(filename)
                elif re_binary_deb.match(filename):
                    contents = "binary"
                    match = re_binary_deb.match(filename)
                elif re_source_deb.match(filename):
                    contents = "source"
                    match = re_source_deb.match(filename)
                else:
                    logging.error(f"Unparseable filename at {native_file.name}:{n}: {line.rstrip()}")
                    continue
                if match is None:
                    logging.error(f"Unparseable {contents} filename at {native_file.name}:{n}: {line.rstrip()}")
                    continue

                version = match.group(1)
                version_major = ".".join(version.split(".")[0:2])

                os = get_os(filename)
                logging.debug(f"Got OS {os}")
                if os == "Unknown":
                    logging.warning(f"Dubious OS found at {native_file.name}:{n}: {line.rstrip()}")

                arch = get_arch(filename)
                logging.debug(f"Got Arch {arch}")
                if arch == "Unknown":
                    logging.warning(f"Dubious Arch found at {native_file.name}:{n}: {line.rstrip()}")

            # Give up
            else:
                logging.debug(f"Did not find match for {line.rstrip()}")
                continue

            data[date]["version"][version] += int(hits)
            data[date]["version_major"][version_major] += int(hits)
            data[date]["osarch"][f"{os}/{arch}"] += int(hits)
            data[date]["os"][os] += int(hits)
            stored += 1

    logging.info(f"Read {n} lines from {native_file.name}")
    logging.info(f"Read {emails} emails from {native_file.name}")
    logging.info(f"Read {paths} filepaths from {native_file.name}")
    logging.info(f"Stored {stored} lines from {native_file.name}")

    return data

def get_keys(data):
    versions_major = set()
    versions = set()
    oss = set()
    osarchs = set()

    for date in data.keys():
        versions_major.update(list(data[date]["version_major"].keys()))
        versions.update(list(data[date]["version"].keys()))
        oss.update(list(data[date]["os"].keys()))
        osarchs.update(list(data[date]["osarch"].keys()))

    versions_major = list(versions_major)
    versions = list(versions)
    oss = list(oss)
    osarchs = list(osarchs)

    versions_major.sort(key=lambda v: list(map(int, v.split("."))))
    versions.sort(key=lambda v: list(map(int, v.split("."))))
    oss.sort()
    osarchs.sort()

    return {"version_major": versions_major, "version": versions, "os": oss, "osarch": osarchs}

def write_csvs(data, args):
    dates = list(data.keys())
    dates.sort()

    keys = get_keys(data)

    fs = {
        # non-cumulative
        "version_major": open(args.outdir / "downloads_by_version_major.csv", "w"),
        "version":       open(args.outdir / "downloads_by_version.csv", "w"),
        "os":            open(args.outdir / "downloads_by_os.csv", "w"),
        "osarch":        open(args.outdir / "downloads_by_arch.csv", "w"),
        # cumulative
        "cum_version_major": open(args.outdir / "cumulative_downloads_by_version_major.csv", "w"),
        "cum_version":       open(args.outdir / "cumulative_downloads_by_version.csv", "w"),
        "cum_os":            open(args.outdir / "cumulative_downloads_by_os.csv", "w"),
        "cum_osarch":        open(args.outdir / "cumulative_downloads_by_arch.csv", "w"),
    }

    # write headers
    for (keyname, f) in fs.items():
        f.write(f"Date,{','.join([col for col in keys[keyname.lstrip('cum_')]])},Total\n")

    # write data
    total_total = 0
    for date in dates:
        # skip dates outside of range
        if not (date.timestamp() >= args.start and date.timestamp() <= args.end):
            continue
        for (keyname, f) in fs.items():
            f.write(f"{date.strftime('%Y-%m-%d')},")
            if keyname.startswith("cum_"):
                # accumulate and write
                for key in keys[keyname.lstrip("cum_")]:
                    data["cum"][keyname][key] += data[date][keyname.lstrip("cum_")][key]
                total = sum([data["cum"][keyname][key] for key in keys[keyname.lstrip("cum_")]])
                f.write(f"{','.join([str(data['cum'][keyname][key]) for key in keys[keyname.lstrip('cum_')]])},{total}\n")
            else:
                # write
                total = sum([data[date][keyname][key] for key in keys[keyname]])
                f.write(f"{','.join([str(data[date][keyname][key]) for key in keys[keyname]])},{total}\n")
            if keyname == "os":
                total_total += total

    with open(args.outdir / "download_data.txt", "w") as f:
        f.write(f"From {datetime.fromtimestamp(args.start)} to {datetime.fromtimestamp(args.end)}:\n")
        f.write(f"HTCondor packages, compiled tarballs, and source code tarballs\n")
        f.write(f"were downloaded {total_total} times.\n")

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", default=OUTDIR, metavar="PATH", type=Path, help="Output directory, defaults to CWD (%(default)s)")
    parser.add_argument("--start", default=START_TS, metavar="TIMESTAMP", type=int, help="Starting timestamp, defaults to one week ago (%(default)d)")
    parser.add_argument("--end", default=END_TS, metavar="TIMESTAMP", type=int, help="Ending timestamp, defaults to now (%(default)d)")
    parser.add_argument("--log_level", metavar="LEVEL", default="WARNING", help="Log level, defaults to %(default)s")
    args = parser.parse_args()
    return args

def main():
    args = parse_args()
    logging.getLogger().setLevel(getattr(logging, args.log_level.upper(), logging.WARNING))
    data = read_native_file(NATIVE_FILE, args)
    log_files = get_log_files(DL_LOGS_PATH)
    for log_file in log_files:
        read_log_file(log_file, data, args)
    write_csvs(data, args)

if __name__ == "__main__":
    main()

