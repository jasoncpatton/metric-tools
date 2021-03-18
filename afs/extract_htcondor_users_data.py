import logging
import re
import argparse
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.WARNING)

ARCHIVE_DIR = Path("/p/list-archives/htcondor-users")

OUTDIR = Path.cwd()
START_TS = int(time.time()) - 3600*24*7
END_TS = int(time.time())

def is_staff(addr):
    addr = addr.lower()

    # Crudely assume anyone from cs.wisc.edu is staff
    if addr[-12:] == "@cs.wisc.edu":
        return True
    else: # Handle other known cases
        others = {
            "dan@help.wisc.edu",
            "jcpatton@wisc.edu",
            "lmichael@wisc.edu",
            "ckoch5@wisc.edu",
            "karpel@wisc.edu",
            "egrasmick@wisc.edu",
            "moate@gmail.com",
        }
        return addr in others

def is_edu(addr):
    addr = addr.lower()
    if addr[-4:] == ".edu":
        return True
    else: # Handle other known cases
        others = {
            "moate@gmail.com",
        }
        return addr in others

def is_ac(addr):
    addr = addr.lower()
    if ".ac." in addr.split("@")[-1]:
        return True
    else: # Handle other known cases
        others = {}
        return addr in others

def get_data(args):
    # data is two level defaultdict with int data:
    # 1. date (datetime.datetime)
    # 2. sender origin (all, staff, edu, ac)
    data = defaultdict(lambda: defaultdict(int))

    origins = {
        "total": lambda x: True,
        "staff": is_staff,
        "edu"  : is_edu,
        "ac"   : is_ac,
    }

    start_dt = datetime.fromtimestamp(args.start)
    end_dt = datetime.fromtimestamp(args.end)

    start_month = datetime(start_dt.year, start_dt.month, 1)
    end_month = datetime(end_dt.year, end_dt.month, 1)

    files = 0
    total = 0
    for mbox_file in ARCHIVE_DIR.glob("*.txt"):

        # only read files that could contain emails in the date range
        try:
            mbox_month = datetime.strptime(mbox_file.stem, "%Y-%B")
        except ValueError:
            logging.info(f"Skipping {mbox_file.name}")
            continue
        if mbox_month < start_month or mbox_month > end_month:
            logging.info(f"Skipping {mbox_file.name}, out of date range")
            continue


        logging.info(f"Opening {mbox_file.name}")
        with open(mbox_file, "rb") as f:
            files += 1
            n = 0
            for line in f:
                n += 1

                #  0    1             2   3   4  5        6
                # "From user@foo.edu  Mon Dec 31 23:59:59 1999"
                #  0             1            2           3
                #  123456   78  90  1 234567890 12 34567890123
                if len(line) < 33 or line[:5] != b"From ":
                    continue
                tokens = line.decode("utf-8").rstrip().split()

                # Auto-responders, booooo
                if tokens[1] == "MAILER-DAEMON":
                    logging.debug(f"Skipping MAILER-DAEMON: {line.rstrip()}")
                    continue

                # Text that starts with "From " should be escaped,
                # but let's add a few checks just in case
                if "@" not in tokens[1] or len(tokens) != 7:
                    logging.warning(f"Skipped potential From line at {mbox_file.name}:{n}: {line.rstrip()}")
                    continue

                logging.debug(f"Parsing {line.rstrip()}")
                addr = tokens[1]
                date_str = " ".join(tokens[3:]) # skip day of week
                dt = datetime.strptime(date_str, "%b %d %H:%M:%S %Y")

                if dt < start_dt or dt > end_dt:
                    logging.debug(f"Skipped {line.rstrip()}: out of date range")
                    continue

                total += 1
                date = datetime(dt.year, dt.month, dt.day) # reduce date to day
                for origin, test in origins.items():
                    data[date][origin] += int(test(addr))
                    if origin != "total" and test(addr):
                        logging.debug(f"Counted {addr} as {origin}")

    logging.info(f"Opened {files} mbox files")
    logging.info(f"Counted {total} emails")
    return data

def write_csv(data, args):
    dates = list(data.keys())
    dates.sort()

    cols = ["edu", "ac", "staff", "total"]

    total = 0
    staff = 0
    with open(args.outdir / "htcondor-users_emails_by_origin.csv", "w") as f:
        f.write(f"date,{','.join(cols)}\n")
        for date in dates:
            f.write(f"{date.strftime('%Y-%m-%d')},{','.join([str(data[date][col]) for col in cols])}\n")
            total += data[date]["total"]
            staff += data[date]["staff"]

    with open(args.outdir / "htcondor-users_data.txt", "w") as f:
        f.write(f"From {datetime.fromtimestamp(args.start)} to {datetime.fromtimestamp(args.end)}:\n")
        f.write(f"Our community-support email list htcondor-users saw {total} messages,\n")
        f.write(f"of which PATh staff sent {staff} emails responding to user questions.\n")

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
    data = get_data(args)
    write_csv(data, args)

if __name__ == "__main__":
    main()
