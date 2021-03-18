import logging
import sys
import time
import argparse
import requests
from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(level=logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.ERROR)

OUTDIR = Path.cwd()
START_TS = int(time.time()) - 3600*24*7
END_TS = int(time.time())

def get_session(args):
    # create a session and get initial cookie
    auth_data = {
        "user": args.username,
        "pass": args.password_file.open().read().rstrip()
    }
    session = requests.Session()
    logging.info(f"Opening session with {args.api_uri} with username {args.username}")
    response = session.post(args.api_uri, params=auth_data)
    response.raise_for_status()
    return session

def parse_response(response):
    # function to parse API responses "key: value\n" into dicts
    data = {}
    for line in response.text.split("\n"):
        tokens = line.split(":")
        if len(tokens) < 2:
            continue
        (key, value) = (tokens[0], ':'.join(tokens[1:]).lstrip().rstrip())
        data[key] = value
    return data

def get_and_write_data(session, args):

    # put start and end dates in format understood by query API
    since = datetime.fromtimestamp(args.start).strftime("%Y-%m-%d %H:%M:%S")
    until = datetime.fromtimestamp(args.end).strftime("%Y-%m-%d %H:%M:%S")

    for queue in args.queues:

        # initialize queue stats
        tickets_created = 0
        tickets_assigned = 0
        emails_recv = 0
        emails_sent = 0
        response_times = []

        # search queue for all tickets from the "since" date
        query = f"(Updated >= '{since}' AND Created <= '{until}') AND Queue = '{queue}'"
        logging.debug(f'Querying /search/ticket for "{query}"')
        response = session.get(args.api_uri + "/search/ticket", params={"query": query})
        response.raise_for_status()
        tickets = parse_response(response)
        logging.info(f"Got {len(tickets)} tickets")

        for ticket in tickets.keys():

            # initialize per-ticket placeholders
            created = False
            assigned = False
            last_recv = None

            logging.debug(f"Getting historical events for #{ticket}")
            response = session.get(args.api_uri + f"/ticket/{ticket}/history")
            response.raise_for_status()
            history = parse_response(response)
            logging.debug(f"Got {len(history)} events from #{ticket}")

            # loop over history in order
            history_ids = list(history.keys())
            history_ids.sort()
            for history_id in history_ids:

                # first two words in the history log define the ticket type
                ticket_type = " ".join(history[history_id].split()[:2])
                logging.debug(f'#{ticket} entry {history_id} is of type "{ticket_type}"')

                # check if ticket created during time period
                if ticket_type == "Ticket created":
                    created = True
                    response = session.get(args.api_uri + f"/ticket/{ticket}/history/id/{history_id}")
                    response.raise_for_status()
                    history_details = parse_response(response)
                    creation_date = datetime.strptime(history_details["Created"], "%Y-%m-%d %H:%M:%S")
                    logging.debug(f"#{ticket} was created on {creation_date}")
                    # only increment number of created tickets if the creation came during the time period
                    if (creation_date >= datetime.fromtimestamp(args.start)) and (creation_date <= datetime.fromtimestamp(args.end)):
                        tickets_created += 1

                # check if ticket assigned for the first time
                if not assigned and (ticket_type == "Taken by" or ticket_type == "Given to"):
                    assigned = True
                    response = session.get(args.api_uri + f"/ticket/{ticket}/history/id/{history_id}")
                    response.raise_for_status()
                    history_details = parse_response(response)
                    assign_date = datetime.strptime(history_details["Created"], "%Y-%m-%d %H:%M:%S")
                    logging.debug(f"#{ticket} was first assigned on {assign_date}")
                    # only increment number of assigned tickets if the first assignment came during the time period
                    if (assign_date >= datetime.fromtimestamp(args.start)) and (assign_date <= datetime.fromtimestamp(args.end)):
                        tickets_assigned += 1

                # check if email exchanged
                if ticket_type in {"Ticket created", "Correspondence added"}:
                    response = session.get(args.api_uri + f"/ticket/{ticket}/history/id/{history_id}")
                    response.raise_for_status()
                    history_details = parse_response(response)
                    sent_date = datetime.strptime(history_details["Created"], "%Y-%m-%d %H:%M:%S")
                    sender = history[history_id].split()[-1]
                    logging.debug(f"#{ticket} had email sent by {sender} on {sent_date}")

                    # ignore if outside time period
                    if (sent_date < datetime.fromtimestamp(args.start)) or (sent_date > datetime.fromtimestamp(args.end)):
                        continue

                    if "@" in sender: # from outside CHTC
                        emails_recv += 1

                        if last_recv is None: # store sent time (unless it's a followup)
                            last_recv = sent_date

                    else: # from inside CHTC
                        emails_sent += 1
                        last_sent = sent_date

                        if last_recv is not None: # compute delta from receive time
                            dt = last_sent - last_recv
                            response_times.append(dt.total_seconds())
                            last_recv = None # make sure not to recompute if followed up

        with open(args.outdir / f"rt_data_{queue}.txt", "w") as f:
            f.write(f"From {datetime.fromtimestamp(args.start)} to {datetime.fromtimestamp(args.end)}:\n")
            f.write(f"{tickets_created} new requests for assistance through our ticket-tracked\n")
            f.write(f"{queue} support system were addressed by {emails_recv + emails_sent} email communications.\n")
            f.write(f"\n")
            f.write(f"PATh staff were newly assigned {tickets_assigned} tickets,\n")
            f.write(f"users sent {emails_recv} emails,\n")
            f.write(f"and PATh staff sent {emails_sent} emails.\n")
            f.write(f"The average PATh staff response time to mail received was {(sum(response_times)/(len(response_times)+1e-12))/3600:0.1f} hours.\n")

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", default=OUTDIR, metavar="PATH", type=Path, help="Output directory, defaults to CWD (%(default)s)")
    parser.add_argument("--start", default=START_TS, metavar="TIMESTAMP", type=int, help="Starting timestamp, defaults to one week ago (%(default)d)")
    parser.add_argument("--end", default=END_TS, metavar="TIMESTAMP", type=int, help="Ending timestamp, defaults to now (%(default)d)")
    parser.add_argument("--log_level", metavar="LEVEL", default="WARNING", help="Log level, defaults to %(default)s")
    parser.add_argument("--queue", default=["htcondor-admin"], action="append", dest="queues", help="Queues to query, can be specified multiple times, defaults to %(default)s")
    parser.add_argument("--api_uri", help="RT API URI", required=True)
    parser.add_argument("--username", help="RT API username", required=True)
    parser.add_argument("--password_file", type=Path, help="File containing RT API password", required=True)
    args = parser.parse_args()
    return args

def main():
    args = parse_args()
    logging.getLogger().setLevel(getattr(logging, args.log_level.upper(), logging.WARNING))
    session = get_session(args)
    get_and_write_data(session, args)

if __name__ == "__main__":
    main()
