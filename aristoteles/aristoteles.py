"""
aristoteles takes data from a https://sourceforge.net/projects/wview/
Sqlite database and exports it as daily HDF5 files.
"""
import os
import sys
import h5py
import arrow
import socket
import sqlite3
import argparse
import configobj
import numpy as np
from aristoteles import __version__ as aristoteles_version

# See https://bao.chimenet.ca/doc/documents/5, Table 3
archive_version = "4.0.0"

# This is the metric metadata we write to the textfille collector
metric_descriptions = {
    "report_time": "aristoteles.prom write time",
    "status": "aristoteles exit status",
    "days_written": "number of days written",
    "yesterday": "yesterday's date",
    "first_day": "first day checked",
    "samples_yesterday": "number of weather samples for yesterday",
}

# Metric values go in here
metric_data = list()

dataset = {
    "barometer": {"type": "pressure"},
    "pressure": {"type": "pressure"},
    "altimeter": {"type": "pressure"},
    "inTemp": {"type": "temperature"},
    "outTemp": {"type": "temperature"},
    "inHumidity": {"type": "percent"},
    "outHumidity": {"type": "percent"},
    "windSpeed": {"type": "speed"},
    "windDir": {"type": "direction"},
    "windGust": {"type": "speed"},
    "windGustDir": {"type": "direction"},
    "rainRate": {"type": "rate"},
    "rain": {"type": "amount"},
    "dewpoint": {"type": "temperature"},
    "windchill": {"type": "temperature"},
    "heatindex": {"type": "temperature"},
}

units = {
    "pressure": "hPa",
    "temperature": "deg C",
    "percent": "%",
    "speed": "km/h",
    "direction": "deg",
    "rate": "mm/hr",
    "amount": "mm",
}

# This is the absolute earliest day we're willing to entertain.
_DAY_LIMIT = arrow.get("2000-01-01")


def write_state(conf, value):
    """Write tommorrow relative to value (an arrow) to state file"""
    with open(conf["state_path"], "w") as f:
        f.write(value.shift(days=1).format("YYYYMMDD"))


def read_state(conf):
    """Read and parse state"""
    try:
        with open(conf["state_path"]) as f:
            return arrow.get(f.read(), "YYYYMMDD")
    except (OSError, arrow.parser.ParserError):
        pass

    return None


def day_arg(arg):
    """Range checker for --reset-state argument"""
    try:
        day = arrow.get(arg, "YYYYMMDD")
    except arrow.parser.ParserError:
        raise argparse.ArgumentTypeError("{0} must be of the form YYYYMMDD".format(arg))

    if day < _DAY_LIMIT or day > arrow.utcnow():
        raise argeparse.ArgumentTypeError("{0} out of range".format(arg))

    return day


def add_metric(metric, value, labels=None):
    """Record a new metric value with optional label dict"""
    metric_data.append({"name": metric, "value": str(value), "labels": labels})


def prom_and_exit(conf, status):
    """Write aristoteles.prom in the textfile collector directory and then exit with status"""

    # Skip promfile creation if we haven't been given the textifle collector directory
    if "netfc_path" in conf:
        path = os.path.join(conf["netfc_path"], "aristoteles.prom")

        add_metric("status", status)
        add_metric("report_time", arrow.utcnow().int_timestamp)

        try:
            with open(path + ".new", "w") as f:
                for metric in metric_data:
                    if metric["name"] in metric_descriptions:
                        name = "aristoteles_" + metric["name"]
                        f.write(
                            "# HELP "
                            + name
                            + " "
                            + metric_descriptions[metric["name"]]
                            + "\n"
                        )
                        f.write("# TYPE " + name + " gauge\n")
                        f.write(name)
                        if metric["labels"] is not None:
                            f.write(
                                "{"
                                + (
                                    ",".join(
                                        [
                                            k + '="' + str(v) + '"'
                                            for (k, v) in metric["labels"].items()
                                        ]
                                    )
                                )
                                + "}"
                            )
                        f.write(" " + metric["value"] + "\n")

        except OSError:
            os.unlink(path + ".new")
        else:
            os.rename(path + ".new", path)

    # Exit
    exit(status)


def entry():
    global __doc__

    # Parse command line
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-c",
        "--conf-file",
        type=str,
        default="./aristoteles.conf",
        help="Read configuration from CONF_FILE",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="write data, even if yesterday (or the stop day, if given) is not "
        "complete",
    )
    parser.add_argument(
        "--reset-state",
        type=day_arg,
        metavar="YYYYMMDD",
        nargs="?",
        const=_DAY_LIMIT,
        default=None,
        help="reset the state to the supplied UTC day, if given, or else to the "
        "earliest date in the database and exit without processing data.  If a "
        "valid state already exists, this will (successfully) do nothing unless "
        "--force is also specified.",
    )
    parser.add_argument(
        "--stop",
        type=day_arg,
        metavar="YYYYMMDD",
        default=None,
        help="stop after writing UTC day YYYYMMDD instead of yesterday",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="be verbose (mostly for debugging)"
    )
    arg = parser.parse_args()

    try:
        conf = configobj.ConfigObj(arg.conf_file, raise_errors=True, file_error=True)
    except OSError as e:
        print("FATAL: error reading config file: " + repr(e.args), file=sys.stderr)
        prom_and_exit(conf, 1)

    for key in ("state_path", "instrument"):
        if key not in conf:
            print("FATAL: Missing configuration key: " + key, file=sys.stderr)
            exit(1)

    # Each weather station has its own section in the config object
    stations = conf.sections
    if len(stations) < 1:
        print("FATAL: No weather stations defined.")
        exit(1)

    # For each station, connect to the DB and get the start date
    db = dict()
    cur = dict()
    start_day = dict()
    for station in stations:
        # Open the database and find the earliest record
        if "db_path" not in conf[station]:
            print(
                "FATAL: Missing configuration key: " + key + " for station " + station,
                file=sys.stderr,
            )
            exit(1)

        if not os.access(conf[station]["db_path"], os.R_OK):
            print(
                "FATAL: Unable to access {0} for station {1}".format(
                    conf[station]["db_path"], station
                ),
                file=sys.stderr,
            )
            exit(1)

        db[station] = sqlite3.connect(conf[station]["db_path"])
        cur[station] = db[station].cursor()

        if arg.verbose:
            print(
                "Reading weather data for {0} from {1}".format(
                    station, conf[station]["db_path"]
                )
            )

        # Get the start date
        cur[station].execute("SELECT dateTime FROM archive ORDER BY dateTime LIMIT 1;")
        start_day[station] = arrow.get(cur[station].fetchone()[0]).floor("day")

    # Current UTC day
    today = arrow.utcnow().floor("day")

    # Force-rewrite the state, if asked to
    if arg.reset_state is not None:
        if arg.force or read_state(conf) is None:

            # Find the earliest start_day
            first_day = today
            for station in stations:
                if first_day > start_day[station]:
                    first_day = start_day[station]

            # If the requested state value is earlier than the first day
            # available, just advance to that day
            if arg.reset_state < first_day:
                arg.reset_state = first_day

            # Today is tomorrow's yesterday
            write_state(conf, arg.reset_state.shift(days=-1))
        else:
            print("State present.  Use --force to overwrite.")
        exit(0)

    # Load the state
    first_day = read_state(conf)
    if first_day is None:
        print("FATAL: Bad state.  Regenerate with --reset-state.", file=sys.stderr)
        exit(1)

    add_metric("first_day", first_day.int_timestamp)

    # This is the last UTC day we're doing
    if arg.stop:
        yesterday = arg.stop
    else:
        yesterday = today.shift(days=-1)

    add_metric("yesterday", yesterday.int_timestamp)

    if arg.verbose:
        print("first day = ", first_day)
        print("yesterday = ", yesterday)

    # Nothing to do
    if yesterday.shift(days=1) <= first_day:
        prom_and_exit(conf, 0)

    # Make sure the archive exists
    if not os.path.exists(conf["archive"]):
        print("FATAL: archive {} not found.".format(conf["archive"]))
        prom_and_exit(conf, 1)

    # For each station, count the number of data points for yesterday.
    # We should have one reading every five minutes, so:
    #
    #  1 day * 1440 minutes/day / 5 minutes = 288
    #
    # We only continue if _all_ stations have a complete day (or if forced)
    for station in stations:
        cur[station].execute(
            "SELECT COUNT() FROM archive WHERE dateTime BETWEEN ? AND ?",
            (yesterday.int_timestamp, yesterday.ceil("day").int_timestamp),
        )

        count = cur[station].fetchone()
        if count is None:
            count = 0
        else:
            count = count[0]

        add_metric("samples_yesterday", count, dict({"station": station}))

        if count != 1440 / 5:
            if arg.force:
                print(
                    "Incomplete yesterday for station {0} ({1} records), "
                    "continuing anyways.".format(station, count)
                )
            else:
                print(
                    "Incomplete yesterday for station {0} ({1} records), "
                    "doing nothing.".format(station, count)
                )
                prom_and_exit(conf, 0)

    col_name = [k for k, v in dataset.items()]
    col = ",".join(["dateTime", "usUnits"] + col_name)

    # Loop over days
    count = 0
    for start, stop in arrow.Arrow.span_range("day", first_day, yesterday):

        # Loop over stations
        data = dict()
        have_data = False
        for station in stations:
            cur[station].execute(
                "SELECT "
                + col
                + " FROM archive WHERE dateTime BETWEEN ? AND ? ORDER BY dateTime",
                (start.int_timestamp, stop.int_timestamp),
            )
            data[station] = np.asarray(cur[station].fetchall(), dtype=float)

            if not data[station].shape[0]:
                if arg.verbose:
                    print(
                        "No data on {0} for station {1}".format(
                            start.format("YYYY-MM-DD"), station
                        )
                    )
            else:
                have_data = True
                if arg.verbose:
                    print(
                        "Found {0} records on {1} for station {1}".format(
                            data[station].shape[0], start.format("YYYY-MM-DD"), station
                        )
                    )

        if not have_data:
            print(
                "No data on {0} for any station, skipping".format(
                    start.format("YYYY-MM-DD"), station
                )
            )
            continue

        # Create the file (and acq, if necessary)
        acq = "{0}Z_{1}_weather".format(
            start.floor("month").format("YYYYMMDDTHHmmss"), conf["instrument"]
        )
        basedir = os.path.join(conf["archive"], acq)
        filename = "{0}.h5".format(start.format("YYYYMMDD"))
        filepath = os.path.join(basedir, filename)
        lockpath = os.path.join(basedir, ".{0}.lock".format(filename))

        if not os.path.exists(basedir):
            if arg.verbose:
                print("Creating acquisition directory: {0}".format(basedir))
            os.makedirs(basedir)

        # Create empty lock file
        open(lockpath, "w").close()

        if arg.verbose:
            print("Writing file {0}".format(filepath))

        # Create the HDF5 file and add global attributes.
        hf = h5py.File(filepath, "w")
        hf.attrs.create(
            "git_version_tag", "aristoteles-{0}".format(aristoteles_version)
        )
        hf.attrs.create("system_user", os.environ["USER"])
        hf.attrs.create("collection_server", socket.gethostname())
        hf.attrs.create("instrument_name", conf["instrument"])
        hf.attrs.create("archive_version", archive_version)
        hf.attrs.create("acquisition_name", acq)
        hf.attrs.create("acquisition_type", "weather")

        # Create the image map
        img = hf.create_group("index_map")

        # Create a group for each station
        n_wrote = 0
        for station in stations:
            # Be there data?
            if not data[station].shape[0]:
                continue

            # Convert from US units, if necessary
            for i in range(data[station].shape[0]):
                if data[station][i, 1]:
                    for j in range(2, data[station].shape[1]):
                        if not data[station][i, j]:
                            continue
                        t = dataset[col_name[j - 2]]["type"]
                        if t == "pressure":
                            data[station][i, j] = (
                                data[station][i, j] * 33.863886
                            )  # inHg to hPa
                        elif t == "temperature":
                            data[station][i, j] = (
                                (data[station][i, j] - 32.0) * 5.0 / 9.0
                            )  # F to C
                        elif t == "speed":
                            data[station][i, j] = (
                                data[station][i, j] * 1.609344
                            )  # mi/h to km/ha
                        elif t == "amount" or t == "rate":
                            data[station][i, j] = (
                                data[station][i, j] * 25.4
                            )  # inch to mm

            img.create_dataset("station_time_" + station, data=data[station][:, 0])

            gr = hf.create_group(station)

            # Attributes
            gr.attrs.create("wview_database", conf[station]["db_path"])

            if "longitude" in conf[station]:
                gr.attrs.create("longitude", float(conf[station]["longitude"]))
            else:
                gr.attrs.create("longitude", float("NaN"))

            if "latitude" in conf[station]:
                gr.attrs.create("latitude", float(conf[station]["latitude"]))
            else:
                gr.attrs.create("latitude", float("NaN"))

            if "description" in conf[station]:
                gr.attrs.create("description", conf[station]["description"])
            else:
                gr.attrs.create("description", "")

            # Create the datasets.
            for i in range(len(col_name)):
                d = gr.create_dataset(col_name[i], data=data[station][:, i + 2])
                d.attrs.create("axis", ["station_time_" + station])
                d.attrs.create("units", units[dataset[col_name[i]]["type"]])

            n_wrote += data[station].shape[0]

        hf.close()

        # Increment count of written files
        count += 1

        # Delete the lock file
        os.unlink(lockpath)

        write_state(conf, start)

        print("Wrote {0} records to {1}".format(n_wrote, filepath))

    # Close
    for station in stations:
        db[station].close()

    add_metric("days_written", count)
    prom_and_exit(conf, 0)
