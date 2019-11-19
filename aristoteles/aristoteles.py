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

archive_version = "2.3.0"

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

_DAY_LIMIT = arrow.get("2000-01-01")


def write_state(conf, value):
    """Write value (an arrow) to state file"""
    with open(conf["state_path"], "w") as f:
        f.write(value.format("YYYYMMDD"))


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
        "earliest date in the database and exit without processing data",
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
        exit(1)

    for key in ("db_path", "state_path", "instrument"):
        if key not in conf:
            raise KeyError("Missing configuration key: " + key)

    # Open the database and find the earliest record
    db = sqlite3.connect(conf["db_path"])
    cur = db.cursor()

    if arg.verbose:
        print("Reading weather data from {0}".format(conf["db_path"]))

    # Get the start date
    cur.execute("SELECT dateTime FROM archive ORDER BY dateTime LIMIT 1;")
    start_day = arrow.get(cur.fetchone()[0]).floor("day")

    # Force-rewrite the state, if asked to
    if arg.reset_state is not None:
        if arg.reset_state < start_day:
            arg.reset_state = start_day
        write_state(conf, arg.reset_state)
        exit(0)

    # Load the state
    first_day = read_state(conf)
    if first_day is None:
        print("FATAL: Bad state.  Regenerate with --reset-state.", file=sys.stderr)
        exit(1)

    # This is the last UTC day we're doing
    if arg.stop:
        yesterday = arg.stop
    else:
        yesterday = arrow.utcnow().floor("day").shift(days=-1)

    print("yesterday = ", yesterday)

    # Nothing to do
    if yesterday == first_day:
        os.exit(0)

    # Count the number of data points for yesterday.  We should have one reading
    # every five minutes, so:
    #
    #  1 day * 1440 minutes/day / 5 minutes = 288
    #
    cur.execute(
        "SELECT COUNT() FROM archive WHERE dateTime BETWEEN ? AND ?",
        (yesterday.timestamp, yesterday.ceil("day").timestamp),
    )

    count = cur.fetchone()
    if count is None:
        count = 0
    else:
        count = count[0]

    if count != 1440 / 5:
        if arg.force:
            print(
                "Incomplete yesterday ({0} records), continuing anyways.".format(count)
            )
        else:
            print("Incomplete yesterday ({0} records), doing nothing.".format(count))
            exit(0)

    col_name = [k for k, v in dataset.items()]
    col = ",".join(["dateTime", "usUnits"] + col_name)

    # Loop over days
    for start, stop in arrow.Arrow.span_range("day", first_day, yesterday):
        cur.execute(
            "SELECT "
            + col
            + " FROM archive WHERE dateTime BETWEEN ? AND ? ORDER BY dateTime",
            (start.timestamp, stop.timestamp),
        )
        data = np.asarray(cur.fetchall(), dtype=float)

        if not data.shape[0]:
            if arg.verbose:
                print("No weather data for {0}".format(start.format("YYYY-MM-DD")))
            continue
        elif arg.verbose:
            print(
                "Found {0} records for {1}".format(
                    data.shape[0], start.format("YYYY-MM-DD")
                )
            )

        acq = "{0}_{1}_weather".format(
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

        # Check if "usUnits" is true; if so, convert from Imperial to metric units.
        for i in range(data.shape[0]):
            if data[i, 1]:
                for j in range(2, data.shape[1]):
                    if not data[i, j]:
                        continue
                    t = dataset[col_name[j - 2]]["type"]
                    if t == "pressure":
                        data[i, j] = data[i, j] * 33.86389  # inHg to hPa
                    elif t == "temperature":
                        data[i, j] = (data[i, j] - 32.0) * 5.0 / 9.0  # F to C
                    elif t == "speed":
                        data[i, j] = data[i, j] * 1.60934  # mi/h to km/ha
                    elif t == "amount" or t == "rate":
                        data[i, j] = data[i, j] * 25.4  # inch to mm

        # Create empty lock file
        open(lockpath, "w").close()

        if arg.verbose:
            print("Writing file {0}".format(filepath))

        # Create the HDF5 file and add global attributes.
        hf = h5py.File(filepath, "w")
        hf.attrs.create("git_version_tag", "aristoteles-v{0}".format(aristoteles_version))
        hf.attrs.create("system_user", os.environ["USER"])
        hf.attrs.create("collection_server", socket.gethostname())
        hf.attrs.create("instrument_name", conf["instrument"])
        hf.attrs.create("archive_version", archive_version)
        hf.attrs.create("acquisition_name", acq)
        hf.attrs.create("acquisition_type", "weather")
        hf.attrs.create("wview_database", conf["db_path"])

        # Create the datasets.
        img = hf.create_group("index_map")
        img.create_dataset("time", data=data[:, 0])
        for i in range(len(col_name)):
            d = hf.create_dataset(col_name[i], data=data[:, i + 2])
            d.attrs.create("axis", ["time"])
            d.attrs.create("units", units[dataset[col_name[i]]["type"]])
        hf.close()

        # Delete the lock file
        os.unlink(lockpath)

        write_state(conf, start)

        print("Wrote {0} records to {1}".format(data.shape[0], filepath))

    db.close()
