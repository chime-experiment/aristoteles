#!/usr/bin/python
"""
Copy weather data from the sqlite database to an HDF5 archive file.
"""

import argparse
import configobj
import datetime
import getpass
import h5py
import numpy as np
import os
import socket
import sqlite3
import sys

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

# Parse command line and get .conf information.
parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
parser.add_argument("date", metavar="<YYYYMMDD>", type=str)
parser.add_argument("-c", "--conf-file", default="ch_translate_weather.conf")
parser.add_argument(
    "-g",
    "--git-tag",
    action="store",
    help="Current git tag, use: " + "-g `git describe --tags` ",
)
arg = parser.parse_args()

# Be paranoid: if the executable is being run from /usr/local/bin we can be
# reasonably assured that the git tag recorded in /etc/CHIME is correct. If it
# is not being run from there, force the user manually insert the git tag as
# an option.
if sys.argv[0] != "/usr/local/bin/ch_translate_weather":
    if not arg.git_tag:
        print('If you are not running this as a daemon from "/usr/sbin", you ')
        print("MUST use the -g option and manually specify which git tag you are ")
        print("running (e.g., ./ch_translate_weather.py -g `git describe --tags`).")
        exit()

conf = configobj.ConfigObj(arg.conf_file)
try:
    conf["db_path"]
    conf["log_path"]
    conf["archive"]
    conf["instrument"]
    conf["type"]
except KeyError:
    print('The .conf file needs both a "db_path" and "log_path" key.')
    exit()

# Figure out the starting UNIX time.
t_start = int(datetime.datetime.strptime(arg.date, "%Y%m%d").strftime("%s"))

# Get the data.
db = sqlite3.connect(conf["db_path"])
cur = db.cursor()
cur.execute("SELECT dateTime FROM archive ORDER BY dateTime LIMIT 1;")
t_first = cur.fetchone()[0]
col_name = [k for k, v in dataset.iteritems()]
col = ",".join(["dateTime", "usUnits"] + col_name)
cur.execute(
    "SELECT %s FROM archive WHERE dateTime BETWEEN %d AND %d "
    "ORDER BY dateTime;" % (col, t_start, t_start + 86399)
)
data = np.asarray(cur.fetchall(), dtype=float)
db.close()

if not data.shape[0]:
    print("No weather data for that day exist.")
    exit()

ts = datetime.datetime.fromtimestamp(t_first).strftime("%Y%m%dT%H%M%SZ")
acq = "%s_%s_%s" % (ts, conf["instrument"], conf["type"])
basedir = "%s/%s" % (conf["archive"], acq)
filename = "{0}.h5".format(arg.date)
filepath = "{0}/{0}".format(basedir, filename)
lockpath = "{0}/.{0}.lock".format(basedir, filename)

if not os.path.exists(basedir):
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

# Get the git tag and for writing to header.
if not arg.git_tag:
    fp = open("/etc/chime/version", "r")
    if not fp:
        print('Could not find git tag in "/etc/chime/version".')
        exit()
    arg.git_tag = fp.read().replace("\n", "")

# Create empty lock file
open(lockpath).close()

# Create the HDF5 file and add global attributes.
hf = h5py.File(filepath, "w")
hf.attrs.create("git_version_tag", arg.git_tag)
hf.attrs.create("system_user", getpass.getuser())
hf.attrs.create("collection_server", socket.gethostname())
hf.attrs.create("instrument_name", conf["instrument"])
hf.attrs.create("archive_version", archive_version)
hf.attrs.create("acquisition_name", "%s" % (acq))
hf.attrs.create("acquisition_type", conf["type"])
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
