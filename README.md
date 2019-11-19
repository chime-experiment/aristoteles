# aristoteles

Aristoteles reads data from a [wview](https://sourceforge.net/projects/wview/)
SQLite database and writes it to daily HDF5 files.

Aristoteles always writes UTC days.  Whenever it outputs an HDF5 file, it
records the day it has output in a state file, so that it can pick up
where it left off next time.

Aristoteles assumes a data period of five minutes for data written by wview.
As a result, it assumes the database will contain 288 samples per day.

Whenever aristoteles runs, it checks the previous UTC day to see if all 288
samples are present.  If all samples are present, it will write that day and
all prior days not yet written (as indicated by the state file).  If not all
samples are present, aristoteles does nothing.  This behaviour is designed to
accomodate late data fetched by the wview server, which can, after periods of
downtime, request the old data it missed from the upstream wview server.

UTC day files are organized into monthly directories.

This is an update of the ch_translate_weather script originally written by Adam Hincks.
