# Copyright 2019 Alex Niu, Garret Fick, Jitendra Rathour, Zhimin Shen
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
from collections import defaultdict, namedtuple
import copy
from datetime import datetime, timedelta
import json
import logging
import os
import re
import sys

log = logging.getLogger("statistics_builder")


class Aggregator:
    """A transformation to the data that aggregates over different timescales."""
    HOURLY = "hourly"
    DAILY = "daily"
    MONTHLY = "monthly"
    YEARLY = "yearly"

    CHOICES = [HOURLY, DAILY, MONTHLY, YEARLY]

    def __init__(self, period):
        self.period = period

        def to_hourly_key(instant):
            return datetime(instant.year, instant.month, instant.day, instant.hour)
        def to_daily_key(instant):
            return datetime(instant.year, instant.month, instant.day)
        def to_monthly_key(instant):
            return datetime(instant.year, instant.month)
        def to_yearly_key(instant):
            return datetime(instant.year)
        
        if period == Aggregator.HOURLY:
            self.to_key = to_hourly_key
        if period == Aggregator.DAILY:
            self.to_key = to_daily_key
        if period == Aggregator.MONTHLY:
            self.to_key = to_monthly_key
        if period == Aggregator.YEARLY:
            self.to_key = to_yearly_key

        self.groups = dict()

    def append(self, data_instant, duration, data):
        """
        Add a group of data, that all has the same time instant.

        The format of the data is a GeoJSON record, which contains data
        for multiple locations.

        :param data_instant: The datetime of the data.
        :param duration: The duration of this interval that we have data for as a timedelta
        :param data: The GeoJSON data for the datetime.
        """
        #Which group are we aggregating into?
        key = self.to_key(data_instant)

        # Transform the data by "integrating" over the time window
        # for this data. The duration is an input parameter, and we assume
        # that things are constant over the entire period
        duration_sec = duration.total_seconds()
        for geometry in data:
            geometry["properties"]["ghinet"] = geometry["properties"]["ghi"] * duration_sec
            del geometry["properties"]["ghi"]

        group = self.groups.get(key, None)
        if group is None:
            # This is the first in the group
            self.groups[key] = data
        else:
            # This is not the first, so join by common index
            for pair in zip(group, data):
                pair[0]["properties"]["netghi"] += pair[1]["properties"]["netghi"]


class Csv:

    CHOICES = ['tocsv']

    Record = namedtuple('Record', ['t', 'v'])

    Index = namedtuple('Index', ['x', 'y'])

    def __init__(self, unused, path):
        self.index_desc = []
        self.records = None
        self.path = path

        if not os.path.isdir(self.path):
            log.error("%s must be a directory", self.path)
            raise ValueError(self.path + " must be a directory", )

    def append(self, data_instant, duration, data):
        if not self.index_desc:
            # This is the first data that we have, so our first task
            # is to figure out how big of an array we need
            for geometry in data:
                coords = geometry["geometry"]["coordinates"]
                self.index_desc.append(Csv.Index(coords[0], coords[1]))

        if not self.records:
            self.records = [[] for x in range(len(self.index_desc))]

        # Now populate the data
        for index, geometry in enumerate(data):
            self.records[index].append(Csv.Record(data_instant, geometry["properties"]["ghi"] ))

    def write(self):
        output_path = os.path.abspath(self.path)

        # Write the index information file
        with open(os.path.join(output_path, "index.csv"), "w") as indexfile:
            indexfile.write("Location Index,X,Y\n") 
            for i, index in enumerate(self.index_desc):
                indexfile.write(str(i) + "," + str(index.x) + "," + str(index.y) + "\n")
        
        for location_index, location_records in enumerate(self.records):
            with open(os.path.join(output_path, str(location_index) + ".csv"), "w") as recordsfile:
                recordsfile.write("Time,Value\n") 
                for record in location_records:
                    recordsfile.write(record.t.isoformat() + "," + str(record.v) + "\n")


def get_data_instant(time_index):
    return datetime(2007, 1, 1) + timedelta(hours=time_index)


def main_cmd(args):
    """The main function for our aggregation application."""
    parser = argparse.ArgumentParser(description="Calculate statistics from GHI")
    parser.add_argument("--interval_hours", type=int, default=1)
    parser.add_argument("op", choices=Aggregator.CHOICES + Csv.CHOICES)
    parser.add_argument("input")
    parser.add_argument("output")
    args = parser.parse_args(args)

    logging.getLogger().setLevel(logging.INFO)

    # Discover the files that we want to process
    if os.path.isfile(args.input):
        files = [os.path.abspath(args.input)]
    elif os.path.isdir(args.input):
        files = [os.path.abspath(os.path.join(args.input, f))
                 for f in os.listdir(args.input)
                 if os.path.isfile(os.path.join(args.input, f))]
    else:
        log.error("not a path or directory - oh no!")
    log.info("Processing files %s", files)

    # What is the interval of the data between time points
    interval = timedelta(hours=args.interval_hours)
    log.info("Interval of the data is %s", interval)

    # Finally, apply the aggregator to the data.
    if args.op in Aggregator.CHOICES:
        transform = Aggregator(args.op)
    else:
        transform = Csv(args.op, args.output)
    for file_index, path in enumerate(files):
        m = re.findall(r"(?P<start>\d+)-(\d+)\.json", path)
        if len(m) != 1 or len(m[0]) != 2:
            log.warning("Ignoring file %s", path)
            continue
        
        start_index = int(m[0][0])
        log.info("File %s start time is %s", path, get_data_instant(start_index))

        with open(path, "r") as input_file:
            data = json.load(input_file)

            for time_index, data_group in enumerate(data):
                group_instant = get_data_instant(start_index + time_index)
                transform.append(group_instant, interval, data_group)

        print("File %d of %d completed" % (file_index, len(files)))

    transform.write()


if __name__ == "__main__":
    main_cmd(sys.argv[1:])