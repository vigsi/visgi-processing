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
import urllib.request

log = logging.getLogger("statistics_builder")


class Aggregator:
    """A transformation to the data that aggregates over different timescales."""
    HOURLY = "hourly"
    DAILY = "daily"
    MONTHLY = "monthly"
    YEARLY = "yearly"

    CHOICES = [HOURLY, DAILY, MONTHLY, YEARLY]

    def __init__(self, period, path):
        self.period = period
        self.path = os.path.abspath(path)

        def to_hourly_key(instant):
            return datetime(instant.year, instant.month, instant.day, instant.hour)
        def to_daily_key(instant):
            return datetime(instant.year, instant.month, instant.day)
        def to_monthly_key(instant):
            return datetime(instant.year, instant.month, 1)
        def to_yearly_key(instant):
            return datetime(instant.year, 1, 1)
        
        if period == Aggregator.HOURLY:
            self.to_key = to_hourly_key
        if period == Aggregator.DAILY:
            self.to_key = to_daily_key
        if period == Aggregator.MONTHLY:
            self.to_key = to_monthly_key
        if period == Aggregator.YEARLY:
            self.to_key = to_yearly_key

        self.groups = dict()

    def start_file(self, filepath):
        pass

    def end_file(self):
        pass

    def append(self, data_instant, duration, data):
        """
        Add a group of data, that all has the same time instant.

        The format of the data is a GeoJSON record, which contains data
        for multiple locations.

        :param data_instant: The datetime of the data.
        :param duration: The duration of this interval that we have data for as a timedelta
        :param data: The GeoJSON data for the datetime.
        """
        # Which group are we aggregating into?
        key = self.to_key(data_instant)

        # Transform the data by "integrating" over the time window
        # for this data. The duration is an input parameter, and we assume
        # that things are constant over the entire period
        duration_sec = duration.total_seconds()
        for geometry in data:
            geometry["properties"]["energy"] = geometry["properties"]["ghi"] * duration_sec
            del geometry["properties"]["ghi"]

        group = self.groups.get(key, None)
        if group is None:
            # This is the first in the group
            self.groups[key] = data
        else:
            # This is not the first, so join by common index
            for pair in zip(group, data):
                pair[0]["properties"]["energy"] += pair[1]["properties"]["energy"]

    def write(self):
        for key, group in self.groups.items():
            group_str = json.dumps(group)
            filename = os.path.join(self.path, key.isoformat().replace(":", "") + ".000Z")
            with open(filename, "w") as output_file:
                output_file.write(group_str)

class AddLatLonCoordinates:
    """
    A transformation of the GeoJSON data to add lat/lon coordinates
    from the H5 coordinate indices.
    """

    CHOICES = ["addlatlon"]

    COORDS_URL = "https://developer.nrel.gov/api/hsds//datasets/d-70e214c6-85f4-11e7-bf89-0242ac110008/value?select=[0:1601:32,0:2975:60]&host=/nrel/wtk-us.h5&api_key=3K3JQbjZmWctY0xmIfSYvYgtIcM3CN0cb1Y2w9bf"

    def __init__(self, unused, path):
        self.output_path = path
        with urllib.request.urlopen(AddLatLonCoordinates.COORDS_URL) as response:
            data = response.read()

        coord_data = json.loads(data)
        self.coords = coord_data["value"]
        self.cur_file_name = None
        self.records = []
        
    def start_file(self, filepath):
        self.cur_file_name = os.path.basename(filepath)

    def end_file(self):
        # Write to string with compact encoding
        data_str = json.dumps(self.records, separators=(",", ":"))
        with open(os.path.join(self.output_path, self.cur_file_name), "w") as output_file:
            output_file.write(data_str )
        self.records = []

    def append(self, data_instant, duration, data):
        """Add a group of data in a file record"""
        max_xi = len(self.coords)
        max_yi = len(self.coords[0])

        def is_end(x):
            coord_index = x["geometry"]["coordinates"]
            xi = coord_index[0]
            yi = coord_index[1]
            return xi + 1 >= max_xi or yi + 1 >= max_yi

        for geometry in [x for x in data if not is_end(x)]:
            coord_index = geometry["geometry"]["coordinates"]
            xi = coord_index[0]
            yi = coord_index[1]

            pt1 = self.coords[xi][yi]
            pt2 = self.coords[xi + 1][yi]
            pt3 = self.coords[xi + 1][yi + 1]
            pt4 = self.coords[xi][yi + 1]

            geometry["geometry"]["coordinates"] = [[pt1, pt2, pt3, pt4, pt1]]

        self.records.append(data)


class SplitByTimeIndex:
    """
    A transformation of the data that splits based on time index
    of the data.
    """

    CHOICES = ["splitindex"]

    def __init__(self, unused, path):
        self.path = path

        if not os.path.isdir(self.path):
            log.error("%s must be a directory", self.path)
            raise ValueError(self.path + " must be a directory")

    def start_file(self, filepath):
        pass

    def end_file(self):
        pass

    def append(self, data_instant, duration, data):
        data_str = json.dumps(data, separators=(",", ":"))
        with open(os.path.join(self.path, data_instant.isoformat() + ".json")) as output_file:
            output_file.write(data_str)

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

class Csv:
    """
    A transformation of the data that converts it to CSV.


    This does a simple transformation of the input data to
    write it out as a csv file, one per location.
    """


    CHOICES = ["tocsv"]

    Record = namedtuple("Record", ["t", "v"])

    Index = namedtuple("Index", ["x", "y"])

    def __init__(self, unused, path):
        self.index_desc = []
        self.records = None
        self.path = path

        if not os.path.isdir(self.path):
            log.error("%s must be a directory", self.path)
            raise ValueError(self.path + " must be a directory")

    def start_file(self, filepath):
        pass

    def end_file(self):
        pass

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
    parser.add_argument("op", choices=Aggregator.CHOICES + Csv.CHOICES + AddLatLonCoordinates.CHOICES)
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

    # Sort the files in numeric order based on the start index
    def get_start_index(filepath):
        filename = os.path.basename(filepath)
        if filename.find("-") < 0:
            return (filepath, None)
        start_index = int(filename[0:filename.find("-")])
        return (filepath, start_index)

    def index_key(path_and_index):
        return path_and_index[1]

    indexed_files = list(sorted(map(get_start_index, files), key=index_key))

    # What is the interval of the data between time points
    interval = timedelta(hours=args.interval_hours)
    log.info("Interval of the data is %s", interval)

    # Finally, apply the transformation to the data. We have a few transformations
    # available, and we choose which one based on which one was input at the command
    # line.
    if args.op in Aggregator.CHOICES:
        transform = Aggregator(args.op, args.output)
    elif args.op in Csv.CHOICES:
        transform = Csv(args.op, args.output)
    elif args.op in AddLatLonCoordinates.CHOICES:
        transform = AddLatLonCoordinates(args.op, args.output)
    for file_index, (path, start_index) in enumerate(indexed_files):
        log.info("File %s start time is %s", path, get_data_instant(start_index))

        with open(path, "r") as input_file:
            data = json.load(input_file)

            transform.start_file(path)
            for time_index, data_group in enumerate(data):
                group_instant = get_data_instant(start_index + time_index)
                transform.append(group_instant, interval, data_group)
            transform.end_file()

        print("File %d of %d completed" % (file_index + 1, len(files)))

    transform.write()


if __name__ == "__main__":
    main_cmd(sys.argv[1:])