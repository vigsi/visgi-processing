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
            self.type = "hourlyenergy"
        if period == Aggregator.DAILY:
            self.to_key = to_daily_key
            self.type = "energy"
        if period == Aggregator.MONTHLY:
            self.to_key = to_monthly_key
            self.type = "monthlyenergy"
        if period == Aggregator.YEARLY:
            self.to_key = to_yearly_key
            self.type = "yearlyenergy"

        self.last_key = None
        self.data = None

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
            geometry["properties"][self.type] = geometry["properties"]["ghi"] * duration_sec
            del geometry["properties"]["ghi"]

        if self.last_key != key:
            # Write out the prior result
            self.write()

            # Wet the next result as our current in-progress work
            self.last_key = key
            self.data = data
        else:
            # Merge into the in-progress group
            for pair in zip(self.data, data):
                pair[0]["properties"][self.type] += pair[1]["properties"][self.type]

    def write(self):
        if self.last_key is not None:
            group_str = json.dumps(self.data)
            filename = os.path.join(self.path, self.last_key.isoformat().replace(":", "") + ".000Z")
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

        if is_end(data):
            return

        coord_index = data["geometry"]["coordinates"]
        xi = coord_index[0]
        yi = coord_index[1]

        pt1 = self.coords[xi][yi]
        pt2 = self.coords[xi + 1][yi]
        pt3 = self.coords[xi + 1][yi + 1]
        pt4 = self.coords[xi][yi + 1]

        pt1 = [pt1[1], pt1[0]]
        pt2 = [pt2[1], pt2[0]]
        pt3 = [pt3[1], pt3[0]]
        pt4 = [pt4[1], pt4[0]]

        data["geometry"]["coordinates"] = [[pt1, pt2, pt3, pt4, pt1]]

        self.records.append(data)

    def write(self):
        pass


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
        # Every time we finish a file, we will append to the output
        # so that we don't use up all of our memory
        output_path = os.path.abspath(self.path)

        for location_index, location_records in enumerate(self.records):
            with open(os.path.join(output_path, str(location_index) + ".csv"), "a") as recordsfile:
                for record in location_records:
                    recordsfile.write(record.t.isoformat() + "," + str(record.v) + "\n")

        self.records = [[] for x in range(len(self.index_desc))]

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
            with open(os.path.join(output_path, str(location_index) + ".csv"), "a") as recordsfile:
                for record in location_records:
                    recordsfile.write(record.t.isoformat() + "," + str(record.v) + "\n")


class FixIncorrectStructure:

    CHOICES = ["fix"]

    def __init__(self, unused, path):
        self.records_by_date = defaultdict(list)
        self.path = path
        if not os.path.isdir(self.path):
            log.error("%s must be a directory", self.path)
            raise ValueError(self.path + " must be a directory")

    def start_file(self, filepath):
        pass

    def end_file(self):
        pass

    def append(self, data_instant, duration, data):
        # Now populate the data
        datestr = data["properties"]["time_stamp"]
        del data["properties"]["time_stamp"]
        date = datetime.strptime(datestr, '%Y-%m-%d')

        data["properties"]["energy"] = data["properties"]["ghi"]
        del data["properties"]["ghi"]

        data["geometry"]["type"] = "Polygon"

        self.records_by_date[date].append(data)

    def write(self):
        for key, value in self.records_by_date.items():
            group_str = json.dumps(value)
            filename = os.path.join(self.path, key.isoformat().replace(":", "") + ".000Z")
            with open(filename, "w") as output_file:
                output_file.write(group_str)


def get_data_instant(time_index):
    if time_index is not None:
        return datetime(2007, 1, 1) + timedelta(hours=time_index)


def main_cmd(args):
    """The main function for our aggregation application."""
    parser = argparse.ArgumentParser(description="Calculate statistics from GHI")
    parser.add_argument("--interval_hours", type=int, default=1)
    parser.add_argument("op", choices=Aggregator.CHOICES + Csv.CHOICES + AddLatLonCoordinates.CHOICES + FixIncorrectStructure.CHOICES)
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

    if args.op not in FixIncorrectStructure.CHOICES:
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
    else:
        indexed_files = [(f, 0) for f in files]

    # What is the interval of the data between time points
    interval = timedelta(hours=args.interval_hours)
    log.info("Interval of the data is %s", interval)

    output_dir = os.path.abspath(args.output)
    if not os.path.isdir(output_dir):
        log.error("Output directory %s must already exist", output_dir)
        sys.exit(-1)

    if os.listdir(output_dir) != []:
        log.error("Output directory %s must be empty", output_dir)
        sys.exit(-1)

    # Finally, apply the transformation to the data. We have a few transformations
    # available, and we choose which one based on which one was input at the command
    # line.
    if args.op in Aggregator.CHOICES:
        transform = Aggregator(args.op, output_dir)
    elif args.op in Csv.CHOICES:
        transform = Csv(args.op, output_dir)
    elif args.op in AddLatLonCoordinates.CHOICES:
        transform = AddLatLonCoordinates(args.op, output_dir)
    elif args.op in FixIncorrectStructure.CHOICES:
        transform = FixIncorrectStructure(args.op, output_dir)
    for file_index, (path, start_index) in enumerate(indexed_files):
        log.info("File %s start time is %s", path, get_data_instant(start_index))

        with open(path, "r") as input_file:
            data = json.load(input_file)

            if isinstance(data, dict):
                data = data["features"]

            transform.start_file(path)
            for time_index, data_group in enumerate(data):
                group_instant = get_data_instant(start_index + time_index)
                transform.append(group_instant, interval, data_group)
            transform.end_file()

        print("File %d of %d completed" % (file_index + 1, len(files)))

    transform.write()


if __name__ == "__main__":
    main_cmd(sys.argv[1:])
