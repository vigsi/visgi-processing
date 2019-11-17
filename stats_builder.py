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
from collections import defaultdict
import copy
from datetime import datetime, timedelta
import json
import logging
import os
import re
import sys

log = logging.getLogger("statistics_builder")


class Aggregator:
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
            geometry["properties"]["ghi"] = geometry["properties"]["ghi"] * duration_sec

        group = self.groups.get(key, None)
        if group is None:
            # This is the first in the group
            self.groups[key] = data
        else:
            # This is not the first, so join by common index
            for pair in zip(group, data):
                pair[0]["properties"]["ghi"] += pair[1]["properties"]["ghi"]

    def to_csv(self):
        pass
    

def get_data_instant(time_index):
    return datetime(2007, 1, 1) + timedelta(hours=time_index)


def main_cmd(args):
    """The main function for our aggregation application."""
    parser = argparse.ArgumentParser(description="Calculate statistics from GHI")
    parser.add_argument("--interval_hours", type=int, default=1)
    parser.add_argument("group_size", choices=Aggregator.CHOICES)
    parser.add_argument("input")
    parser.add_argument("output")
    args = parser.parse_args()

    # Discover the files that we want to process
    if os.path.isfile(args.input):
        files = [os.path.abspath(args.input)]
    elif os.path.isdir(args.input):
        files = [os.path.abspath(os.path.join(args.input, f))
                 for f in os.path.listdir(args.input)
                 if os.path.isfile(os.path.join(args.input), f)]
    else:
        log.error("not a path or directory - oh no!")
    log.info("Processing files %s", files)

    # What is the interval of the data between time points
    interval = timedelta(hours=args.interval_hours)
    log.info("Interval of the data is %s", interval)

    # Finally, apply the aggregator to the data.
    aggregator = Aggregator(args.group_size)
    for path in files:
        m = re.findall(r"(?P<start>\d+)-(\d+)\.json", path)
        if len(m) != 2:
            log.warn("Ignoring file %s", path)
        
        start_index = int(m[0][0])
        log.info("File %s start time is %s", path, get_data_instant(start_index))

        with open(path, "r") as input_file:
            data = json.load(input_file)

            for time_index, data_group in enumerate(data):
                group_instant = get_data_instant(start_index + time_index)
                aggregator.append(group_instant, interval, data_group)


if __name__ == "__main__":
    main_cmd(sys.argv[1:])