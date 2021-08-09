"""
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0
"""
import csv
import dataclasses
import gzip
import json
from pathlib import Path
from typing import List

import pravega_client

from common import args


@dataclasses.dataclass
class ZoneLookup():
    """This is a zone record that maps the id to details.
    Sample record:
    1,EWR,Newark Airport,EWR
    column1:  location id
    column2:  borough
    column3:  zone
    column4:  service zone

    NOTE: the data type hinted below will be str during the runtime,
    since there is no type conversion at all.
    """
    location_id: int
    borough: str
    zone: str
    service: str


@dataclasses.dataclass
class TripRecord():
    """The trip record data set contains following elements.
    It has total 17 columns of which we are extracting only some of them.
    Sample record:
    2,2018-01-30 17:51:30,2018-01-30 18:10:37,2,2.09,1,N,186,229,1,13,1,0.5,2.96,0,0.3,17.76
    column1:  vendor id
    column2:  pickup time
    column3:  drop off time
    column4:  passenger count
    column5:  trip distance
    column6:  rate code id  # ignore
    column7:  store_and_fwd_flag  # ignore
    column8:  PU Location id # start location id
    column9:  DO Location id # drop off location id
    column10 and below # ignore
    """
    ride_id: int
    vendor_id: int
    pickup_time: str
    drop_off_time: str
    passenger_count: int
    trip_distance: float
    start_location_id: int
    dest_location_id: int
    start_location_borough: str
    start_location_zone: str
    start_location_service_zone: str
    dest_location_borough: str
    dest_location_zone: str
    dest_location_service_zone: str

    def __post_init__(self):
        # type conversion
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if not isinstance(value, field.type):
                setattr(self, field.name, field.type(value))


def parse_zone_data(zone_filepath: Path) -> ZoneLookup:
    """Read the taxi_zone_lookup and generate a zone dict.
    This dict is used in the trip record to replace location id.

    Args:
        zone_filepath (Path): The file path of the taxi_zone_lookup.csv.gz

    Returns:
        ZoneLookup: The zone dict
    """
    zone_data: dict[int, ZoneLookup] = {}

    with gzip.open(zone_filepath, mode='rt', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=',', quotechar='"')
        next(reader)  # skip the first line
        for row in reader:
            zone_data[row[0]] = ZoneLookup(*row)

    return zone_data


def generate_table_data(zone_data: ZoneLookup,
                        trip_filepath: Path) -> List[TripRecord]:
    """Generate the table data imported by the flink run in the prepare_main.

    Args:
        zone_data (ZoneLookup): Find the location details in the zone dict
        trip_filepath (Path): Path of the yellow_tripdata_2018-01-segment.csv.gz

    Returns:
        List[TripRecord]: The table data
        `List` could be changed to `list` in py3.9
    """
    table_data: list[TripRecord] = []
    ride_id = 1

    with gzip.open(trip_filepath, mode='rt', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=',', quotechar='"')
        next(reader)  # skip the first line
        for ride_id, row in enumerate(reader, start=1):
            table_data.append(
                TripRecord(ride_id,
                           *(map(row.__getitem__, [0, 1, 2, 3, 4, 7, 8])),
                           *dataclasses.astuple(zone_data[row[7]])[1:],
                           *dataclasses.astuple(zone_data[row[8]])[1:]))

    return table_data


def write_data_to_pravega(controller_uri: str, scope: str, stream: str,
                          table_data: List[TripRecord]) -> None:
    """Write data directly to the Pravega, without the help of Flink.

    Args:
        controller_uri (str): The pravega uri
        scope (str): Scope name
        stream (str): Stream name
        table_data (List[TripRecord]): The data processed by Flink
    """
    manager = pravega_client.StreamManager(controller_uri)
    manager.create_scope(scope_name=scope)
    manager.create_stream(scope_name=scope,
                          stream_name=stream,
                          initial_segments=3)

    uncapitalize = lambda s: s[0].lower() + s[1:] if s else ''

    writer = manager.create_writer(scope, stream)
    for row in table_data:
        event = {
            # convert dataclass to dict with key in camel case
            uncapitalize(''.join(w.title() for w in k.split('_'))): v
            for k, v in dataclasses.asdict(row).items()
        }
        print(event)
        writer.write_event(json.dumps(event),
                           routing_key=str(row.start_location_id))


if __name__ == '__main__':
    # get the zone dict
    zone_data = parse_zone_data(Path.cwd().parent / 'resources' /
                                'taxi_zone_lookup.csv.gz')

    # read the trip data and use the zone dict to generate
    # the final data processed by the flink
    table_data = generate_table_data(
        zone_data,
        Path.cwd().parent / 'resources' /
        'yellow_tripdata_2018-01-segment.csv.gz')

    # create the scope and the stream
    write_data_to_pravega(args.controller_uri, args.scope, args.stream,
                          table_data)
