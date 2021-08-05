import csv
import dataclasses
import gzip
from pathlib import Path
from dataclasses import dataclass


@dataclass
class ZoneLookup():
    location_id: int
    borough: str
    zone: str
    service: str


@dataclass
class TripRecord():
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


def parse_zone_data(zone_filepath: Path):
    zone_data: dict[int, ZoneLookup] = {}

    with gzip.open(zone_filepath, mode='rt', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=',', quotechar='"')
        next(reader)  # skip the first line
        for row in reader:
            zone_data[row[0]] = ZoneLookup(*row)

    return zone_data


def generate_table_data(zone_data: ZoneLookup, trip_filepath: Path, output_filepath: Path):
    table_data: list[TripRecord] = []
    ride_id = 1

    with gzip.open(trip_filepath, mode='rt', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=',', quotechar='"')
        next(reader)  # skip the first line
        for row in reader:
            table_data.append(
                TripRecord(ride_id,
                           *(map(row.__getitem__, [0, 1, 2, 3, 4, 7, 8])),
                           *dataclasses.astuple(zone_data[row[7]])[1:],
                           *dataclasses.astuple(zone_data[row[8]])[1:]))
            ride_id += 1

    with open(output_filepath, 'w', newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        for row in table_data:
            writer.writerow(dataclasses.astuple(row))


if __name__ == '__main__':
    zone_data = parse_zone_data(Path.cwd() / 'res' / 'taxi_zone_lookup.csv.gz')
    generate_table_data(
        zone_data,
        Path.cwd() / 'res' / 'yellow_tripdata_2018-01-segment.csv.gz',
        Path('/') / 'tmp' / 'table_data.csv')
