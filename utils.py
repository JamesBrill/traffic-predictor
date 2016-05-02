import csv

def get_detectors():
    detectors = {}
    with open('detectors_sample.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            detector_id = row['scn']
            if detector_id in detectors:
                detectors[detector_id] += 1
            else:
                detectors[detector_id] = 0
    return detectors

def print_dict(dict):
    for key in dict:
        print key, dict[key]

def get_data_sorted_by_timestamp(): # TODO: replace with groupby
    timestamps = {}
    with open('detectors_sample.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            timestamp = get_epoch_timestamp(row['timestamp'])
            if timestamp in timestamps:
                row.pop('timestamp', 0)
                timestamps[timestamp].append(row)
            else:
                timestamps[timestamp] = []
                return timestamps
