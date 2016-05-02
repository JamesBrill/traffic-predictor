import csv
import time
import numpy as np
from sklearn.preprocessing import StandardScaler

def get_epoch_timestamp(timestamp):
    if len(timestamp) != 19:
        timestamp = timestamp[:-7]
    return int(round(time.mktime(time.strptime(timestamp, "%Y-%m-%d %H:%M:%S"))));

def get_data_from_csv():
    data = []
    with open('detectors_sample.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            timestamp = get_epoch_timestamp(row['timestamp'])
            row['timestamp'] = timestamp
            data.append(row)
    return data

def remove_faulty_data(data):
    return [x for x in data if x['status'] == '1']

def aggregate_records(timestamp, records):
    output = []
    ids = [record['scn'] for record in records]
    for id in ids:
        records_for_id = filter((lambda x : x['scn'] == id), records)
        vehicle_lengths = map((lambda x : int(x['vehicle_length'])), records_for_id)
        headways = map((lambda x : int(x['headway'])), records_for_id)
        mean_vehicle_length = np.mean(vehicle_lengths)
        mean_headway = np.mean(headways)
        output.append({'scn' : id, 'timestamp' : timestamp, 'vehicle_length' : mean_vehicle_length, 'headway' : mean_headway})
    return output

def discretise_data(data):
    discretised_records = []
    records = []
    timestamps = sorted(list(set([record['timestamp'] for record in data])))
    start = timestamps[0]
    for timestamp in timestamps:
        if timestamp < (start + 10): #TODO: for full data, should be +900 for 15 minutes
            records += [record for record in data if record['timestamp'] == timestamp]
        else:
            discretised_records += aggregate_records(start, records)
            records = []
            start = timestamp
    return discretised_records

def simplify_data(data):
    for record in data:
        id = record['scn']
        density = float(record['vehicle_length']) / float(record['headway'])
        record.pop('scn', 0)
        record.pop('headway', 0)
        record.pop('vehicle_length', 0)
        record.pop('status', 0)
        record['id'] = id
        record['density'] = density
    return data

def standardise_density(data):
    densities = [record['density'] for record in data]
    reshaped_densities = np.array(densities).reshape((len(densities), 1)) # to 2D array for scikit
    scaler = StandardScaler()
    standardised_densities = scaler.fit_transform(reshaped_densities)
    for i in range(len(data)):
        data[i]['density'] = standardised_densities[i][0]
    return data

def convert_to_csv_format(data):
    output = [["Timestamp", "Id", "Density"]]
    for record in data:
        output.append([record['timestamp'], record['id'], record['density']])
    return output

def export_to_csv(data):
    with open('output.csv', 'w', newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        writer.writerows(data)

def main():
    print "Getting data from CSV..."
    raw_data = get_data_from_csv()
    print "Removing damaged data..."
    clean_data = remove_faulty_data(raw_data)
    print "Aggregating data into 10 second intervals..."
    discretised_data = discretise_data(clean_data)
    print "Converting sensor data into density values..."
    simplified_data = simplify_data(discretised_data)
    print "Standardising density values..."
    standardised_data = standardise_density(simplified_data)
    print "Converting to CSV format..."
    csv_friendly_data = convert_to_csv_format(standardised_data)
    print "Exporting to CSV..."
    export_to_csv(csv_friendly_data)

main()
