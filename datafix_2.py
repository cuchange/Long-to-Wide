#!/usr/local/bin/python
import os
import csv
import json
import requests
import time
from pathlib import Path
import cgi
import cgitb
cgitb.enable()


def createRecordList(long_data):
    distinct_records = set([])

    for row in long_data:
        distinct_records.add(row['Record ID'])

    return list(distinct_records)

def makeCompleteRow(newkeys, original_keys, same_rec, display_back):
    wide_row = dict.fromkeys(newkeys)
    #same_rec[0] --> T00, same_rec[1] --> T01, same_rec[2] --> Ta2, same_rec[3] --> Tb2
    for og_key in original_keys:
        if og_key not in ['Record ID', 'Event Name']:
            if display_back == 'True':
                if same_rec[0] is not None:
                    wide_row[og_key + '_T00'] = same_rec[0][og_key]
                if same_rec[1] is not None:
                    wide_row[og_key + '_T01'] = same_rec[1][og_key]
                if same_rec[2] is not None:
                    wide_row[og_key + '_Ta2'] = same_rec[2][og_key]
                if same_rec[3] is not None:
                    wide_row[og_key + '_Tb2'] = same_rec[3][og_key]
            else:
                if same_rec[0] is not None:
                    wide_row['T00_' + og_key] = same_rec[0][og_key]
                if same_rec[1] is not None:
                    wide_row['T01_' + og_key] = same_rec[1][og_key]
                if same_rec[2] is not None:
                    wide_row['Ta2_' + og_key] = same_rec[2][og_key]
                if same_rec[3] is not None:
                    wide_row['Tb2_' + og_key] = same_rec[3][og_key]

    return wide_row



def longToWide(long_data, long_file, display_back):


    #all distinct record ids
    ### Note: records are not in same order occasioanlly -> dummy records can be mixed in
    same_record_list = sorted(createRecordList(long_data))

    #all records indexed at same record id with different timepoints
    record_list = [[None] * 4 for _ in range(len(same_record_list))]

    timepoints = ['T00', 'T01', 'Ta2', 'Tb2']
    timepoints_full = [['Baseline (CINC) (Arm 1: Flower)', 'Baseline (CINC) (Arm 2: Edible)', 'Baseline (CINC) (Arm 3: Control)'],
     ['2-Week (CINC) (Arm 1: Flower)', '2-Week (CINC) (Arm 2: Edible)', '2-Week (CINC) (Arm 3: Control)'],
     ['Pre-Use Mobile Lab (Arm 1: Flower)', 'Pre-Use Mobile Lab (Arm 2: Edible)', 'Pre-Use Mobile Lab (Arm 3: Control)'],
     ['Immediate Post-Use Mobile Lab (Arm 1: Flower)', 'Immediate Post-Use Mobile Lab (Arm 2: Edible)', 'Immediate Post-Use Mobile Lab (Arm 3: Control)']]

    long_file.seek(0)
    next(long_data)

    newkeys = []
    original_keys = []
    isLongData = False

    #make record_list
    for row in long_data:

        #makes new header names
        original_keys = list(dict.keys(row))

        for key in original_keys:
            if key == 'Event Name':
                isLongData = True

        if isLongData == False:
            return ['File is not in long format.', '']

        newkeys = ['Record ID']
        for t in timepoints:
            for og_key in original_keys:
                if og_key not in ['Record ID', 'Event Name']:
                    # name displays timepoint at the back
                    if display_back == 'True':
                        newkeys.append(og_key + '_' + t)
                    else:
                        newkeys.append(t + '_' + og_key)

        new_row = {}
        for og_key in original_keys:
            new_row[og_key] = row[og_key]

        #map the row to the corresponding slot in record_list[i][j] where i is the record number and j is the timepoint
        index1 = -1
        index2 = -1
        for rec in same_record_list:
            if row['Record ID'] == rec:
                index1 = same_record_list.index(rec)
                for time in timepoints_full:
                    for group in time:
                        if row['Event Name'] == group:
                            index2 = timepoints_full.index(time)

        record_list[index1][index2] = new_row

    record_dict = {}

    #make one row for same record ID
    for same_rec in record_list:
        wide_row = makeCompleteRow(newkeys, original_keys, same_rec, display_back)
        wide_row['Record ID'] = same_record_list[record_list.index(same_rec)]
        record_dict[same_record_list[record_list.index(same_rec)]] = wide_row

    return [record_dict, newkeys]



def datafix(long_filename, wide_filename, display_back):
    old_file_path = 'uploads/' + long_filename
    path_to_file_new = 'uploads/' + wide_filename

    try:
        with open(path_to_file_new, 'w') as y:
            with open(old_file_path, newline='') as long_file:
                start = long_file.read(4096)
                dialect = csv.Sniffer().sniff(start)
                long_file.seek(0)
                long_data = csv.DictReader(long_file)
                [record_dict, newkeys] = longToWide(long_data, long_file, display_back)
                if record_dict == 'File is not in long format.':
                    return [record_dict, '', '', '', True]
                dw = csv.writer(y)
                dw.writerow(newkeys)
                for key, value in record_dict.items():
                    keys = list(dict.keys(value))
                    dw.writerow(value[val] for val in keys)

                y.close()
        return ['Successfully converted file! Click to download your new wide format file!', '', '', '', False]
    except csv.Error:
        return ['Error: One or more of the following errors occured. Check your file.', 'Input file is not a valid csv.', 'File has no data.', 'File is in incorrect format.', True]
    except FileNotFoundError:
        return ['Error: File not found.', '', '', '', True]
