#!/usr/local/bin/python
import os
import csv
import json


def getColNames(long_data):
    col_names = []
    for row in long_data:
        if row[0] in ['Record ID']:
            col_names = row
    return col_names

def createRecordList(long_data):
    distinct_records = set([])

    for row in long_data:
        if row[0] not in ['Record ID']:
            distinct_records.add(row[0])

    new_list = sorted(list(distinct_records))

    return new_list

def makeCompleteRow(same_rec, record_number, width):
    wide_row = []
    wide_row.append(record_number)
    #same_rec[0] --> T00, same_rec[1] --> T01, same_rec[2] --> Ta2, same_rec[3] --> Tb2
    if same_rec[0] is not None:
        for val in same_rec[0]:
            wide_row.append(val)
    else:
        for _ in range(width):
            wide_row.append('')

    if same_rec[1] is not None:
        for val in same_rec[1]:
            wide_row.append(val)
    else:
        for _ in range(width):
            wide_row.append('')

    if same_rec[2] is not None:
        for val in same_rec[2]:
            wide_row.append(val)
    else:
        for _ in range(width):
            wide_row.append('')

    if same_rec[3] is not None:
        for val in same_rec[3]:
            wide_row.append(val)
    else:
        for _ in range(width):
            wide_row.append('')

    return wide_row


def longToWidePRISM(long_data, long_file, display_back):

    original_keys = getColNames(long_data)

    #go back to beginning of file
    long_file.seek(0)
    long_data = csv.reader(long_file)

    #all distinct record ids
    ### Note: records are not in same order occasioanlly -> dummy records can be mixed in
    same_record_list = createRecordList(long_data)

    #all records indexed at same record id with different timepoints
    record_list = [[None] * 4 for _ in range(len(same_record_list))]

    timepoints = ['T00', 'Ta1', 'Tb1', 'Tc1']
    timepoints_full = ['T00', 'Ta1', 'Tb1', 'Tc1']

    #go back to beginning of file, skipping first row (column names)
    long_file.seek(0)
    next(long_data)

    newkeys = []
    isLongData = False

    #declare full data matrix: store values for new csv
    #new_data = [['' for _ in range(((len(original_keys)-2)*4)+1)] for _ in range(len(same_record_list)+1)]
    new_data = [[''] for _ in range(len(same_record_list)+1)]

    #make record_list
    for row in long_data:

        #check if file is in long data format (has an event name attribute)
        for key in original_keys:
            if key == 'Event Name':
                isLongData = True

        if isLongData == False:
            return 'File is not in long format.'

        #make new keys with appended timepoints
        newkeys = ['Record ID']
        for t in timepoints:
            for og_key in original_keys:
                if og_key not in ['Record ID', 'Event Name']:
                    # name displays timepoint at the back
                    if display_back:
                        newkeys.append(og_key + '_' + t)
                    else:
                        newkeys.append(t + '_' + og_key)

        #put column name in full data matrix
        new_data[0] = newkeys;

        new_row = []
        for val in row:
            new_row.append(val)

        #map the row to the corresponding slot in record_list[i][j] where i is the record number and j is the timepoint
        index1 = -1
        index2 = -1
        for rec in same_record_list:
            if row[0] == rec:
                index1 = same_record_list.index(rec)
                new_row.remove(rec)
                for time in timepoints_full:
                    if row[1] == time:
                        index2 = timepoints_full.index(time)
                        new_row.remove(time)

        record_list[index1][index2] = new_row

    #make one row for same record ID
    for index, same_rec in enumerate(record_list):
        wide_row = makeCompleteRow(same_rec, same_record_list[index], len(original_keys)-2)
        new_data[index+1] = wide_row

    return new_data

def longToWideOASIS(long_data, long_file, display_back):

    original_keys = getColNames(long_data)

    #go back to beginning of file
    long_file.seek(0)
    long_data = csv.reader(long_file)

    #all distinct record ids
    ### Note: records are not in same order occasioanlly -> dummy records can be mixed in
    same_record_list = createRecordList(long_data)

    #all records indexed at same record id with different timepoints
    record_list = [[None] * 4 for _ in range(len(same_record_list))]

    timepoints = ['T00', 'T01', 'Ta2', 'Tb2']
    timepoints_full = [['T00 (Arm 1: Flower)', 'T00 (Arm 2: Edible)', 'T00 (Arm 3: Control)'],
     ['T01 (Arm 1: Flower)', 'T01 (Arm 2: Edible)', 'T01 (Arm 3: Control)'],
     ['Ta2 (Arm 1: Flower)', 'Ta2 (Arm 2: Edible)', 'Ta2 (Arm 3: Control)'],
     ['Tb2 (Arm 1: Flower)', 'Tb2 (Arm 2: Edible)', 'Tb2 (Arm 3: Control)']]

    #go back to beginning of file, skipping first row (column names)
    long_file.seek(0)
    next(long_data)

    newkeys = []
    isLongData = False

    #declare full data matrix: store values for new csv
    #new_data = [['' for _ in range(((len(original_keys)-2)*4)+1)] for _ in range(len(same_record_list)+1)]
    new_data = [[''] for _ in range(len(same_record_list)+1)]

    #make record_list
    for row in long_data:

        #check if file is in long data format (has an event name attribute)
        for key in original_keys:
            if key == 'Event Name':
                isLongData = True

        if isLongData == False:
            return 'File is not in long format.'

        #make new keys with appended timepoints
        newkeys = ['Record ID']
        for t in timepoints:
            for og_key in original_keys:
                if og_key not in ['Record ID', 'Event Name']:
                    # name displays timepoint at the back
                    if display_back:
                        newkeys.append(og_key + '_' + t)
                    else:
                        newkeys.append(t + '_' + og_key)
                elif og_key in ['Event Name']:
                    if display_back:
                        newkeys.append('Arm_' + t)
                    else:
                        newkeys.append(t + '_Arm')

        #put column name in full data matrix
        new_data[0] = newkeys;

        new_row = []
        for val in row:
            new_row.append(val)

        #map the row to the corresponding slot in record_list[i][j] where i is the record number and j is the timepoint
        index1 = -1
        index2 = -1
        for rec in same_record_list:
            if row[0] == rec:
                index1 = same_record_list.index(rec)
                new_row.remove(rec)
                for time in timepoints_full:
                    for group in time:
                        if row[1] == group:
                            index2 = timepoints_full.index(time)
                            new_row.remove(group)
                            if time.index(group) == 0:
                                new_row.insert(0,'Flower')
                            elif time.index(group) == 1:
                                new_row.insert(0,'Edible')
                            elif time.index(group) == 2:
                                new_row.insert(0,'Control')

        record_list[index1][index2] = new_row

    #make one row for same record ID
    for index, same_rec in enumerate(record_list):
        wide_row = makeCompleteRow(same_rec, same_record_list[index], len(original_keys)-2)
        new_data[index+1] = wide_row

    return new_data


def write_new_file(long_filename_path, new_file_name_path):
    try:
        with open(new_file_name_path, 'w') as new_long_file:
            with open(long_filename_path, newline='', encoding='latin-1', errors='strict') as long_file:
                long_data = csv.reader(long_file)
                dw = csv.writer(new_long_file)
                for row in long_data:
                    new_row = []
                    for cell in row:
                        record = cell.encode('ascii', 'ignore').decode('utf-8').strip('\"')
                        new_row.append(record)
                    dw.writerow(new_row)
                new_long_file.close()
    except UnicodeDecodeError:
        print("Error: UnicodeDecodeError")


def checkWhatData(filepath):
    try:
        with open(filepath, newline='') as long_file:
            start = long_file.read(4096)
            dialect = csv.Sniffer().sniff(start)
            return True
    except UnicodeDecodeError:
        return False


def datafix(filename, long_filename, wide_filename, display_back):

    is_oasis = checkWhatData('uploads/' + filename)

    if is_oasis:
        old_file_path = 'uploads/' + filename
    else:
        write_new_file('uploads/' + filename, 'uploads/' + long_filename)
        old_file_path = 'uploads/' + long_filename


    path_to_file_new = 'uploads/' + wide_filename

    try:
        with open(path_to_file_new, 'w') as y:
            with open(old_file_path, newline='') as long_file:
                start = long_file.read(4096)
                dialect = csv.Sniffer().sniff(start)
                long_file.seek(0)
                long_data = csv.reader(long_file)
                if is_oasis:
                    wide_data = longToWideOASIS(long_data, long_file, display_back)
                else:
                    wide_data = longToWidePRISM(long_data, long_file, display_back)

                if wide_data == 'File is not in long format.':
                    return [wide_data, '', '', '', True]

                dw = csv.writer(y)

                for row in wide_data:
                    dw.writerow(row)

                y.close()

        return ['Successfully converted file! Click to download your new wide format file!', '', '', '', False]
    except csv.Error:
        return ['Error: One or more of the following errors occured. Check your file.', 'Input file is not a valid csv.', 'File has no data.', 'File is in incorrect format.', True]
    except FileNotFoundError:
        return ['Error: File not found.', '', '', '', True]
