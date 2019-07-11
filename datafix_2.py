#!/usr/local/bin/python
import os
import csv
import json


def getColNames(long_data):
    col_names = []
    record_id_labels = ['Record ID', 'record_id', 'PID is the five digit pin + entry date + random number', 'pid']
    for row in long_data:
        if row[0] in record_id_labels or row[1] in record_id_labels:
            col_names = row
            return col_names
    return col_names

def createRecordList(long_data, otlfb):
    distinct_records = set([])
    report_flag = False
    for row in long_data:
        if otlfb:
            if row[0] in ['Subject ID', 'subid', '']:
                report_flag = True
            elif row[2] not in ['Subject ID', 'subid', ''] and not report_flag:
                distinct_records.add(row[2])
            elif report_flag and row[0] not in ['Subject ID', 'subid', '']:
                distinct_records.add(row[0])
        else:
            if row[0] not in ['Record ID', 'record_id']:
                distinct_records.add(row[0])

    new_list = sorted(list(distinct_records), key=lambda x: float(x))

    return new_list

def makeCompleteRow(same_rec, record_number, width, num_timepoints):
    wide_row = []

    #same_rec[0] --> T00, same_rec[1] --> T01, same_rec[2] --> Ta2, same_rec[3] --> Tb2
    wide_row.append(record_number)

    for index in range(num_timepoints):
        if same_rec[index] is not None:
            for val in same_rec[index]:
                wide_row.append(val)
        else:
            for _ in range(width):
                wide_row.append('')

    return wide_row


def longToWidePRISM(long_data, long_file, display_back, isRaw):

    original_keys = getColNames(long_data)

    #go back to beginning of file
    long_file.seek(0)
    long_data = csv.reader(long_file)

    #all distinct record ids
    ### Note: records are not in same order occasioanlly -> dummy records can be mixed in
    same_record_list = createRecordList(long_data, False)

    #all records indexed at same record id with different timepoints
    record_list = [[None] * 4 for _ in range(len(same_record_list))]

    timepoints = ['T00', 'Ta1', 'Tb1', 'Tc1']
    if isRaw:
        timepoints_full = ['t00_arm_1', 'ta1_arm_1', 'tb1_arm_1', 'tc1_arm_1']
    else:
        timepoints_full = ['T00', 'Ta1', 'Tb1', 'Tc1']

    #go back to beginning of file, skipping first row (column names)
    long_file.seek(0)
    next(long_data)

    newkeys = []
    isLongData = False

    #declare full data matrix: store values for new csv
    new_data = [[''] for _ in range(len(same_record_list)+1)]

    #this iteration skips the first row (variable names)
    #make record_list
    for row in long_data:

        #check if file is in long data format (has an event name attribute)
        for key in original_keys:
            if key == 'Event Name' or key == 'redcap_event_name':
                isLongData = True

        if isLongData == False:
            return 'File is not in long format.'

        #make new keys with appended timepoints
        if isRaw:
            newkeys = ['record_id']
        else:
            newkeys = ['Record ID']
        for t in timepoints:
            for og_key in original_keys:
                if og_key not in ['Record ID', 'Event Name', 'redcap_event_name', 'record_id']:
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
        wide_row = makeCompleteRow(same_rec, same_record_list[index], len(original_keys)-2, len(timepoints))
        new_data[index+1] = wide_row

    return new_data

def longToWideOASIS(long_data, long_file, display_back, isRaw):

    original_keys = getColNames(long_data)

    #go back to beginning of file
    long_file.seek(0)
    long_data = csv.reader(long_file)

    #all distinct record ids
    ### Note: records are not in same order occasioanlly -> dummy records can be mixed in
    same_record_list = createRecordList(long_data, False)

    #all records indexed at same record id with different timepoints
    record_list = [[None] * 4 for _ in range(len(same_record_list))]

    timepoints = ['T00', 'T01', 'Ta2', 'Tb2']
    if isRaw:
        timepoints_full = [['t00_arm_1', 't00_arm_2', 't00_arm_3'],
         ['t01_arm_1', 't01_arm_2', 't01_arm_3'],
         ['ta2_arm_1', 'ta2_arm_2', 'ta2_arm_3'],
         ['tb2_arm_1', 'tb2_arm_2', 'tb2_arm_3']]
    else:
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
    new_data = [[''] for _ in range(len(same_record_list)+1)]

    #this iteration skips the first row (variable names)
    #make record_list
    for row in long_data:

        #check if file is in long data format (has an event name attribute)
        for key in original_keys:
            if key == 'Event Name'or key == 'redcap_event_name':
                isLongData = True

        if isLongData == False:
            return 'File is not in long format.'

        #make new keys with appended timepoints
        if isRaw:
            newkeys = ['record_id']
        else:
            newkeys = ['Record ID']
        for t in timepoints:
            for og_key in original_keys:
                if og_key not in ['Record ID', 'Event Name', 'redcap_event_name', 'record_id']:
                    # name displays timepoint at the back
                    if display_back:
                        newkeys.append(og_key + '_' + t)
                    else:
                        newkeys.append(t + '_' + og_key)
                elif og_key in ['Event Name', 'redcap_event_name']:
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
        wide_row = makeCompleteRow(same_rec, same_record_list[index], len(original_keys)-1, len(timepoints))
        new_data[index+1] = wide_row

    return new_data

def longToWideOTLFB(long_data, long_file, display_back, isRaw):

    original_keys = getColNames(long_data)

    #get back to beginning of file
    long_file.seek(0)
    long_data = csv.reader(long_file)

    #all distinct 5 digit pins
    same_record_list = createRecordList(long_data, True)

    #all records indexed at same record id with different timepoints
    #currently only timepoint -> baseline
    record_list = [[None] * 11 for _ in range(len(same_record_list))]

    timepoints = ['T00', 'T01', 'Ta2', 'Ta1', 'M1', 'M2', 'M3', 'M4', 'M5', 'M6']
    if isRaw:
        timepoints_full = ['T00', 'T01', 'Ta2', 'Ta1', 'M1', 'M2', 'M3', 'M4', 'M5', 'M6']
    else:
        timepoints_full = ['T00', 'T01', 'Ta2', 'Ta1', 'M1', 'M2', 'M3', 'M4', 'M5', 'M6']

    #go back to beginning of file, skipping first row (column names)
    long_file.seek(0)
    next(long_data)

    newkeys = []
    isLongData = False

    #declare full data matrix: store values for new csv
    new_data = [[''] for _ in range(len(same_record_list)+1)]

    #this iteration skips the first row
    #make record_list
    for row in long_data:

        #check if file is in long data format (has an event name attribute)
        for key in original_keys:
            if key == 'Timepoint' or key == 'timepoint':
                isLongData = True

        if isLongData == False:
            return 'File is not in long format.'

        #make new keys with appended timepoints

        if isRaw:
            newkeys = ['subid']
        else:
            newkeys = ['Subject ID']

        for t in timepoints:
            for index, og_key in enumerate(original_keys):
                if og_key not in ['Subject ID', 'Timepoint', 'subid', 'timepoint']:
                    # name displays timepoint at the back
                    if display_back:
                        newkeys.append(og_key + '_' + t)
                    else:
                        newkeys.append(t + '_' + og_key)
                elif index > 4:
                    if display_back:
                        newkeys.append(og_key + '_' + t)
                    else:
                        newkeys.append(t + '_' + og_key)


        #put column name in full data matrix
        new_data[0] = newkeys;

        ###works up to here

        #won't put pid and five digit in right place (swap)
        new_row = []
        for val in row:
            new_row.append(val)

        #map the row to the corresponding slot in record_list[i][j] where i is the record number and j is the timepoint
        index1 = -1
        index2 = -1
        for rec in same_record_list:
            if row[2] == rec or row[0] == rec:
                index1 = same_record_list.index(rec)
                new_row.remove(rec)
                for time in timepoints_full:
                    if row[3] == time or row[4] == time:
                        index2 = timepoints_full.index(time)
                        new_row.remove(time)

        record_list[index1][index2] = new_row


    #make one row for same record ID
    data_index = 0;
    for index, same_rec in enumerate(record_list):
        wide_row = makeCompleteRow(same_rec, same_record_list[index], len(original_keys)-2, len(timepoints))
        #makes sure empty row is not added
        if not wide_row:
            data_index = data_index - 1;
        else:
            new_data[data_index+1] = wide_row

        data_index = data_index + 1;

    return new_data



def checkIfRawData(filepath):
    with open(filepath, newline='') as long_file:
        long_data = csv.reader(long_file)
        for row in long_data:
            if row[0] in ['record_id', 'pid']:
                return True
            else:
                return False
    return False

def checkWhatData(filepath):
    try:
        with open(filepath, newline='') as long_file:
            long_data = csv.reader(long_file)
            for row in long_data:
                if row[1] in ['tc1_arm_1', 'Tc1', 'tb1_arm_1', 'Tb1', 'ta1_arm_1', 'Ta1']:
                    return 'PRISM'
                elif row[1] in ['t01_arm_1', 't01_arm_2', 't01_arm_3', 'T01 (Arm 1: Flower)', 'T01 (Arm 2: Edible)', 'T01 (Arm 3: Control)']:
                    return 'OASIS'
                elif row[0] in ['PID is the five digit pin + entry date + random number', 'pid'] or row[1] in ['PID is the five digit pin + entry date + random number', 'pid']:
                    return 'OTLFB'
        return 'Invalid'
    except:
        return 'Invalid'

#change call to datafix in index.py
def datafix(filename, wide_filename, display_back):

    what_data = checkWhatData('uploads/' + filename)

    if what_data == 'Invalid':
        return ['Error: Could not identify as OASIS/PRISM/O-TLFB', '', '', '', True]

    old_file_path = 'uploads/' + filename
    path_to_file_new = 'uploads/' + wide_filename

    isRaw = checkIfRawData(old_file_path)

    try:
        with open(path_to_file_new, 'w') as y:
            with open(old_file_path, newline='') as long_file:
                dialect = csv.Sniffer().sniff(long_file.readline())
                long_file.seek(0)
                long_data = csv.reader(long_file)

                if what_data == 'OASIS':
                    wide_data = longToWideOASIS(long_data, long_file, display_back, isRaw)
                elif what_data == 'PRISM':
                    wide_data = longToWidePRISM(long_data, long_file, display_back, isRaw)
                elif what_data == 'OTLFB':
                    wide_data = longToWideOTLFB(long_data, long_file, display_back, isRaw)

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
