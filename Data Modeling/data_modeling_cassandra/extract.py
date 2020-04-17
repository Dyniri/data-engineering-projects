import os
import csv

def get_filenames(path):
    """
    Walks through the directories to get all the
    relative file paths
    """
    file_names = []

    for dirs in os.walk(path):
        if len(dirs[2]) > 0:
            for file in dirs[2]:
                file_names.append(dirs[0]+'/'+file)

    return file_names


def read_file(file):
    """
    Loads the csv file, skipping headers
    """
    with open(file, encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=',')
        data = []
        rowNum = 0

        for row in reader:
            if rowNum > 0:
                data.append(row)

            rowNum = rowNum + 1

        return data


def get_events(path):
    """
    Loads the log files and flattens the list
    """
    files = get_filenames(path)
    data = []
    flattened_data = []

    for file in files:
        data.append(read_file(file))

    for sublist in data:
        for event in sublist:
            flattened_data.append(event)
    
    return flattened_data
