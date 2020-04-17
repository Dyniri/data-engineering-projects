import os
import json
import sys

def get_filenames(path):
    """
    Walks through the directories to get all the
    relative file paths
    """
    files = []

    for dirs in os.walk(path):
        if len(dirs[2]) > 0:
            for file in dirs[2]:
                files.append(dirs[0]+'/'+file)

    return files

def load_file(file):
    """
    Loads and parses the json file
    """
    with open(file) as f:
        return json.load(f)

def load_events(path):
    """
    Loads the log files
    """
    files = get_filenames(path)
    data = []
    flat_data = []
    
    for file in files:
        data.append(load_file(file)['events'])
    
    for sublist in data:
        for event in sublist:
            flat_data.append(event)
    
    return flat_data

def load_songs(path):
    """
    Loads the song files
    """
    files = get_filenames(path)
    data = []
    for file in files:
        data.append(load_file(file))
    
    return data
