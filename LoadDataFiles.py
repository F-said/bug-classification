"""
Script to create java files that contain buggy code. Could be done in console, but writing in file for documentation
"""

filetype = "java"

# Path on local machine to data files. Change to your own path if running on local machine
path = "/Users/farukhsaidmuratov/Desktop/SemesterFolder/Summer2019/NSFREUNJIT/Code-and-Data/Pig/"

# Load PigBuggyLines
buggyfiles = open("PigBuggyLines.txt", "r")
path_list = []

# Extract paths of buggy lines
for line in buggyfiles:
    # If line contains file name
    if line[-5:-1] == filetype:
        # If path not already recorded then record
        if line not in path_list:
            path_list.append(line[:-1])

# Write file names of buggy lines and copy file at path to the empty file name
for file in path_list:
    # Find last instance of forward slash to get file name. Add 1 to remove forward slash
    begin_ind = file.rfind("/") + 1
    file_name = file[begin_ind:]

    # create file of "file_name" and copy code from file that is in the path
    open(file_name, "w").writelines([l for l in open(path + file, "r").readlines()])
