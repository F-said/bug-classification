import os

"""
Script to create java files that contain buggy code, concatenate files into one file, and then create csv file of
statements with respective labels.
"""

def loadFiles(filetype, path, target_file):
    """
    :param filetype: file type that you are loading. For example, "java" for java files, "txt" for text files, etc.
    :param path: path where the source files are located. On local machine:
    /Users/farukhsaidmuratov/Desktop/SemesterFolder/Summer2019/NSFREUNJIT/Code-and-Data/Pig/
    :param target_file: name of file where buggy files are parsed from: PigBuggyLines.txt; pig_the_other_version_buggy_list.txt
    :return:
    """
    # Load PigBuggyLines
    buggyfiles = open(target_file, "r")
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

    #TODO: MOVE FILES INTO "DATA" DIRECTORY
