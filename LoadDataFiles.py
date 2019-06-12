"""
Script to create java files that contain buggy code. Could be done in console, but writing in file for documentation
purposes
"""

filetype = "java"

# Load PigBuggyLines
buggyfiles = open("PigBuggyLines.txt", "r")
buggyfile_list = []

# Extract file names of buggy lines
for line in buggyfiles:
    # If line contains file name
    if line[-5:-1] == filetype:
        # Find last instance of forward slash to get file name. Add 1 to remove forward slash
        begin_ind = line.rfind("/") + 1
        file_name = line[begin_ind:-1]

        if file_name not in buggyfile_list:
            buggyfile_list.append(file_name)

# Write empty file names of buggy lines
for file in buggyfile_list:
    writefile = open(file, "w")
    writefile.close()
