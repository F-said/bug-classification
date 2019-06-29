import pandas as pd
import os
import applyw2v
import shutil

"""
Script to create java files that contain buggy code, concatenate files into one file, and then create csv file of
statements with respective labels.
"""

def loadFiles(filetype, paths, target_files):
    """
    :param filetype: file type that you are loading. For example, "java" for java files, "txt" for text files, etc.
    :param path: path where the source files are located. On local machine:
    /Users/farukhsaidmuratov/Desktop/SemesterFolder/Summer2019/NSFREUNJIT/Code-and-Data/Pig/
    :param target_file: name of file where buggy files are parsed from: PigBuggyLines.txt; pig_the_other_version_buggy_list.txt
    :return:
    """
    index = 0
    for txt_file in target_files:
        # Load PigBuggyLines
        buggyfiles = open(txt_file, "r")
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
            open(file_name, "w").writelines([l for l in open(paths[index] + file, "r").readlines()])

            # Move files into data directory
            shutil.move("/Users/farukhsaidmuratov/PycharmProjects/bug-classification/" + file_name
                        , "/Users/farukhsaidmuratov/PycharmProjects/bug-classification/data/" + file_name)
        # Move to next path
        index += 1

    print("Files loaded")


def concatFiles(write_file):
    """
    :param write_file: name of file to be written
    :return:
    """
    # Path on local machine
    path = "/Users/farukhsaidmuratov/PycharmProjects/bug-classification/data/"

    # Write one java file to contain all code
    data = open(write_file, "w")

    # For each file in data
    for filename in os.listdir('data'):
        # Write it to the data file
        data.writelines([l for l in open(path + filename, "r").readlines()])

    data.close()
    print(write_file, " created")

def labelBugs(target_files, save=True):
    """
    :param target_file: Name of file to parse statements. Should be same as "write_file" in concatFiles function
    :return:
    """
    # Path on local machine to data
    data_path = "/Users/farukhsaidmuratov/PycharmProjects/bug-classification/data/"

    # Create data frame to hold code line and bug classification (1 for bug, 0 for no bug)
    buggy_code_df = pd.DataFrame(columns=['File', 'Statement', 'Bug'])

    # Create dict of lists of buggy lines with keys being file names
    filetype = "java"

    for txt_file in target_files:
        buggyfiles = open(txt_file, "r")
        buggy_dict = {}

        # Extract paths of buggy lines
        for file in buggyfiles:
            # If line contains file name
            if file[-5:-1] == filetype:
                # Record its name
                begin_ind = file.rfind("/") + 1
                file_name = file[begin_ind:-1]

                # If file not already recorded then record it in dict with buggy line of code as first element in list
                if file_name not in buggy_dict.keys():
                    buggy_lines = list()
                    buggy_lines.append(buggyfiles.readline())
                    buggy_dict[file_name] = buggy_lines
                # If already recorded, then just append buggy line to list
                else:
                    buggy_dict[file_name].append(buggyfiles.readline())

        # Tokenize the lists in the dict
        for file in buggy_dict.keys():
            sents = [applyw2v.tokenizeCode(line) for line in buggy_dict[file]]
            filtered_sentences = [sent for sent in sents if applyw2v.filterNonsense(sent)]
            joined_sentences = applyw2v.joinstatement(filtered_sentences)

            buggy_dict[file] = joined_sentences

        # For each line in data, classify as bug if line of code appears in buggy dictionary, with file name as the key and
        # with line of code in the list
        for filename in os.listdir('data'):
            # Open file to read and record statements
            with open(data_path + filename, 'r') as f:
                lines = f.readlines()
                # Tokenize
                sents = [applyw2v.tokenizeCode(line) for line in lines]
                filtered_sentences = [sent for sent in sents if applyw2v.filterNonsense(sent)]
                joined_sentences = applyw2v.joinstatement(filtered_sentences)

                # For each statement
                for sentence in joined_sentences:
                    # If statement shows up in buggy code
                    if sentence in buggy_dict[filename]:
                        # Get buggy lines of this specific file
                        file_bugs = buggy_code_df.loc[buggy_code_df["File"] == filename]

                        # If statement already appeared in file, classify statement in that file as non-bug
                        # and discard code from buggy_dict, so that all future statements will be classified as non-bugs
                        if sentence in list(file_bugs["Statement"]):
                            # Append as non-bug
                            buggy_code_df = buggy_code_df.append({'File': filename, 'Statement': sentence, 'Bug': 0},
                                                                 ignore_index=True)

                            # Classify previously recorded statement as non-bug by removing previous and adding new
                            drop_ind = buggy_code_df[(buggy_code_df["File"] == filename) & (buggy_code_df["Bug"] == 1) &
                                                    (buggy_code_df["Statement"].astype(str) == str(sentence))].index
                            buggy_code_df = buggy_code_df.drop(drop_ind)

                            buggy_code_df = buggy_code_df.append({'File': filename, 'Statement': sentence, 'Bug': 0},
                                                                 ignore_index=True)

                            buggy_dict[filename].remove(sentence)
                        # Otherwise append as bug
                        else:
                            buggy_code_df = buggy_code_df.append({'File': filename, 'Statement': sentence, 'Bug': 1},
                                                                 ignore_index=True)
                    else:
                        # If a regular line of code, append as non-bug
                        buggy_code_df = buggy_code_df.append({'File': filename, 'Statement': sentence, 'Bug': 0},
                                                             ignore_index=True)
    if save:
        # Save as csv file
        buggy_code_df.to_csv(path_or_buf="bug-classification.csv", index=False)
    print("Buggy code csv created")

def main():
    # Text files that contain buggy code
    buggy = ["PigBuggyLines.txt", "pig_the_other_version_buggy_list.txt"]
    # Respective paths path
    path = ["/Users/farukhsaidmuratov/Desktop/SemesterFolder/Summer2019/NSFREUNJIT/Code-and-Data/Pig/",
            "/Users/farukhsaidmuratov/Desktop/SemesterFolder/Summer2019/NSFREUNJIT/extra-data-and-example-code/extra-data/Pig/"]
    # Type of file
    file_type = "java"
    # File to save all buggy lines to
    target = "PigCode.java"

    # Load Data files
    loadFiles(file_type, path, buggy)
    # Write all files into one file
    concatFiles(target)
    # Apply w2v model
    applyw2v.main()
    # Create csv file of buggy lines
    labelBugs(buggy)


if __name__ == '__main__':
    main()
