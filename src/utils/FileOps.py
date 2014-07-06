#!/usr/bin/python


## ------------ Imports ----------- ##
import os
import linked
## ------------ Imports ----------- ##

## ------------ Constants ----------- ##
MAX_LINES_IN_FILE_CHUNK = 2
MAX_LINES_IN_BUFFER = 4  # max number of lines to be kept in buffer
## ------------ Constants ----------- ##

class FileOperations(object):
    """Readd or Write any flat files."""
    def __init__(self, out_file):
        self.buffer_ = []
        self.buffer_now = 0
        self.out_file = out_file

    def read(self, file_):
        pass

    def read_line(self, file_):
        fp = open(file_, 'r')
        line = fp.readline()
        while line:
            if line and line != '\n':
                yield line
            line = fp.readline()

    def write(self, file_, data):
        if isinstance(data, list):
            with open(file_, 'a') as fp:
                fp.writelines(','.join([str(ele) for ele in  data]))
        else:
            with open(file_, 'a') as fp:
                fp.writelines(data)

    def write_buffer(self, file_, data):
        if data:
            if self.buffer_now >= MAX_LINES_IN_BUFFER:
                self.write(self.buffer_)
                self.buffer_ = []
                self.buffer_now = 0
            else:
                self.buffer_.append(data)


class MakeData(FileOperations):
    def __init__(self, in_file):
        self.input_file = in_file
        self.out_file_path = r'/home/local/PALYAM/nsivakumar/BIGDATA/engine'
        self.out_file_templ = "{0}_%04d.csv"# .format(os.path.split(self.input_file)[-1].rstrip('.csv'))
        self.file_name_suffix = 0

        self.col_names = []

    def __call__(self):
        for index, each_line in enumerate(self.read_line(self.input_file)):
            if index == 0:
                self.col_names = each_line.split(',')
                print self.col_names
                continue

            if not index % MAX_LINES_IN_FILE_CHUNK:
                self.file_name_suffix += 1

            out_file_dir = os.path.join(self.out_file_path, os.path.split(self.input_file)[-1].rstrip('.csv'))
            if not os.path.exists(out_file_dir):
                os.makedirs(out_file_dir)

            for col_idx, each_col in enumerate(each_line.split(',')):
                out_file_abs_path = os.path.join(out_file_dir, self.out_file_templ.format(self.col_names[col_idx]) % self.file_name_suffix)
                self.write(out_file_abs_path, [index, each_col])

            if index >= 100:
                break


if __name__ == '__main__':
    """This Bolck is used for Unit Test.
    """
    hadoop = MakeData(r'/home/local/PALYAM/nsivakumar/BIGDATA/engine/sample_4_col.csv')
    hadoop()
