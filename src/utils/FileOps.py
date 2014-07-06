#!/usr/bin/python


## ------------ Imports ----------- ##
import os
from . import chains
## ------------ Imports ----------- ##

## ------------ Constants ----------- ##
MAX_LINES_IN_FILE_CHUNK = 2
MAX_LINES_IN_BUFFER = 4  # max number of lines to be kept in buffer
## ------------ Constants ----------- ##


class File_(object):
    """Readd or Write any flat files."""
    def __init__(self, out_file):
        self.buffer_ = []
        self.buffer_now = 0
        self.out_file = out_file

    def read_headers(self, file_):
        with open(file_, 'r') as fp:
            return fp.readline()

    def read_line_by_line(self, file_):
        fp = open(file_, 'r')
        line = fp.readline()
        # Skip reading the Headers
        line = fp.readline()
        while line:
            if line.strip('\n'):
                yield line
            line = fp.readline()

    def write(self, file_, data):
        if isinstance(data, list):
            with open(file_, 'a') as fp:
                fp.writelines(','.join([str(ele) for ele in  data]))
        else:
            with open(file_, 'a') as fp:
                fp.writelines(data)

class Buffers(object):
    def __init__(self, col_name):
        self.buff = []
        self.col_name = col_name

    def __repr__(self):
        return "Buffers({0})".format(self.col_name)

class CSV_(File_):
    def __init__(self):
        self.col_headers = []
        self.infile = ''
        self.file_count = 0
        self.db_path = r'/home/local/PALYAM/nsivakumar/BIGDATA/dbs'
        self.outfile_temp = "{0}_{1}_%06d.csv".format(os.path.split(self.infile)[-1].rstrip('.csv'), '{1}')

        self.db_feed_data = {'DATABASE': None}

    def make_db(self, infile):
        self.infile = infile

        for header_line in self.read_headers(self.infile):
            self.col_headers = [ele.strip() for ele in_line.split(',')]

        buffers = [Buffers(_col_name) for _col_name in self.col_headers]

        for _row_id, _lines in enumerate(self.read_line_by_line(self.infile)):

            if not _row_id % MAX_LINES_IN_FILE_CHUNK and not len(buffers[0]):
                ## flush it towards the storage files.
                self.file_count += 1
                for buffer_ in buffers:

                    _file_chunk_name = self.write(self.outfile_temp.format(buffer_.col_name) % (self.file_count)

                    if self.write(_file_chunk_name, buffer_.buff):
                        ## Empty the buffer list after the flush
                        buffer_.buff = []

                        ## Compute hash for the just generated file.
                        pass

                        ## add the info of this generated file in to the Linked list as a Node.
                        #TODO: make sure of the Metadata.
                        pass

                    else:
                        raise Exception("Unable to Flush the Buffer: {0}".format(buffer_))

            _split_lines = [ele.strip() for ele in _lines.split(',')]

            if len(_split_lines) != len(buffers):
                raise Exception("Columns in the CSV file: {0} are not matching".format(self.infile))

            for index, col_data in enumerate(_split_lines):
                buffers[index].buff.append(col_data)
