#!/bin/env python3
# coding: utf8
#
# WhatIsMySchema
#
# Copyright (c) 2018 Tim Gubner
#
#

import argparse
import logging


class Column:
    """Receives a list of values and determines the best tight type for it
    """

    __slots__ = "name", "null"

    def __init__(self, name, null):
        self.name = name
        self.null = null

    def push(self, attr):
        pass


class Table:
    """Table keeps track of all columns in the csv file.
    Also generates the SQL query to create the table.
    """
    __slots__ = "columns", "name", "null"

    def __init__(self, name, null):
        self.columns = []
        self.name = name
        self.null = null

    def push(self, attrs):
        # If the tuple has more or less attributes than the header
        # raise error, CSV badly formatted
        if len(attrs) > len(self.columns):
            logging.error("More attributes than headers")
            raise Exception("CSV with incorrect format,\
                             more attributes than header.")
        if len(attrs) < len(self.columns):
            logging.error("Less attributes than headers")
            raise Exception("CSV with incorrect format,\
                             more attributes than header.")

        # Push attributes to respective columns
        for attr, col in zip(attrs, self.columns):
            logging.info(f"""Pushing value to column {col.name}""")
            col.push(attr)

    def set_headers(self, headers):
        self.columns = list(
            map(lambda name: Column(name, self.null), headers)
        )

    def to_sql(self):
        pass


class SchemaDiscovery:
    """Given a *correct* CSV file, discovers the best fitting tight schema.
    """
    __slots__ = "file_path", "skip_lines", "separator", "current_line", "table"

    def __init__(self, table_name, file_path, skip_lines, separator, null):
        self.file_path = file_path
        self.skip_lines = skip_lines
        self.separator = separator
        self.current_line = 0
        self.table = Table(table_name, null)

    def run(self):
        logging.info('Started SchemaDiscovery')
        with open(self.file_path, "r") as f:
            # skips the indicated number of lines
            logging.info('Opening file and skipping requested lines')
            for _ in range(self.skip_lines):
                next(f)

            # read the header
            logging.info('Reading Header')
            headers = self.__split_line(next(f))
            self.table.set_headers(headers)

            self.current_line = self.skip_lines + 1

            # read the tuples
            logging.info('Start reading tuples')
            for line in f:
                logging.info(f"""Processing line {self.current_line}""")
                self.table.push(
                    self.__split_line(next(f))
                )
                self.current_line += 1
        self.table.to_sql()

    def __split_line(self, line):
        return line.rstrip('\n').rstrip('\r').split(self.separator)


def main():
    # ----- Logging Info -----
    logging.basicConfig(
        filename='schema_discovery.log',
        filemode='a',
        level=logging.INFO
    )
    logging.Formatter(
        "%(asctime)s| %(name)s - %(levelname)s - %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )
    # ----- Logging Info -----

    parser = argparse.ArgumentParser(
        description="""Determine SQL schema from CSV data."""
    )

    parser.add_argument('files', metavar='FILES', nargs='+',
                        help='CSV files to process.')
    parser.add_argument("-F", "--sep", dest="separator",
                        help="Use <SEPERATOR> as delimiter between columns",
                        default="|")
    parser.add_argument("-B", "--begin", type=int, dest="begin",
                        help="Skips first <BEGIN> rows", default="0")
    parser.add_argument("-N", "--null", dest="null", type=str,
                        help="Interprets <NULL> as NULLs", default="")
    parser.add_argument("-P", "--parallelism", "--parallel",
                        dest="num_parallel", type=int,
                        help="Parallelizes using <NUM_PARALLEL> threads.\
                            If <NUM_PARALLEL> is less than 0 the degree of\
                            parallelism will be chosen.",
                        default="1")
    parser.add_argument("--parallel-chunk-size", dest="chunk_size", type=int,
                        help="Sets chunk size for parallel reading.\
                            Default is 16k lines.",
                        default=str(2**14))

    args = parser.parse_args()

    for f in args.files:
        discoverer = SchemaDiscovery(
            "test",
            f,
            args.begin,
            args.separator,
            args.null
        )
        discoverer.run()


if __name__ == '__main__':
    main()
