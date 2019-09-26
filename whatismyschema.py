#!/bin/env python3
# coding: utf8


import argparse
import logging
import pandas as pd
import swifter


class SchemaDiscovery:
    """Given a *correct* CSV file, discovers the best fitting tight schema.

    Args:
        table_name (str): table_name to use on the SQL script.abs
        file_path (str): path to csv file (full or relative path).
        skip_lines (uint): number of lines to skip from start of CSV.
                           Default = 0.
        separator (str): attribute separator for CSV. Default = ','.
        null (str): string that represents a NULL value.
    """

    def __init__(self, table_name, file_path, skip_lines=0, separator=',',
                 null=''):
        """Please see help(SchemaDiscovery) for more info"""
        self.file_path = file_path
        self.skip_lines = skip_lines
        self.separator = separator
        self.table_name = table_name

    def run(self):
        """Discovers the schema.

        Returns:
            str: returns the SQL string to create the table using the schema.
        """
        logging.info('!@@ Started SchemaDiscovery @@!')

        logging.info('Reading CSV')
        df = self.__read_csv()

        logging.info('Starting type discovery')
        types = df.swifter.apply(self.__discover_type, axis=0)

        logging.info('Translating to SQL create table statement')
        return self.__to_sql(types)

    def __read_csv(self):
        """Reads the CSV file and stores it in a Pandas DataFrame

        Returns:
            DataFrame: pandas dataframe
        """
        df = pd.read_csv(
                self.file_path,
                dtype=str,
                sep=self.separator,
                skiprows=self.skip_lines,
                index_col=False)
        return df

    def __discover_type(self, values):
        """Given a list, discovers the best fitting type for it.

        Args:
            values (list of strings): list of strings.
        """
        return pd.Series(["string"])

    def __to_sql(self, df):
        """Translates the DataFrame to a SQL create table.

        Example:
            DataFrame:
                col1 | col2 | col3
                str  | int  | real

            Returns:
                CREATE TABLE <table_name> (
                    "col1" string,
                    "col2" integer,
                    "col3" real
                );

        Returns:
            str: sql create table statement.
        """
        columns = df.columns

        sql = f"""CREATE TABLE \"{self.table_name}\" ("""

        for col in columns:
            sql += f"""\n\t"{col}" {df[col].iloc[0]},"""

        sql = sql[:-1]  # remove last comma
        sql += "\n);"

        return sql


def main():
    # ----- Logging Info -----
    logging.basicConfig(
        filename='/tmp/schema_discovery.log',
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
        print(discoverer.run())


if __name__ == '__main__':
    main()
