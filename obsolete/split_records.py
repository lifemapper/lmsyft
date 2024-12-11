"""Class to split a CSV file of records into files grouped by a value in one field."""
from logging import ERROR
import os

from specnet.tools.util.logtools import Logger
from specnet.tools.util.fileop import get_csv_reader, get_csv_writer

ENCODING = "utf-8"


# ...............................................
def usage():
    """Print a usage help message."""
    output = """
    Usage:
        gbifsort infile [split | sort | merge | check]
    """
    print(output)
    exit(-1)


# ..........................................................................
class DataSplitter(object):
    """Class to split CSV files by a value."""

    # ...............................................
    def __init__(self, infname, indelimiter, group_col, logname):
        """Split a large CSV file into individual files grouped by one column.

        Args:
            infname: full pathname to a CSV file containing records to be
                grouped on the value in a field of the records
            indelimiter: separator between fields of a record
            group_col: the column name (for files with a header) or column
                index for the field to be used for grouping
            logname: the basename for a message logger

        Raises:
            Exception: on group_col is not in the input file header.
        """
        self.messyfile = infname
        self._basepath, fname = os.path.split(self.messyfile)
        self._dataname = os.path.basename(fname)
        self._log = Logger(logname, log_path=self._basepath, log_console=True)

        self.indelimiter = indelimiter
        self.group_col = group_col
        self.header = self._get_header()

        self.group_idx = None
        try:
            self.group_idx = int(group_col)
        except ValueError:
            try:
                self.group_idx = self.header.index(group_col)
            except ValueError:
                raise Exception(
                    f"Field {self.group_col} does not exist in header {self.header}")
        # New output files, named by and grouped on group_col
        self._files = {}

    # ...............................................
    def close(self, fname=None):
        """Close one or more output files.

        Args:
            fname: single file to close.
        """
        if fname is not None:
            self._files[fname].close()
            f = {}
            f.pop(fname)
        else:
            for f in self._files.values():
                f.close()
            self._files = {}

    # ...............................................
    def _open_group_file(self, grpval, out_delimiter):
        basefname = f"{self._dataname}_{grpval}.csv"
        grp_fname = os.path.join(self._basepath, basefname)
        writer, outf = get_csv_writer(grp_fname, out_delimiter, ENCODING)
        writer.writerow(self.header)
        self._files[grp_fname] = outf
        return writer

    # ...............................................
    def gather_groupvals(self, fname):
        """Read a CSV file, and track unique group values and the record count for each.

        Args:
            fname: Input data file

        Raises:
            Exception: on failure to get CSVReader.
        """
        try:
            reader, inf = get_csv_reader(self.messyfile, self.indelimiter, ENCODING)
        except Exception:
            raise
        else:
            groups = {}

            grpval = None
            grpcount = 0
            for row in reader:
                try:
                    currval = row[self.group_idx]
                except Exception:
                    self._log.log(
                        f"Failed to get column {self.group_idx} from record "
                        f"{reader.line_num}", refname=self.__class__.__name__,
                        log_level=ERROR
                    )
                else:
                    if grpval is None:
                        grpval = currval
                    if currval != grpval:
                        self._log.log(
                            f"Start new group {currval} on record {reader.line_num}",
                            refname=self.__class__.__name__, log_level=ERROR
                        )
                        try:
                            groups[grpval] += grpcount
                        except KeyError:
                            groups[grpval] = grpcount
                        grpcount = 1
                        grpval = currval
                    else:
                        grpcount += 1
            inf.close()

        try:
            writer, outf = get_csv_writer(fname, self.indelimiter, ENCODING)
        except Exception:
            raise
        else:
            writer.writerow(["groupvalue", "count"])
            try:
                for grpval, grpcount in groups.items():
                    writer.writerow([grpval, grpcount])
            except Exception:
                self._log.log(
                    f"Failed to write record {[grpval, grpcount]} at line"
                    f" {writer.line_num}", refname=self.__class__.__name__,
                    log_level=ERROR
                )
            finally:
                outf.close()

    # ...............................................
    def write_group_files(self, out_delimiter):
        """Split large file into smaller files with records of a single group value.

        Args:
            out_delimiter: field delimiter for output files.

        Raises:
            Exception: on failure to get CSVReader.

        Note:
            * The number of group files must be small enough for the system to
                have them all open at the same time.
            * Use `gather` to evaluate the dataset first.
        """
        try:
            reader, inf = get_csv_reader(self.messyfile, self.indelimiter, ENCODING)
        except Exception:
            raise
        else:
            # Open infile
            self._files[self.messyfile] = inf
            header = next(reader)
            if self.group_idx is None:
                try:
                    self.group_idx = header.index(self.group_col)
                except ValueError:
                    raise Exception(
                        f"Field {self.group_col} does not exist in header {header}"
                    )

            # {groupval: csvwriter}
            groupfiles = {}
            # Read/write each record to a new or existing groupfile
            for row in reader:
                try:
                    grpval = row[self.group_idx]
                except Exception:
                    self._log.log(
                        f"Failed to get column {self.group_idx} from record "
                        f"{reader.line_num}", refname=self.__class__.__name__,
                        log_level=ERROR
                    )
                else:
                    try:
                        wtr = groupfiles[grpval]
                    except KeyError:
                        wtr = self._open_group_file(grpval, out_delimiter)
                        groupfiles[grpval] = wtr
                        wtr.writerow(header)

                    wtr.writerow(row)
            # Close all files
            self.close()

    # ...............................................
    def _get_header(self):
        reader, inf = get_csv_reader(self.messyfile, self.indelimiter, ENCODING)
        header = next(reader)
        inf.close()
        return header

    # # ...............................................
    # def _read_sortvals(self, group_cols):
    #     self._log.log(
    #         f"Gathering unique sort values from file {self.messyfile}",
    #         refname=self.__class__.__name__
    #     )
    #     reader, inf = get_csv_reader(self.messyfile, self.indelimiter, ENCODING)
    #
    #     group_idxs = self._get_sortidxs(reader, group_cols)
    #     sortvals = set()
    #     try:
    #         for row in reader:
    #             vals = []
    #             for idx in group_idxs:
    #                 vals.append(row[idx])
    #             sortvals.add(tuple(vals))
    #     except Exception as e:
    #         self._log.log(
    #             f"Exception reading infile {self.messyfile}: {e}",
    #             refname=self.__class__.__name__, log_level=ERROR
    #         )
    #     finally:
    #         inf.close()
    #
    #     self._log.info(
    #         f"File contained {len(sortvals)} unique sort values",
    #         refname=self.__class__.__name__, log_level=ERROR
    #     )
    #     return sortvals

    # ...............................................
    def test(self, test_fname, outdelimiter):
        """Test merged/sorted file.

        Args:
            test_fname: input file for testing.
            outdelimiter: field delimiter for input file.
        """
        self._log.log(f"Testing file {test_fname}", refname=self.__class__.__name__)
        reccount = 0
        reader, outf = get_csv_reader(test_fname, outdelimiter, ENCODING)
        header = next(reader)
        if header[self.group_idx] != "gbifID":
            self._log.log(
                f"Bad header in {test_fname}", refname=self.__class__.__name__,
                log_level=ERROR
            )

        currid = 0
        for row in reader:
            reccount += 1
            try:
                gbifid = int(row[self.group_idx])
            except IndexError:
                self._log.log(
                    f"Group index {self.group_idx} not present in {len(row)} fields of "
                    f"rec {reader.line_num}",
                    refname=self.__class__.__name__, log_level=ERROR
                )
            except ValueError:
                self._log.log(
                    f"Bad gbifID on rec {reader.line_num}",
                    refname=self.__class__.__name__, log_level=ERROR
                )
            else:
                if gbifid < currid:
                    self._log.log(
                        f"Bad sort gbifID {gbifid} on rec {reader.line_num}",
                        refname=self.__class__.__name__, log_level=ERROR
                    )
                    break
                elif gbifid == currid:
                    self._log.log(
                        f"Duplicate gbifID {gbifid} on rec {reader.line_num}",
                        refname=self.__class__.__name__, log_level=ERROR
                    )
                else:
                    currid = gbifid

        self._log.log(
            f"File contained {reccount} records", refname=self.__class__.__name__)
        outf.close()


# .............................................................................
if __name__ == "__main__":
    # inputFilename, delimiter, group_index, logname)
    import argparse
    parser = argparse.ArgumentParser(
        description=(
            "Group CSV dataset, optionally into separate files, on a given field")
    )
    parser.add_argument(
        "infile", type=str,
        help="Absolute pathname of the input delimited text file"
    )
    parser.add_argument(
        "--input_delimiter", type=str, default="$",
        help="Delimiter between fields for input file"
    )
    parser.add_argument(
        "--output_delimiter", type=str, default="$",
        help="Delimiter between fields for output file(s)"
    )
    parser.add_argument(
        "--group_column", type=str, default="resource_id",
        help="Index or column name of field for data grouping"
    )
    args = parser.parse_args()
    unsorted_file = args.infile
    in_delimiter = args.input_delimiter
    out_delimiter = args.output_delimiter
    group_col = args.group_column

    if not os.path.exists(unsorted_file):
        print(f"Input CSV file {unsorted_file} does not exist")
    else:
        scriptname, ext = os.path.splitext(os.path.basename(__file__))

        pth, fname = os.path.split(unsorted_file)
        dataname, ext = os.path.splitext(fname)
        logname = f"{scriptname}_{dataname}"

        gf = DataSplitter(unsorted_file, in_delimiter, group_col, logname)

        try:
            gf.write_group_files(out_delimiter)
        finally:
            gf.close()
