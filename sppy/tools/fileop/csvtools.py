"""Miscellaneous tools for reading and writing CSV files."""
import csv
import subprocess
from sys import maxsize

EXTRA_VALS_KEY = "rest"
REQUIRED_FIELDS = []
CENTROID_FIELD = "CENTROID"


# .............................................................................
def get_line_count(filename):
    """Find total number lines in a file.

    Args:
        filename: file to read the header from

    Returns:
        number of lines in the file.
    """
    cmd = "wc -l {}".format(repr(filename))
    info, _ = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    temp = info.split(b"\n")[0]
    line_count = int(temp.split()[0])
    return line_count


# .............................................................................
def get_header(filename):
    """Get fieldnames from the first line of a CSV file.

    Args:
        filename: file to read the header from

    Returns:
        header: header line of a file.
    """
    header = None
    try:
        f = open(filename, "r", encoding="utf-8")
        header = f.readline()
    except Exception as e:
        print("Failed to read first line of {}: {}".format(filename, e))
    finally:
        f.close()
    return header


# .............................................................................
def get_csv_reader(datafile, delimiter, encoding):
    """Get a CSV DictReader that can handle encoding.

    Args:
        datafile: filename for CSV input.
        delimiter: field separator for input
        encoding: file encoding for input

    Returns:
        reader: a CSV Reader
        f: an open file object.

    Raises:
        Exception: on failure to read or open the datafile.
    """
    try:
        f = open(datafile, "r", encoding=encoding)
        reader = csv.reader(
            f, delimiter=delimiter, escapechar="\\", quoting=csv.QUOTE_NONE)
    except Exception as e:
        raise Exception(f"Failed to read or open {datafile}, ({e})")
    else:
        print(f"Opened file {datafile} for read")
    return reader, f


# .............................................................................
def get_csv_writer(datafile, delimiter, encoding, fmode="w"):
    """Get a CSV writer that can handle encoding.

    Args:
        datafile: filename for CSV output.
        delimiter: field separator for output
        encoding: file encoding for output
        fmode: mode for writing, either write ("w") or append ("a")

    Returns:
        writer: a CSV Writer
        f: an open file object.

    Raises:
        Exception: on invalid file mode.
        Exception: on failure to read or open the datafile.
    """
    if fmode not in ("w", "a"):
        raise Exception("File mode must be 'w' (write) or 'a' (append)")

    csv.field_size_limit(maxsize)
    try:
        f = open(datafile, fmode, encoding=encoding)
        writer = csv.writer(
            f, escapechar="\\", delimiter=delimiter, quoting=csv.QUOTE_NONE)
    except Exception as e:
        raise Exception(f"Failed to read or open {datafile}, ({e})")
    else:
        print(f"Opened file {datafile} for write")
    return writer, f


# .............................................................................
def get_csv_dict_reader(
        datafile, delimiter, encoding, fieldnames=None, ignore_quotes=True):
    """Get a CSV DictReader that can handle encoding.

    Args:
        datafile: filename for CSV input.
        delimiter: field separator for input
        encoding: file encoding for input
        fieldnames: fieldnames for input records
        ignore_quotes: no special processing of quote characters

    Returns:
        reader: a CSV DictReader
        f: an open file object.

    Raises:
        Exception: on failure to read or open the datafile.
    """
    try:
        f = open(datafile, "r", encoding=encoding)
        if fieldnames is None:
            header = next(f)
            tmpflds = header.split(delimiter)
            fieldnames = [fld.strip() for fld in tmpflds]
        if ignore_quotes:
            dreader = csv.DictReader(
                f, fieldnames=fieldnames, quoting=csv.QUOTE_NONE,
                escapechar="\\", restkey=EXTRA_VALS_KEY, delimiter=delimiter)
        else:
            dreader = csv.DictReader(
                f, fieldnames=fieldnames, restkey=EXTRA_VALS_KEY,
                escapechar="\\", delimiter=delimiter)

    except Exception as e:
        raise Exception(f"Failed to read or open {datafile}, ({e})")
    else:
        print(f"Opened file {datafile} for dict read")
    return dreader, f


# .............................................................................
def get_csv_dict_writer(datafile, delimiter, encoding, fieldnames, fmode="w"):
    """Get a CSV writer that can handle encoding.

    Args:
        datafile: filename for CSV output.
        delimiter: field separator for output
        encoding: file encoding for output
        fieldnames: fieldnames for output records
        fmode: mode for writing, either write ("w") or append ("a")

    Returns:
        writer: a CSV DictWriter
        f: an open file object.

    Raises:
        Exception: on invalid file mode.
        Exception: on failure to read or open the datafile.
    """
    if fmode not in ("w", "a"):
        raise Exception("File mode must be 'w' (write) or 'a' (append)")

    csv.field_size_limit(maxsize)
    try:
        f = open(datafile, fmode, encoding=encoding)
        writer = csv.DictWriter(
            f, fieldnames=fieldnames, delimiter=delimiter, escapechar="\\",
            quoting=csv.QUOTE_NONE)
    except Exception as e:
        raise Exception(f"Failed to read or open {datafile}, ({e})")
    else:
        print(f"Opened file {datafile} for dict write")
    return writer, f


# ...............................................
def makerow(rec, outfields):
    """Create a row for CSV output.

    Args:
        rec: dictionary record of fieldnames and values
        outfields: fieldnames for output

    Returns:
        a row formatted as a string for writing to a CSV output file.
    """
    row = []
    for fld in outfields:
        try:
            val = rec[fld]
            if val in (None, "None"):
                row.append("")
            else:
                if isinstance(val, str) and val.startswith("\""):
                    val = val.strip("\"")
                row.append(val)
        # Add output fields not present in record
        except Exception:
            row.append("")
    return row


# ...............................................
def getLine(csvreader, recno):
    """Return a line while keeping track of the line number and errors.

    Args:
        csvreader: a csv.reader object opened with a file
        recno: the current record number

    Returns:
        line: current line number of the csvfile.
        recno: current record number of the csvfile.
    """
    success = False
    line = None
    while not success and csvreader is not None:
        try:
            line = next(csvreader)
            if line:
                recno += 1
            success = True
        except OverflowError as e:
            recno += 1
            print(f"Overflow on record {recno}, line {csvreader.line_num} ({e})")
        except StopIteration:
            print(f"EOF after record {recno}, line {csvreader.line_num}")
            success = True
        except Exception as e:
            recno += 1
            print(f"Bad record on record {recno}, line {csvreader.line_num} ({e})")

    return line, recno
