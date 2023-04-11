"""Module containing file functions."""

import glob
import os

SHP_EXT = "shp"
SHP_EXTENSIONS = [
    ".shp", ".shx", ".dbf", ".prj", ".sbn", ".sbx", ".fbn", ".fbx", ".ain",
    ".aih", ".ixs", ".mxs", ".atx", ".shp.xml", ".cpg", ".qix"],


# ...............................................
def ready_filename(full_filename, overwrite=False):
    """Prepare the specified file location for writing.

    Args:
        full_filename: Full path to filename to check.
        overwrite: True to delete the full_filename if it exists.

    Returns:
        boolean flag, true if ready to write, false if not.

    Raises:
        Exception: on filename to check is None.
        Exception: on parent directories do not exist and cannot be created.
    """
    if full_filename is None:
        raise Exception("full_filename is `None`")

    if os.path.exists(full_filename):
        if overwrite:
            success, msg = delete_file(full_filename)
            if not success:
                raise Exception("Unable to delete {}: {}".format(
                    full_filename, msg))
            return True
        return False

    pth, _ = os.path.split(full_filename)
    try:
        os.makedirs(pth, 0o775)
    except OSError:
        pass

    if os.path.isdir(pth):
        return True

    raise Exception(
        f"Failed to create dirs {pth}, checking for ready_filename {full_filename}")


# ...............................................
def delete_file(full_filename, delete_dir=False):
    """Delete file if it exists, delete directory if it becomes empty.

    Args:
        full_filename: Full path to filename to check.
        delete_dir: True to delete the parent directory if empty after file deletion.

    Returns:
        success: True on successful deletion, False on failure.
        msg: An error message if success is False

    Note:
        If file is shapefile, delete all related files
    """
    success = True
    msg = ""
    if full_filename is None:
        msg = "Cannot delete file `None`"
    else:
        pth, _ = os.path.split(full_filename)
        if full_filename is not None and os.path.exists(full_filename):
            base, ext = os.path.splitext(full_filename)
            if ext == SHP_EXT:
                similar_file_names = glob.glob(f"{base}.*")
                try:
                    for sim_file_name in similar_file_names:
                        _, sim_ext = os.path.splitext(sim_file_name)
                        if sim_ext in SHP_EXTENSIONS:
                            os.remove(sim_file_name)
                except Exception as e:
                    success = False
                    msg = f"Failed to remove {sim_file_name}, {e}"
            else:
                try:
                    os.remove(full_filename)
                except Exception as e:
                    success = False
                    msg = f"Failed to remove {full_filename}, {e}"
            if delete_dir and len(os.listdir(pth)) == 0:
                try:
                    os.removedirs(pth)
                except Exception as e:
                    success = False
                    msg = f"Failed to remove {pth}, {e}"
    return success, msg


# .............................
# def zip_files(fnames, zip_fname):
#     """Returns a wrapper around a tar gzip file stream
#
#     Args:
#         base_name: (optional) If provided, this will be the prefix for the
#             names of the shape file"s files in the zip file.
#     """
#     tg_stream = StringIO()
#     zipf = zipfile.ZipFile(
#         tg_stream, mode="w", compression=zipfile.ZIP_DEFLATED,
#         allowZip64=True)
#
#     for fname in fnames:
#         ext = os.path.splitext(fname)[1]
#         zipf.write(fname, "{}.{}".format(zip_fname, ext))
#     zipf.close()
#
#     tg_stream.seek(0)
#     ret = "".join(tg_stream.readlines())
#     tg_stream.close()
#     return ret
