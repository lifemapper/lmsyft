"""Process a Darwin Core Archive and add the data to the Specify Cache."""
import csv
import glob
import io
import os
import requests
import shutil
import xml.etree.ElementTree as ET
import zipfile

import flask_app.sp_cache.solr_controller as controller
import flask_app.sp_cache.config as config


DEFAULT_META_FILENAME = 'meta.xml'
DEFAULT_NAMESPACE = 'http://rs.tdwg.org/dwc/terms/'
TARGET_NAMESPACE = '{http://rs.tdwg.org/dwc/text/}'
CSV_PARAMS = [
    ('delimiter', 'fieldsTerminatedBy'),
    ('lineterminator', 'linesTerminatedBy'),
    ('quotechar', 'fieldsEnclosedBy'),
]
MY_PARAMS = [
    ('encoding', 'encoding'),
    ('num_header_rows', 'ignoreHeaderLines'),
    ('row_type', 'rowType'),
]
VALIDATE_KEYS = {
    'latitude': float,
    'decimallatitude': float,
    'longitude': float,
    'decimallongitude': float
}

SERVER_URL = os.environ['FQDN']
RESOLVER_ENDPOINT_URL = '{}/api/v1/resolve'.format(SERVER_URL)
SOLR_POST_LIMIT = 1000
# Valid fields for identifier in reverse preference order (best option last)
VALID_IDENTIFIERS = ['occurrenceID', 'globaluniqueidentifier']


# .....................................................................................
def get_full_tag(tag, namespace=TARGET_NAMESPACE):
    """Get the full tag, including namespace, to search for.

    Args:
        tag (str): An XML tag without namespace.
        namespace (str): The namespace to prepend to the tag string.

    Returns:
        str - A full XML tag including namespace.
    """
    return '{}{}'.format(namespace, tag)


# .....................................................................................
def post_results(post_recs, collection_id, mod_time):
    """Post results to Solr index and resolver.

    Args:
        post_recs (list of dict): A list of dictionaries representing records to post
            to solr.
        collection_id (str): An identifier associated with the collection containing
            these records.
        mod_time (tuple): A tuple of year, month, day modification time.
    """
    _ = controller.update_collection_occurrences(collection_id, post_recs)
    resolver_recs = []
    for rec in post_recs:
        try:
            resolver_recs.append(
                {
                    'id': rec.get('occurrenceID', 'no_id'),
                    'dataset_guid': collection_id,
                    # Use collection ID if no collection code
                    'who': rec.get('collectionCode', collection_id),
                    'what': rec.get('basisOfRecord', 'unknown'),
                    'when': '{}-{}-{}'.format(*mod_time),
                    'url': '{}api/v1/sp_cache/collection/{}/occurrences/{}'.format(
                        SERVER_URL,
                        collection_id,
                        rec['id']
                     )
                }
            )
        except KeyError:  # Raised if the record ID is None, we can't use it if so
            pass
    resolver_response = requests.post(RESOLVER_ENDPOINT_URL, json=resolver_recs)
    print(resolver_response)


# .....................................................................................
def process_meta_xml(meta_contents):
    """Process the meta.xml file contents.

    Args:
        meta_contents (str): The string contents of a meta.xml file.

    Returns:
        (str, dict, dict, dict) - An occurrence filename, field dictionary, our
            parmeters dictionary, csv parameters dictionary.
    """
    # Convert bytes to string and process xml
    # Need to return occurrence filename and lookup
    occurrence_filename = None
    fields = {}
    # extensions = []
    constants = []
    root_el = ET.fromstring(meta_contents)
    core_el = root_el.find(get_full_tag('core'))
    # Process core
    my_params = {'encoding': 'utf8', 'num_header_rows': 0}
    csv_reader_params = {}
    for my_key, core_att in MY_PARAMS:
        if core_att in core_el.attrib.keys():
            my_params[my_key] = core_el.attrib[core_att]
    for csv_key, core_att in CSV_PARAMS:
        if core_att in core_el.attrib.keys():
            csv_reader_params[csv_key] = core_el.attrib[core_att]

    occurrence_filename = core_el.find(
        get_full_tag('files')
    ).find(get_full_tag('location')).text
    for field_el in core_el.findall(get_full_tag('field')):
        # Process field
        if 'index' in field_el.attrib.keys():
            fields[
                int(field_el.attrib['index'])
            ] = field_el.attrib['term'].split(DEFAULT_NAMESPACE)[1]
        else:
            constants.append((field_el.attrib['term'], field_el.attrib['default']))
    for id_el in core_el.findall(get_full_tag('id')):
        fields[int(id_el.attrib['index'])] = 'id'
    return occurrence_filename, fields, my_params, csv_reader_params


# .....................................................................................
def process_occurrence_file(
    occurrence_file,
    fields,
    my_params,
    csv_reader_params,
    collection_id,
    mod_time
):
    """Process an occurrence file.

    Args:
        occurrence_file (file-like object): File-like object of occurrence data.
        fields (dict): A dictionary of fields in the occurrence file.
        my_params (dict): A dictionary of our parameters for processing data.
        csv_reader_params (dict): A dictionary of parameters to forward to csv.reader.
        collection_id (str): An identifier for the collection these data are from.
        mod_time (tuple): Modification time information tuple.
    """
    for _ in range(int(my_params['num_header_rows'])):
        next(occurrence_file)
    reader = csv.reader(occurrence_file, **csv_reader_params)
    solr_post_recs = []
    for row in reader:
        rec = {fields[idx]: row[idx] for idx in fields.keys()}
        rec['collection_id'] = collection_id
        # Set identifier field
        for ident_field in VALID_IDENTIFIERS:
            if ident_field in rec.keys() and len(rec[ident_field]) > 0:
                rec['identifier'] = rec[ident_field]
        # Remove empty strings
        pop_keys = []
        for k in rec.keys():
            if rec[k] == '':
                pop_keys.append(k)
        for k in pop_keys:
            rec.pop(k)
        if validate_rec(rec):
            solr_post_recs.append(rec)
        if len(solr_post_recs) >= SOLR_POST_LIMIT:
            post_results(solr_post_recs, collection_id, mod_time)
            solr_post_recs = []
    if len(solr_post_recs) > 0:
        post_results(solr_post_recs, collection_id, mod_time)


# .....................................................................................
def process_dwca(dwca_filename, collection_id, meta_filename=DEFAULT_META_FILENAME):
    """Process the Darwin Core Archive.

    Args:
        dwca_filename (str): A filename for a DarwinCore Archive file.
        collection_id (str): An identifier for the collection containing these data.
        meta_filename (str): A file contained in the archive containing metadata.
    """
    # Get the last segment which should contain time information
    time_parts = dwca_filename.split('-post-')[-1].split('_')
    # Year month day are the first three parts to time information
    mod_time = time_parts[:3]

    with zipfile.ZipFile(dwca_filename) as zip_archive:
        meta_xml_contents = zip_archive.read(meta_filename)
        occurrence_filename, fields, my_params, csv_reader_params = process_meta_xml(
            meta_xml_contents
        )
        process_occurrence_file(
            io.TextIOWrapper(
                zip_archive.open(
                    occurrence_filename, mode='r'
                )
            ), fields, my_params, csv_reader_params, collection_id, mod_time
        )


# .....................................................................................
def process_dwca_directory(in_directory, out_directory, error_directory):
    """Process the Darwin Core Archive files in the input directory and move them.

    Args:
        in_directory (str): A directory of Darwin Core Archive files to ingest.
        out_directory (str): A directory to store processed DwCA files.
        error_directory (str): A directory to store problem DWCA files.
    """
    glob_path = os.path.join(in_directory, 'collection-*.zip')
    dwca_files = glob.glob(glob_path)
    dwca_files.sort(key=os.path.getmtime, reverse=True)
    for dwca_filename in dwca_files:
        print('Processing file: {}'.format(dwca_filename))
        # File pattern is: "collection-{collection_id}-post-YYYY_MM_DD_HH_MM_SS.zip"
        # Get collection id, handle scenario where -post- is present within
        # Start after "collection-"
        collection_id = os.path.basename(dwca_filename)[11:].split('-post-')[0]
        try:
            process_dwca(dwca_filename, collection_id)
            shutil.move(
                dwca_filename,
                os.path.join(out_directory, os.path.basename(dwca_filename))
            )
        except Exception as err:
            print('Failed to process {}, {}'.format(dwca_filename, err))
            shutil.move(
                dwca_filename,
                os.path.join(error_directory, os.path.basename(dwca_filename))
            )


# .....................................................................................
def validate_rec(rec):
    """Validate a record before adding to solr.

    Args:
        rec (dict): A record to post.

    Returns:
        bool: An indication if the record is valid.
    """
    try:
        for k in rec.keys():
            # If we have a validate method for a key, try it
            if k.lower() in VALIDATE_KEYS:
                VALIDATE_KEYS[k.lower()](rec[k])
        return True
    except ValueError:
        return False


# .....................................................................................
def main():
    """Main method for script."""
    process_dwca_directory(
        config.DWCA_PATH,
        config.PROCESSED_DWCA_PATH,
        config.ERROR_DWCA_PATH
    )


# .....................................................................................
if __name__ == '__main__':
    main()
