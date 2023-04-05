"""lm_test style tests for the Specify cache."""
import requests

import lmtest.base.test_base as test_base


# .....................................................................................
class SpCacheCollectionTest(test_base.LmTest):
    """Collection tests (create, get, delete)."""

    # .............................
    def __init__(self, collection_values_json, collection_endpoint, do_verify=True):
        """Constructor for collection test.

        Args:
            collection_values_json (dict): A dictionary of collection values.
            collection_endpoint (str): The API endpoint for collections.
        """
        test_base.LmTest.__init__(self)
        self.collection_endpoint = collection_endpoint
        self.collection_config = collection_values_json
        self.test_name = 'Specify Cache Collection Test for {}'.format(
            self.collection_config['collection_id']
        )
        self._do_verify = do_verify

    # .............................
    def __repr__(self):
        """Return a string representation of this test.

        Returns:
            str: A string representation of the instance.
        """
        return self.test_name

    # .............................
    def run_test(self):
        """Run the test.

        Raises:
            LmTestFailure: Raised if a collection exists when it shouldn't.
        """
        collection_id = self.collection_config['collection_id']
        # Make sure that the collection doesn't exist
        req = requests.get(
            '{}/{}'.format(self.collection_endpoint, collection_id),
            verify=self._do_verify)
        if req.status_code != 404:
            raise test_base.LmTestFailure(
                'Found collection: {} when it should not exist (before_post)'.format(
                    collection_id
                )
            )

        # POST the collection
        requests.post(
            self.collection_endpoint, json=self.collection_config,
            verify=self._do_verify)

        # GET the collection
        collection_get_response = requests.get(
            '{}/{}'.format(self.collection_endpoint, collection_id),
            verify=self._do_verify
        )
        if collection_get_response.status_code != 200:
            raise test_base.LmTestFailure(
                'Collection {} not found when expected.'.format(collection_id)
            )

        # DELETE the collection
        requests.delete(
            '{}/{}'.format(self.collection_endpoint, collection_id),
            verify=self._do_verify)

        # Make sure that the collection is gone again
        req = requests.get(
            '{}/{}'.format(self.collection_endpoint, collection_id),
            verify=self._do_verify)
        if req.status_code != 404:
            raise test_base.LmTestFailure(
                'Found collection: {} when it should not exist (after delete)'.format(
                    collection_id
                )
            )


# .....................................................................................
class SpCacheCollectionOccurrencePostTest(test_base.LmTest):
    """Test posting data for a collection."""

    # .............................
    def __init__(self, dwca_filename, occurrence_endpoint, do_verify=True):
        """Constructor for occurrences post test.

        Args:
            dwca_filename (str): File location of DarwinCore Archive to POST.
            occurrence_endpoint (str): URL for posting occurrences.
        """
        test_base.LmTest.__init__(self)
        self.dwca_filename = dwca_filename
        self.occurrence_endpoint = occurrence_endpoint
        self.test_name = 'Specify Cache Collection Occurrence Test for {}'.format(
            self.occurrence_endpoint
        )
        self._do_verify = do_verify

    # .............................
    def __repr__(self):
        """Return a string representation of this test.

        Returns:
            str: A string representation of the instance.
        """
        return self.test_name

    # .............................
    def run_test(self):
        """Run the test.

        Raises:
            LmTestFailure: Raised if the data cannot be posted successfully.
        """
        with open(self.dwca_filename, mode='rb') as dwca_file:
            data = dwca_file.read()
        resp = requests.post(self.occurrence_endpoint, data=data, verify=self._do_verify)
        if resp.status_code != 204:
            raise test_base.LmTestFailure(
                'Occurrence data post at {} responded with code {}'.format(
                    self.occurrence_endpoint, resp.status_code
                )
            )
