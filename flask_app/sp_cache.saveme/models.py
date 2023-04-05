"""Models for Specify Cache."""


# .....................................................................................
class Collection:
    """Class containing collection information."""
    # ............................
    def __init__(self, attribute_dict):
        """Construct a Collection object.

        Args:
            attribute_dict (dict): A dictionary of attribute keys and value values.
        """
        self.attributes = attribute_dict

    # ............................
    def serialize_json(self):
        """Serialize the object for JSON responses.

        Returns:
            dict: A JSON-serializable dictionary.
        """
        return self.attributes

    # ............................
    def validate(self):
        """Validate the collection."""
        pass


# .....................................................................................
class SpecimenRecord:
    """Class containing specimen record information."""
    # ............................
    def __init__(self, attribute_dict):
        """Construct a SpecimenRecord.

        Args:
            attribute_dict (dict): A dictionary of attributes.  The top level key
                should be the namespace.  Under that should be attributes in that
                namespace with their values.
        """
        self.attributes = attribute_dict

    # ............................
    def serialize_json(self):
        """Serialize the object for JSON responses.

        Returns:
            dict: A JSON-serializable dictionary.
        """
        return self.attributes
