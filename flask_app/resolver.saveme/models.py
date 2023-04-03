"""Models for Resolver."""
from flask_app.resolver.config import ARK_PATTERN


# .....................................................................................
class Ark:
    """Class containing Ark information."""
    # ............................
    def __init__(self, attribute_dict):
        """Construct an Ark object.

        Args:
            attribute_dict (dict): A dictionary of attribute keys and value values.
        """
        self.attributes = attribute_dict
        self.attributes['where'] = ARK_PATTERN.replace('<guid>', self.attributes['id'])

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
