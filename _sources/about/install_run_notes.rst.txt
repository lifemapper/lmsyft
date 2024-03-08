Install/Run Notes
#########################

Contains
============

* Specify Network API services

    * Tools/classes for broker, including

        * Flask application for individual API endpoints and frontend
        * classes for Provider API connectors
        * standardized API service output (s2n)

    * Tools/classes for analyst, including

        * AWS scripts and
        * Classes for use on EC2 or other AWS resources
            * geotools for geospatial intersection/annotations
            * aggregation, summary tools for writing tabular summaries
            *



Dependencies:
==============

Schema openapi3==1.1.0


TODO:
============

Add swagger doc generation for APIs