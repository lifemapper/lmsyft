

S-to-the-n services
----------------------

* map: <server>/api/v1/map:: 
  return metadata, url endpoints, and layernames for predicted species distributions and 
  occurrence points

* name: <server>/api/v1/name::
  return metadata for taxonomic information on a string

* occ: <server>/api/v1/occ::
  return metadata for species occurrence points for a GUID, or for a GBIF dataset GUID

* resolve: <server>/api/v1/resolve::
  return unique identifier metadata including a direct URL for a data object.  Currently
  implemented only for Specify GUIDs and endpoints 

Code resources
--------------------

* The core APIs are defined in the directory: src/LmRex/services/api/v1 .
  There are currently 4 files (categories) that organize them: 
  map, name, occ, resolve, and I will add a 5th - heartbeat. 
    
* The classes in these files all inherit from _S2nService in the base.py file, 
  which implements some methods to ensure they all behave consistently and use a 
  subset of the same parameters and defaults.  The _standardize_params method 
  contains defaults for url keyword parameters.

* The root.py file contains the cherrypy commands and configuration to expose 
  the services.

* In the src/LmRex/common/lmconstants.py file are the constants that are used in 
  multiple places. 

  * **TST_VALUES** contains names and guids that can be used for testing
    services, some will return data, some will not, but none should return 
    errors.

  * **APIService** contains the URL service endpoints for the different 
    categories of services. 

  * **ServiceProvider** contains the name, and service categories 
    available for that provider.

  * All service endpoints (following the server url) will start the 
    root (/api/v1), then category.  The "tentacles" service that queries all 
    available providers for that category will be at that endpoint 
    (example: /api/v1/name). 

  * All service endpoints accept a query parameter "provider" for the providers 
    available for that service, listed in ServiceProvider class.  The values may be one or
    more of the following: bison, gbif, idb, itis, lm, mopho, specify.
    
TODO
----------------------
* 
