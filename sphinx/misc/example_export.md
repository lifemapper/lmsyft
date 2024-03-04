# Example Specify 6 Export

## Overview

A Specify 6 export is packaged as a Darwin Core Archive consisting of a meta.xml file
and a file of occurrence data.  An example of what those files contain can be found
below:

## meta.xml

The meta.xml file is an XML document containing metadata about the Darwin Core
Archive.  This document is required for all Darwin Core Archive packages.

## occurrences.csv

The name of the occurrence data file is specified in the `meta.xml` document.  The
content of the file should follow what is described in the `meta.xml` file.

```csv
id,accessRights,basisOfRecord,catalogNumber,collectionCode,continent,country,county,datasetName,decimallatitude,decimallongitude,eventDate,geodeticDatum,institutionCode,institutionID,kingdom,locality,occurrenceID,preparations,rights,stateprovince,class,family,genus,order,phylum,scientificName,scientificNameAuthorship,specificepithet,modified,globaluniqueidentifier,preparations,recordedBy,license,datasetname,fieldnumber,highergeography,waterbody
2facc7a2-dd88-44af-b95a-733cc27527d4,http://biodiversity.ku.edu/research/university-kansas-biodiversity-institute-data-publication-and-use-norms,MaterialSample,5035,KUIT,Indian Ocean,South Africa,Scottburgh,University of Kansas Biodiversity Institute Fish Tissue Collection,,,01/26/2002,NAD27,KU,http://grbio.org/cool/iakn-125z,Animalia,Aliwal Shoal - Tiger ledge,2facc7a2-dd88-44af-b95a-733cc27527d4,Tissue - 100,http://creativecommons.org/licenses/by/4.0/deed.en_US,,Actinopterygii,Holocentridae,Myripristis,Beryciformes,Chordata,Myripristis berndti,,,2002-05-24 09:35:15.0,,Tissue - 100,"Bentley, Andrew C; Preston, Mike; Addison, Mark",,,,,
bc01c962-53b2-4346-ba6a-7eeb03795896,http://biodiversity.ku.edu/research/university-kansas-biodiversity-institute-data-publication-and-use-norms,MaterialSample,5030,KUIT,Indian Ocean,South Africa,Scottburgh,University of Kansas Biodiversity Institute Fish Tissue Collection,,,01/26/2002,NAD27,KU,http://grbio.org/cool/iakn-125z,Animalia,Aliwal Shoal - Tiger ledge,bc01c962-53b2-4346-ba6a-7eeb03795896,Tissue - 100,http://creativecommons.org/licenses/by/4.0/deed.en_US,,Actinopterygii,Pomacanthidae,Pomacanthus,Perciformes,Chordata,Pomacanthus rhomboides,,,2002-05-24 09:35:00.0,,Tissue - 100,"Bentley, Andrew C; Preston, Mike; Addison, Mark",,,,,
...
```
