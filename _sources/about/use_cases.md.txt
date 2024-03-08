# Use Cases

## Data Cleaning
* Users upload occurrence data for cleaning using a number of filters / modifiers / wranglers to produce a dataset suitable for modeling

## Specify Export Storage
* Specify collections upload some or all of their collection holdings as Darwin Core Archives
* Specify collections delete records from their holdings (Specify may have trouble doing this)
* Specify collections update their metadata
* Aggregators consume Specify archives?
* We provide aggregator services on Specify data?
  * Query
  * Download
  * Standardization
* General public can get an individual record (GUID may resolve to our URL)

## Resolver Interaction
 * If GUID is not resolvable yet, add entry pointing at our cache
 * If GUID is present, add to ID mapper?
 * If they have direct access to data for GUID, can we add / update resolver with that info?  Where does it come from?  Collection metadata?  Does it update when collection metadata is updated?

## Syfting

### Syft-ed Data Retrieval
* Users request information about their collection
  * Aggregated statistics
  * Graphics about collection holdings
  * Collection strengths and weaknesses
  * Requests may filter on a variety of parameters like geography, taxonomy, phylogenetics, temporal, etc
  * Maps and rankings

* Users request information about a species
  * Statistics and images about species distribution in a variety of data spaces
  * Individual specimen information with respect to species distribution
  * Images and statistics for species and specimen
  * Requests may filter on a variety of parameters like geography, temporal, etc
  * Maps and rankings

* Users request information about occurrences with filters
  * Spatial, temporal, taxonomic, phylogenetic, climate, etc
  * The goal is to let them explore the data to find unique values

### Syft new data

* Users upload collection data to update their collection holdings and get new syft statistics
* Users check where a point would fall among similar occurrences (uniqueness / correctness / representation / etc)
