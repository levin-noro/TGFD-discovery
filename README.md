# TGFD Discovery

## Dataset Formats

### DBpedia

#### Source

https://databus.dbpedia.org/dbpedia/collections/latest-core

#### Dataset preparation
Download as many snapshots as you need and place the snapshots in a new folder on your local machine. Each snapshot must be place in its own subfolder, and the name of the subfolder must be a timestamp `YYYYMMDD`. Each snapshot file must be in `.ttl` format.

### IMDB

#### Sources:
- https://www.imdb.com/interfaces/
- ftp://ftp.fu-berlin.de/pub/misc/movies/database/frozendata/

#### Dataset preparation
Download as many snapshots as you need from a source and place the snapshots in a new folder on your local machine. Each snapshot must use the following naming convention `imdb-YYYYMMDD.nt`. Each snapshot file must be in `.nt` format.

### Synthetic

Synthetic dataset must be created using [gMark](https://github.com/gbagan/gmark).

#### Dataset preparation
Once you have successfully generated snapshots using gMark, place the snapshots in a new folder on your local machine. Each snapshot must use the following naming convention `graphYYYYMMDD.txt`.

## Getting Started

### Prerequisites

1. Java 15
2. Maven

### Instructions

1. Download/clone this repository.
2. Using a Java IDE such as IntelliJ, open the VF2SubIso folder as a new project.
3. Build a jar that uses `TgfdDiscovery.java` as its main class. For more information on building jars, read the [IntelliJ documentation](https://www.jetbrains.com/help/idea/compiling-applications.html#package_into_jar).
4. Run the following command `java <optional_java_args> -cp path/to/jar TgfdDiscovery.TgfdDiscovery <required_args>`

### Optional arguments for `java`

1. `-Xmx<integer>g`. If you encounter an `OutOfMemory` error, increase the amount of memory available to java by specifying this argument. For example, `-Xmx128g` allocates 128 gigabytes of memory for java.

### Required arguments for `TgfdDiscovery.TgfdDiscovery`


| Argument                                                | Description                                                                         |
|---------------------------------------------------------|-------------------------------------------------------------------------------------|
| <code>-loader [dbpedia&#124;imdb&#124;synthetic]</code> | Specify a loader for parsing the dataset.                                           |
| `-path path/to/jar`                                     | Specify the path to the folder containing the entire dataset.                       |
| `-t <integer>`                                          | Number of snapshots to use.                                                         |
| `-k <integer>`                                          | Discover patterns with up to k edges.                                               |
| `-theta <percent>`                                      | Use a support threshold between 0.0 and 1.0.                                        |
| `-a <integer>`                                          | Number of frequent attributes that will be considered during dependency generation. |
| `-f <integer>`                                          | Number of frequent edges that will be considered during pattern generation.         |

### Optional arguments for `TgfdDiscovery.TgfdDiscovery`

| Argument                                   | Description                                                                                                                                                                            |
|--------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `-interestLabels <comma_seperated_values>` | Specify an additional list of labels to include in the sets of frequent attributes and edges.                                                                                          |
| `-maxLit <integer>`                        | Discover dependencies with up to n literals.                                                                                                                                           |
| <code>-changefile [all&#124;opt] </code>   | Build graphs using change-files instead of snapshots. Specify `all` to consider all changes. Specify `opt` to only consider relevant changes. Must be used with `-changefilePath`.     |
| `-incremental`                             | Use incremental matching to avoid recomputing matching. Must be used with `-changefilePath` and works best when the number of changes between snapshots is small.                      |
| `-changefilePath /path/to/changefiles`     | Specify path to folder that contains all changefiles.                                                                                                                                  |
| `-skipK1`                                  | Skips discovery of TGFDs for patterns of size k = 1                                                                                                                                    |
| `-dontStore`                               | Does not store any snapshots or changefiles in memory. Snapshots and changefiles will be read from memory as needed. This option reduces memory usage at expense of increased runtime. |
| `-simplifySuperVertex <integer>`           | Dissolves vertices in the that have an in-degree greater than specified integer.                                                                                                       |
| `-k0`                                      | Discover TGFDs for patterns of size k = 0.                                                                                                                                             |

### (For developers) Optional arguments for `TgfdDiscovery.TgfdDiscovery`

| Argument               | Description                                                                                                            |
|------------------------|------------------------------------------------------------------------------------------------------------------------|
| `-noMinimalityPruning` | Disable the pruning of redundant dependencies during dependency generation.                                            |
| `-noSupportPruning`    | Disable the pruning of low-support patterns during pattern generation.                                                 |
| `-uninteresting`       | Disable restriction that forces every vertex in a pattern to participate in a dependency. Must be used with `-maxLit`. |
| `-K`                   | Print runtime to file after each level i, where 0 <= i <= k.                                                           |
| `-validation`          | Run experiment without localized subgraph isomorphism. This is very slow. Only use for validation testing.             |
| `-slow`                | Disable pattern matching optimizations for localized subraph isomorphism.                                              |

