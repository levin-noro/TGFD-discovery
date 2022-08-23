# TGFD Discovery

<!-- TOC -->
* [TGFD Discovery](#tgfd-discovery)
  * [1. Overview](#1-overview)
  * [2. Datasets](#2-datasets)
    * [2.1. DBpedia](#21-dbpedia)
      * [Source](#source)
      * [Dataset preparation](#dataset-preparation)
    * [2.2. IMDB](#22-imdb)
      * [Sources:](#sources-)
      * [Dataset preparation](#dataset-preparation)
    * [2.3. Synthetic](#23-synthetic)
      * [Dataset preparation](#dataset-preparation)
  * [3. Getting Started](#3-getting-started)
    * [3.1. Prerequisites](#31-prerequisites)
    * [3.2. Instructions](#32-instructions)
      * [3.2.1. Arguments for `<optional_java_args>`](#321-arguments-for-optional_java_args)
      * [3.2.2. Required arguments for `<tgfd_discovery_args>`](#322-required-arguments-for-tgfd_discovery_args)
      * [3.2.3. Optional arguments for `<tgfd_discovery_args>`](#323-optional-arguments-for-tgfd_discovery_args)
      * [3.2.4. (For developers) Optional arguments for `<tgfd_discovery_args>`](#324--for-developers--optional-arguments-for-tgfd_discovery_args)
      * [3.2.5. How to generate change-files](#325-how-to-generate-change-files)
<!-- TOC -->

## 1. Overview

Temporal Graph Functional Dependencies (TGFDs) are a recently defined class of data quality rules for enforcing consistency over evolving graphs. This project is an automated solution for discovering TGFDs in large-scale graphs.

## 2. Datasets

Our system has been tested on the following large-scale graph datasets. We provide a sources for obtaining each dataset. We also provide instructions on how to prepare the datasets for use with our system.  

### 2.1. DBpedia

#### Source

- https://databus.dbpedia.org/dbpedia/collections/latest-core

#### Dataset preparation
Download as many snapshots from the source as you need and place the snapshots in a new folder on your local machine. Each snapshot file must be placed inside its own subfolder, and the name of the subfolder must be a timestamp of the format `YYYYMMDD`. Each snapshot file must be in `.ttl` format and use the `.ttl` file extension.

### 2.2. IMDB

#### Sources
- https://www.imdb.com/interfaces/
- ftp://ftp.fu-berlin.de/pub/misc/movies/database/frozendata/

#### Dataset preparation
Download as many snapshots from the source as you need from a source and place the snapshots in a new folder on your local machine. Each snapshot file must use the naming format `imdb-YYYYMMDD.nt`. Each snapshot file must be in `.nt` format and use the `.nt` file extension.

### 2.3. Synthetic

Synthetic datasets can be generated using [gMark](https://github.com/gbagan/gmark).

#### Dataset preparation
Once you have successfully generated snapshots using gMark, place the snapshots in a new folder on your local machine. Each snapshot file must use the naming format `graphYYYYMMDD.txt`.

## 3. Getting Started

### 3.1. Prerequisites
- Java 15 
- Maven

### 3.2. Instructions

1. Download/clone this repository.
2. Using a Java IDE such as IntelliJ, open the VF2SubIso folder as a new project.
3. Build a jar that uses `src/main/java/TgfdDiscovery/TgfdDiscovery.java` as its main class. For more information on building jars using IntelliJ, read the [IntelliJ documentation](https://www.jetbrains.com/help/idea/compiling-applications.html#package_into_jar).
4. Execute the jar using the command: `java <optional_java_args> -cp path/to/jar TgfdDiscovery.TgfdDiscovery <tgfd_discovery_args>`

#### 3.2.1. Arguments for `<optional_java_args>`

| Argument         | Description                                                                                                                                                                               |
|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `-Xmx<integer>g` | If you encounter an `OutOfMemory` error, increase the amount of memory available to java by specifying this argument. For example, `-Xmx128g` allocates 128 gigabytes of memory for java. |

#### 3.2.2. Required arguments for `<tgfd_discovery_args>`

| Argument                                                | Description                                                                         |
|---------------------------------------------------------|-------------------------------------------------------------------------------------|
| <code>-loader [dbpedia&#124;imdb&#124;synthetic]</code> | Specify one of three loaders (dbpedia, imdb, synthetic) for parsing the dataset.    |
| `-path path/to/jar`                                     | The path to the folder containing the dataset files.                                |
| `-t <integer>`                                          | Number of snapshots to use.                                                         |
| `-k <integer>`                                          | Discover graph patterns with up to k edges.                                         |
| `-theta <percent>`                                      | Specify a support threshold between 0.0 and 1.0.                                    |
| `-a <integer>`                                          | Number of frequent attributes that will be considered during dependency generation. |
| `-f <integer>`                                          | Number of frequent edges that will be considered during pattern generation.         |

#### 3.2.3. Optional arguments for `<tgfd_discovery_args>`

| Argument                                   | Description                                                                                                                                                                                                          |
|--------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `-interestLabels <comma_seperated_values>` | Specify an additional list of labels to include in the sets of frequent attributes and edges.                                                                                                                        |
| `-maxLit <integer>`                        | Discover dependencies with up to n literals during dependency generation, where n is the specified integer.                                                                                                          |
| <code>-changefile [all&#124;opt] </code>   | Build graphs using change-files instead of snapshots. Specify `all` to consider all changes in a change-file. Specify `opt` to only consider relevant changes in a change-file. Must be used with `-changefilePath`. |
| `-incremental`                             | Use incremental matching to avoid recomputing matches between snapshots. Must be used with `-changefilePath`. Works best when the number of changes between snapshots is small.                                      |
| `-changefilePath /path/to/changefiles`     | Path to a folder that contains all change-files. Refer to Section [3.2.5](#325-how-to-generate-change-files) for instructions on how to generate change-files for a dataset.                                         |
| `-skipK1`                                  | Skips discovery of TGFDs for graph patterns of size k = 1.                                                                                                                                                           |
| `-dontStore`                               | Does not store any snapshots or change-files in memory. Snapshots and change-files will be read from memory as needed. This option reduces memory usage at the expense of increased runtime.                         |
| `-simplifySuperVertex <integer>`           | Dissolves all vertices in each snapshot that have an in-degree greater than the specified integer.                                                                                                                   |
| `-k0`                                      | Discover TGFDs for graph patterns of size k = 0.                                                                                                                                                                     |

#### 3.2.4. (For developers) Optional arguments for `<tgfd_discovery_args>`

| Argument               | Description                                                                                                            |
|------------------------|------------------------------------------------------------------------------------------------------------------------|
| `-noMinimalityPruning` | Disable the pruning of redundant dependencies during dependency generation.                                            |
| `-noSupportPruning`    | Disable the pruning of low-support graph patterns during pattern generation.                                           |
| `-uninteresting`       | Disable restriction that forces every vertex in a pattern to participate in a dependency. Must be used with `-maxLit`. |
| `-K`                   | Print to file the runtime of each level i in the TGFD discovery process, where 0 <= i <= k.                            |
| `-validation`          | Run experiment without localized subgraph isomorphism. This is very slow. Only use for validation testing.             |
| `-slow`                | Disable pattern matching optimizations for localized subgraph isomorphism.                                             |

#### 3.2.5. How to generate change-files
1. Build a jar that uses `src/test/java/testDiffExtractor.java` as its main class.
2. Execute the jar using the command `java <optional_java_args> -cp path/to/jar testDiffExtractor <required_args>`, where `<optional_java_args>` are defined in Section [3.2.1](#321-arguments-for-optional_java_args) and `<required_args>` consists of the following three arguments:
   - `-path path/to/dataset`
   - `-loader [dbpedia|imdb|synthetic]`. Choose one of three values: dbpedia, imdb, synthetic.
   - `-percent <percent>`. Specify a value between 0.0 and 1.0.