# MC855_spark_experiments
Some Spark Experiments

Created only with educational purposes

K-d Tree implementation based on http://www.sanfoundry.com/java-program-find-nearest-neighbour-using-k-d-tree-search/
IP Database: http://dev.maxmind.com/geoip/geoip2/geolite2/
Cities: https://github.com/bahar/WorldCityLocations

## Building
To build jar package run

```bash
$GRADLE_HOME/bin/gradle build
```

This will generate `spark_experiments.jar` in `build/libs/`.

## Running with Apache Spark - single machine mode
To run the aplication, using the provided IPs and city location data execute:

```bash
$SPARK_HOME/bin/spark-submit --class $CALCULATION_APPROACH --master local[4] build/lib/spark_experiments.jar
```

where `$CALCULATION_APPROACH` should be one of the following strings: "trabalho2.BruteForceIPLocalization" or "IPLocalizationOptimized". This command should run under the project root directory, so resource can be reached. Another possible approach is to use command line arguments (check code for details). 

## Running with Apache Spark - cluster machine mode
TODO
