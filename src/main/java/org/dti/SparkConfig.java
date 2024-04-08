package org.dti;

import org.apache.spark.SparkConf;

public interface SparkConfig {
    SparkConf sparkConf();
    String dataPath();
    String securitiesPath();
    String positionsPath();
    String pnlsPath();
}
