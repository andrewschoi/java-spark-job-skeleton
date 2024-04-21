package org.dti;

import org.apache.spark.SparkConf;

public class ClusterSparkConfig implements SparkConfig{
    public static final String EXECUTOR_MEMORY = "4g";
    public static final String EXECUTOR_INSTANCES = "2";
    public static final String EXECUTOR_CORES = "4";
    public static final String DRIVER_MEMORY = "16g";
    @Override
    public SparkConf sparkConf() {
        return new SparkConf().setAppName("cluster").set("spark.executor.memory", EXECUTOR_MEMORY).set("spark.executor.instances", EXECUTOR_INSTANCES).set("spark.executor.cores", EXECUTOR_CORES).set("spark.driver.memory", DRIVER_MEMORY);
    }

    @Override
    public String dataPath() {
        return "s3a://source-ca44c1c8baf68341/";
    }

    @Override
    public String securitiesPath() {
        return dataPath() + "securities.csv";
    }

    @Override
    public String positionsPath() {
        return dataPath() + "positions.csv";
    }

    @Override
    public String pnlsPath() {
        return dataPath() + "pnl.csv";
    }
}
