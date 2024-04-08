package org.dti;

import org.apache.spark.SparkConf;

public class TestSparkConfig implements SparkConfig {
    @Override
    public SparkConf sparkConf() {
        return new SparkConf().setMaster("local[*]").setAppName("test");
    }

    @Override
    public String dataPath() {
        return "src/main/java/org/dti/data/";
    }

    @Override
    public String securitiesPath() {
        return "src/main/java/org/dti/data/securities.csv";
    }

    @Override
    public String positionsPath() {
        return "src/main/java/org/dti/data/positions.csv";
    }

    @Override
    public String pnlsPath() {
        return "src/main/java/org/dti/data/pnl.csv";
    }
}
