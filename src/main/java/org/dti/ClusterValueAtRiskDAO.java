package org.dti;

import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Data
public class ClusterValueAtRiskDAO implements ValueAtRiskDAO{
    private final SparkSession spark;
    private final SparkConfig sparkConfig;

    @Override
    public Dataset<Row> securities() {
        return spark.read().option("header", "true").schema(Schemas.SECURITIES_SCHEMA).csv(sparkConfig.securitiesPath());
    }

    @Override
    public Dataset<Row> positions() {
        return spark.read().option("header", "true").schema(Schemas.POSITIONS_SCHEMA).csv(sparkConfig.positionsPath());
    }

    @Override
    public Dataset<Row> pnls() {
        return spark.read().option("header", "true").schema(Schemas.PNLS_SCHEMA).csv(sparkConfig.pnlsPath());
    }
}
