package org.dti;

import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Data
public class LocalValueAtRiskDAO implements ValueAtRiskDAO {
    private final SparkSession spark;
    private final SparkConfig sparkConfig;


    @Override
    public Dataset<Row> securities() {
        Dataset<Row> securities;
        if (Files.exists(Paths.get(sparkConfig.dataPath(), "securities.parquet"))) {
            securities = spark.read().parquet(Paths.get(sparkConfig.dataPath(), "securities.parquet").toString());
        } else {
            securities = spark.read().option("header", "true").schema(Schemas.SECURITIES_SCHEMA).csv(sparkConfig.securitiesPath());
            securities.write().parquet(sparkConfig.dataPath() + "securities.parquet");
        }
        return securities;
    }

    @Override
    public Dataset<Row> positions() {
        Dataset<Row> positions;
        if (Files.exists(Paths.get(sparkConfig.dataPath(), "positions.parquet"))) {
            positions = spark.read().parquet(Paths.get(sparkConfig.dataPath(), "positions.parquet").toString());
        } else {
            positions = spark.read().option("header", "true").schema(Schemas.POSITIONS_SCHEMA).csv(sparkConfig.positionsPath());
            positions.write().parquet(sparkConfig.dataPath() + "positions.parquet");
        }
        return positions;
    }

    @Override
    public Dataset<Row> pnls() {
        Dataset<Row> pnls;
        if (Files.exists(Paths.get(sparkConfig.dataPath(), "pnls.parquet"))) {
            pnls = spark.read().parquet(Paths.get(sparkConfig.dataPath(), "pnls.parquet").toString());
        } else {
            pnls = spark.read().option("header", "true").schema(Schemas.PNLS_SCHEMA).csv(sparkConfig.pnlsPath());
            pnls.write().parquet(sparkConfig.dataPath() + "pnls.parquet");
        }
        return pnls;
    }

}
