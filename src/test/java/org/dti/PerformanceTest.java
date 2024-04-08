package org.dti;

import lombok.extern.java.Log;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;

@Log
public class PerformanceTest {
    private static final SparkConfig SPARK_CONFIG = new TestSparkConfig();
    private static SparkSession spark;
    private static final double SAMPLING_SIZE = 1.0 / 10000;
    private static Dataset<Row> securitiesRowSample, positionsRowSample, pnlsRowSample;

    private static Dataset<Row> sampleSecuritiesRow(SparkSession spark) {
        Dataset<Row> securities = spark.read().option("header", "true").schema(Schemas.SECURITIES_SCHEMA).csv(PerformanceTest.SPARK_CONFIG.securitiesPath());
        return securities.sample(false, SAMPLING_SIZE);
    }
    private static Dataset<Row> samplePositionsRow(SparkSession spark) {
        Dataset<Row> positions = spark.read().option("header", "true").schema(Schemas.POSITIONS_SCHEMA).csv(PerformanceTest.SPARK_CONFIG.positionsPath());
        return positions.sample(false, SAMPLING_SIZE);
    }

    private static Dataset<Row> samplePnlsRow(SparkSession spark) {
        Dataset<Row> pnls = spark.read().option("header", "true").schema(Schemas.PNLS_SCHEMA).csv(PerformanceTest.SPARK_CONFIG.pnlsPath());
        return pnls.sample(false, SAMPLING_SIZE);
    }

    @BeforeAll
    static void setUp() throws AnalysisException {
        log.warning("randomly sampling from securities.csv, positions.csv, and pnl.csv");
        spark = SparkSession.builder().config(SPARK_CONFIG.sparkConf()).getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        securitiesRowSample = sampleSecuritiesRow(spark);
        positionsRowSample = samplePositionsRow(spark);
        pnlsRowSample = samplePnlsRow(spark);
        assert !securitiesRowSample.isEmpty();
        assert !positionsRowSample.isEmpty();
        assert !pnlsRowSample.isEmpty();
        securitiesRowSample.createOrReplaceGlobalTempView("securities");
        positionsRowSample.createOrReplaceGlobalTempView("positions");
        pnlsRowSample.createOrReplaceGlobalTempView("pnls");
    }

    @AfterAll
    static void shutdown() {
        assert spark != null;
        spark.stop();
    }


    @Test
    void testValueAtRiskQueryProducesValidQueries() throws AnalysisException {
        for(int i = 0; i < 50; i++) {
            String query = ValueAtRiskQuery.makeRandomQuery(securitiesRowSample.collectAsList(), positionsRowSample.collectAsList());
            log.warning("testing if query is valid -  " + query);
            spark.sessionState().sqlParser().parsePlan(query);
            spark.sql(query).queryExecution().analyzed();
        }
    }

    @Test
    @Timeout(100)
    void testQueryRunsUnderTimeLimit() {
        String query = ValueAtRiskQuery.makeRandomQuery(securitiesRowSample.collectAsList(), positionsRowSample.collectAsList());
        log.warning("running query - " + query);
        spark.sql(query);
    }
    @Test
    void testValueAtRiskServicePerformance() {
        log.warning("securities table cardinality - " + String.valueOf(securitiesRowSample.collectAsList().size()));
        log.warning("positions table cardinality - " + String.valueOf(positionsRowSample.collectAsList().size()));
        log.warning("pnls table cardinality - " + String.valueOf(pnlsRowSample.collectAsList().size()));

        for(int i = 0; i < 10; i++) {
            testQueryRunsUnderTimeLimit();
        }
    }
}
