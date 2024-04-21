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

    private static ValueAtRiskDAO clusterValueAtRiskDAO;
    private static ValueAtRiskService clusterValueAtRiskService;

    private static SparkSession spark;
    private static final double SAMPLING_SIZE_SMALL = 1.0 / 10000;
    private static final double SAMPLING_SIZE_MEDIUM = 1.0 / 1000;
    private static final double SAMPLING_SIZE_LARGE = 1.0 / 100;
    private static Dataset<Row> securitiesRowSample, positionsRowSample, pnlsRowSample;

    private static Dataset<Row> sampleSecuritiesRow(SparkSession spark, double samplingSize) {
        Dataset<Row> securities = spark.read().option("header", "true").schema(Schemas.SECURITIES_SCHEMA).csv(PerformanceTest.SPARK_CONFIG.securitiesPath());
        return securities.sample(false, samplingSize);
    }
    private static Dataset<Row> samplePositionsRow(SparkSession spark, double samplingSize) {
        Dataset<Row> positions = spark.read().option("header", "true").schema(Schemas.POSITIONS_SCHEMA).csv(PerformanceTest.SPARK_CONFIG.positionsPath());
        return positions.sample(false, samplingSize);
    }

    private static Dataset<Row> samplePnlsRow(SparkSession spark, double samplingSize) {
        Dataset<Row> pnls = spark.read().option("header", "true").schema(Schemas.PNLS_SCHEMA).csv(PerformanceTest.SPARK_CONFIG.pnlsPath());
        return pnls.sample(false, samplingSize);
    }

    @BeforeAll
    static void setUp() throws AnalysisException {
        spark = SparkSession.builder().config(SPARK_CONFIG.sparkConf()).getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        log.warning("Randomly sampling from securities, positions, and pnl files\n");
        securitiesRowSample = sampleSecuritiesRow(spark, SAMPLING_SIZE_SMALL);
        positionsRowSample = samplePositionsRow(spark, SAMPLING_SIZE_SMALL);
        pnlsRowSample = samplePnlsRow(spark, SAMPLING_SIZE_SMALL);

        assert !securitiesRowSample.isEmpty();
        assert !positionsRowSample.isEmpty();
        assert !pnlsRowSample.isEmpty();

        securitiesRowSample.createOrReplaceGlobalTempView("securities");
        positionsRowSample.createOrReplaceGlobalTempView("positions");
        pnlsRowSample.createOrReplaceGlobalTempView("pnls");

        clusterValueAtRiskDAO = new ClusterValueAtRiskDAO(spark, SPARK_CONFIG);
        clusterValueAtRiskService = new ClusterValueAtRiskService(spark, clusterValueAtRiskDAO);
    }

    @AfterAll
    static void shutdown() {
        assert spark != null;
        spark.stop();
    }


    @Test
    void testValueAtRiskQueryProducesValidQueries() throws AnalysisException {
        log.warning("==========================================");
        log.warning("TESTING QUERIES ARE VALID");
        log.warning("==========================================\n\n");

        for(int i = 0; i < 10; i++) {
            String query = ValueAtRiskQuery.makeRandomQuery(securitiesRowSample.collectAsList(), positionsRowSample.collectAsList());
            log.warning("CHECKING -  " + query);
            spark.sessionState().sqlParser().parsePlan(query);
            spark.sql(query).queryExecution().analyzed();
        }
    }

    @Test
    @Timeout(100)
    void testQueryRunsUnderTimeLimit() {
        String query = ValueAtRiskQuery.makeRandomQuery(securitiesRowSample.collectAsList(), positionsRowSample.collectAsList());
        log.warning("RUNNING - " + query);
        clusterValueAtRiskService.query(query);
    }
    @Test
    void testValueAtRiskServicePerformanceSmall() {
        log.warning("==========================================");
        log.warning("TESTING SERVICE ON SMALL TABLE SIZES");
        log.warning("==========================================\n\n");

        log.warning("Randomly sampling from securities, positions, and pnl files\n");
        securitiesRowSample = sampleSecuritiesRow(spark, SAMPLING_SIZE_SMALL);
        positionsRowSample = samplePositionsRow(spark, SAMPLING_SIZE_SMALL);
        pnlsRowSample = samplePnlsRow(spark, SAMPLING_SIZE_SMALL);

        assert !securitiesRowSample.isEmpty();
        assert !positionsRowSample.isEmpty();
        assert !pnlsRowSample.isEmpty();

        securitiesRowSample.createOrReplaceGlobalTempView("securities");
        positionsRowSample.createOrReplaceGlobalTempView("positions");
        pnlsRowSample.createOrReplaceGlobalTempView("pnls");

        log.warning("securities table cardinality - " + String.valueOf(securitiesRowSample.collectAsList().size()));
        log.warning("positions table cardinality - " + String.valueOf(positionsRowSample.collectAsList().size()));
        log.warning("pnls table cardinality - " + String.valueOf(pnlsRowSample.collectAsList().size()));

        for(int i = 0; i < 10; i++) {
            testQueryRunsUnderTimeLimit();
        }
    }

    @Test
    void testValueAtRiskServicePerformanceMedium() {
        log.warning("==========================================");
        log.warning("TESTING SERVICE ON MEDIUM TABLE SIZES");
        log.warning("==========================================\n\n");

        log.warning("Randomly sampling from securities, positions, and pnl files\n");

        securitiesRowSample = sampleSecuritiesRow(spark, SAMPLING_SIZE_MEDIUM);
        positionsRowSample = samplePositionsRow(spark, SAMPLING_SIZE_MEDIUM);
        pnlsRowSample = samplePnlsRow(spark, SAMPLING_SIZE_MEDIUM);

        assert !securitiesRowSample.isEmpty();
        assert !positionsRowSample.isEmpty();
        assert !pnlsRowSample.isEmpty();

        securitiesRowSample.createOrReplaceGlobalTempView("securities");
        positionsRowSample.createOrReplaceGlobalTempView("positions");
        pnlsRowSample.createOrReplaceGlobalTempView("pnls");

        log.warning("securities table cardinality - " + String.valueOf(securitiesRowSample.collectAsList().size()));
        log.warning("positions table cardinality - " + String.valueOf(positionsRowSample.collectAsList().size()));
        log.warning("pnls table cardinality - " + String.valueOf(pnlsRowSample.collectAsList().size()));

        for(int i = 0; i < 10; i++) {
            testQueryRunsUnderTimeLimit();
        }
    }

    @Test
    void testValueAtRiskServicePerformanceLarge() {
        log.warning("==========================================");
        log.warning("TESTING SERVICE ON LARGE TABLE SIZES");
        log.warning("==========================================\n\n");

        log.warning("Randomly sampling from securities, positions, and pnl files\n");

        securitiesRowSample = sampleSecuritiesRow(spark, SAMPLING_SIZE_LARGE);
        positionsRowSample = samplePositionsRow(spark, SAMPLING_SIZE_LARGE);
        pnlsRowSample = samplePnlsRow(spark, SAMPLING_SIZE_LARGE);

        assert !securitiesRowSample.isEmpty();
        assert !positionsRowSample.isEmpty();
        assert !pnlsRowSample.isEmpty();

        securitiesRowSample.createOrReplaceGlobalTempView("securities");
        positionsRowSample.createOrReplaceGlobalTempView("positions");
        pnlsRowSample.createOrReplaceGlobalTempView("pnls");

        log.warning("securities table cardinality - " + String.valueOf(securitiesRowSample.collectAsList().size()));
        log.warning("positions table cardinality - " + String.valueOf(positionsRowSample.collectAsList().size()));
        log.warning("pnls table cardinality - " + String.valueOf(pnlsRowSample.collectAsList().size()));

        for(int i = 0; i < 10; i++) {
            testQueryRunsUnderTimeLimit();
        }
    }
}
