package org.dti;
import lombok.extern.java.Log;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Log
public class Main {
    private static final boolean useLocalImpl = true;
    public static void main(String[] args) throws AnalysisException {
        SparkConfig sparkConfig;
        SparkSession spark;
        ValueAtRiskDAO valueAtRiskDAO;
        ValueAtRiskService valueAtRiskService;
        if (useLocalImpl) {
            sparkConfig = new LocalSparkConfig();
            spark = SparkSession.builder().config(sparkConfig.sparkConf()).getOrCreate();
            valueAtRiskDAO = new LocalValueAtRiskDAO(spark, sparkConfig);
            valueAtRiskService = new LocalValueAtRiskService(spark, valueAtRiskDAO);
        } else {
            sparkConfig = new ClusterSparkConfig();
            spark =SparkSession.builder().config(sparkConfig.sparkConf()).getOrCreate();
            valueAtRiskDAO =new ClusterValueAtRiskDAO(spark, sparkConfig);
            valueAtRiskService = new ClusterValueAtRiskService(spark, valueAtRiskDAO);
        }

        long systemTimeStart = System.currentTimeMillis();

        for(int i = 0; i < 1; i++) {
            Dataset<Row> result = valueAtRiskService.query("select\n" +
                    "p.desk, p.pod, s.assetclass, percentile_approx(CAST(x.pnl AS DOUBLE), 0.05, 10000)\n\n" +
                    "from\n" +
                    "global_temp.positions p, global_temp.securities s, global_temp.pnls x\n" +
                    "where\n" +
                    "p.securityid = s.securityid and x.securityid = s.securityid\n" +
                    "and p.supervisor = 'Fundamental EQ'\n" +
                    "and s.tradingcountry = 'US'\n" +
                    "group by\n" +
                    "p.desk, p.pod, s.assetclass\n");
        }
        long systemTimeEnd = System.currentTimeMillis();
        log.warning("==========================================");
        log.warning("EXECUTION TIME FOR 10 RANDOM QUERIES");
        log.warning("==========================================\\n");
        log.warning(String.valueOf((systemTimeEnd - systemTimeStart) / 1000) + " seconds");
        spark.stop();
    }


}