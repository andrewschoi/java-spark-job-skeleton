package org.dti;
import lombok.extern.java.Log;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Log
public class Main {
    private static final boolean useLocalImpl = false;
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
        result.explain();
        result.show();

        spark.stop();
    }


}