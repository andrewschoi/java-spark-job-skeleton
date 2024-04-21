package org.dti;

import lombok.Data;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Data
public class ClusterValueAtRiskService implements ValueAtRiskService{
    private final SparkSession spark;
    private final ValueAtRiskDAO valueAtRiskDAO;

    public ClusterValueAtRiskService(SparkSession spark,ValueAtRiskDAO valueAtRiskDAO) throws AnalysisException {
        this.spark = spark;
        this.valueAtRiskDAO = valueAtRiskDAO;
        this.valueAtRiskDAO.securities().createOrReplaceGlobalTempView("securities");
        this.valueAtRiskDAO.positions().createOrReplaceGlobalTempView("positions");
        this.valueAtRiskDAO.pnls().createOrReplaceGlobalTempView("pnls");
    }
    @Override
    public Dataset<Row> query(String sqlQuery) {
        return spark.sql(sqlQuery);
    }
}
