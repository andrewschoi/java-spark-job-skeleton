package org.dti;

import lombok.Data;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Data
public class LocalValueAtRiskService implements ValueAtRiskService{
    private final SparkSession spark;
    private final ValueAtRiskDAO valueAtRiskDAO;

    public LocalValueAtRiskService(SparkSession spark, ValueAtRiskDAO valueAtRiskDAO) throws AnalysisException {
        this.spark = spark;
        this.valueAtRiskDAO = valueAtRiskDAO;
        this.valueAtRiskDAO.securities().createGlobalTempView("securities");
        this.valueAtRiskDAO.positions().createGlobalTempView("positions");
        this.valueAtRiskDAO.pnls().createGlobalTempView("pnls");
    }

    @Override
    public Dataset<Row> query(String sqlText) {
        return spark.sql(sqlText);
    }
}
