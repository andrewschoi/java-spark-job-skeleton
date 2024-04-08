package org.dti;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface ValueAtRiskDAO {
    Dataset<Row> securities();
    Dataset<Row> positions();
    Dataset<Row> pnls();
}
