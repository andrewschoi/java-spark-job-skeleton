package org.dti;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface ValueAtRiskService {
    Dataset<Row> query(String sqlQuery);
}
