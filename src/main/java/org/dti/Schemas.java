package org.dti;
import lombok.Data;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Schemas {
    private final static StructField[] securitiesStructFields = {
            DataTypes.createStructField("securityid", DataTypes.StringType, false),
            DataTypes.createStructField("assetclass", DataTypes.StringType, false),
            DataTypes.createStructField("securitytype", DataTypes.StringType, false),
            DataTypes.createStructField("tradingcountry", DataTypes.StringType, false),
            DataTypes.createStructField("tradingcurrency", DataTypes.StringType, false),
            DataTypes.createStructField("issuername", DataTypes.StringType, false),
            DataTypes.createStructField("ticker", DataTypes.StringType, false),
            DataTypes.createStructField("rating", DataTypes.StringType, false),
            DataTypes.createStructField("industrygroup", DataTypes.StringType, false),
            DataTypes.createStructField("industry", DataTypes.StringType, false),
            DataTypes.createStructField("securityname", DataTypes.StringType, false),
            DataTypes.createStructField("maturitydate", DataTypes.StringType, false),
            DataTypes.createStructField("underlyingname", DataTypes.StringType, false),
            DataTypes.createStructField("regionname", DataTypes.StringType, false),
            DataTypes.createStructField("issuercountryofrisk", DataTypes.StringType, false)
    };
    public final static StructType SECURITIES_SCHEMA = new StructType(securitiesStructFields);

    private final static StructField[] positionsStructFields = {
            DataTypes.createStructField("securityid", DataTypes.StringType, false),
            DataTypes.createStructField("trader", DataTypes.StringType, false),
            DataTypes.createStructField("microstrategy", DataTypes.StringType, false),
            DataTypes.createStructField("pod", DataTypes.StringType, false),
            DataTypes.createStructField("desk", DataTypes.StringType, false),
            DataTypes.createStructField("supervisor", DataTypes.StringType, false),
            DataTypes.createStructField("quantity", DataTypes.StringType, false)
    };

    public final static StructType POSITIONS_SCHEMA = new StructType(positionsStructFields);

    private final static StructField[] pnlsStructfields = {
            DataTypes.createStructField("securityid", DataTypes.StringType, false),
            DataTypes.createStructField("date", DataTypes.DateType, false),
            DataTypes.createStructField("pnl", DataTypes.FloatType, false)
    };

    public final static StructType PNLS_SCHEMA = new StructType(pnlsStructfields);


    private final static class Securities {
        List<String> fields = new ArrayList<>();

        Securities(String... fields) {
            this.fields.addAll(Arrays.asList(fields));
        }
    }


    private final static class Positions {

    }

    private final static class Pnls {

    }
}
