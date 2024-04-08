package org.dti;

import lombok.extern.java.Log;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Log
public class ValueAtRiskQuery {
    private static final int SECURITIES_SCHEMA_LENGTH = Schemas.SECURITIES_SCHEMA.length();
    private static final String[] SECURITIES_FIELD_NAMES = Schemas.SECURITIES_SCHEMA.fieldNames();
    private static final int POSITION_SCHEMA_LENGTH = Schemas.POSITIONS_SCHEMA.length();
    private static final String[] POSITION_FIELD_NAMES = Schemas.POSITIONS_SCHEMA.fieldNames();
    private static final int PNLS_SCHEMA_LENGTH = Schemas.PNLS_SCHEMA.length();
    private static final String[] PNLS_FIELD_NAMES = Schemas.PNLS_SCHEMA.fieldNames();



    public static String makeRandomQuery(List<Row> securitiesRowSample, List<Row> positionsRowSample) {
        assert !securitiesRowSample.isEmpty();
        assert !positionsRowSample.isEmpty();

        List<String> SECURITIES_FIELD_NAMES = Arrays.asList(ValueAtRiskQuery.SECURITIES_FIELD_NAMES);
        List<String> POSITION_FIELD_NAMES = Arrays.asList(ValueAtRiskQuery.POSITION_FIELD_NAMES);

        Collections.shuffle(SECURITIES_FIELD_NAMES);
        Collections.shuffle(POSITION_FIELD_NAMES);

        int securitiesFieldsNumber = ThreadLocalRandom.current().nextInt(1, SECURITIES_SCHEMA_LENGTH);
        int positionsFieldsNumber = ThreadLocalRandom.current().nextInt(1, POSITION_SCHEMA_LENGTH);

        assert securitiesFieldsNumber < SECURITIES_FIELD_NAMES.size();
        assert positionsFieldsNumber < POSITION_FIELD_NAMES.size();

        StringBuilder selectClause = new StringBuilder("select ");
        for (int i = 0; i < Math.max(securitiesFieldsNumber, positionsFieldsNumber); i++) {
            if (securitiesFieldsNumber >= i) {
                selectClause.append("s.");
                selectClause.append(SECURITIES_FIELD_NAMES.get(i));
                selectClause.append(", ");
            }
            if (positionsFieldsNumber >= i) {
                selectClause.append("p.");
                selectClause.append(POSITION_FIELD_NAMES.get(i));
                selectClause.append(", ");
            }

        }
        selectClause.deleteCharAt(selectClause.length() - 2);

        String fromClause = "from global_temp.securities s, global_temp.positions p, global_temp.pnls x ";

        assert securitiesFieldsNumber < SECURITIES_FIELD_NAMES.size();
        assert positionsFieldsNumber < POSITION_FIELD_NAMES.size();

        StringBuilder whereClause = new StringBuilder("where s.securityid = p.securityid and p.securityid = x.securityid");
        if (ThreadLocalRandom.current().nextInt(0, 2) == 0) {
            whereClause.append(" and ");
        } else {
            whereClause.append(" or ");
        }
        for (int i = 0; i < Math.max(securitiesFieldsNumber, positionsFieldsNumber); i++) {
            if (securitiesFieldsNumber >= i && !SECURITIES_FIELD_NAMES.get(i).equals("securityid")) {
                whereClause.append("s.");
                whereClause.append(SECURITIES_FIELD_NAMES.get(i));
                whereClause.append("=");
                Collections.shuffle(securitiesRowSample);
                Row securitiesRow = securitiesRowSample.get(0);
                String fieldValue = securitiesRow.get(securitiesRow.fieldIndex(SECURITIES_FIELD_NAMES.get(i))).toString();
                whereClause.append("\"");
                whereClause.append(fieldValue);
                whereClause.append("\"");
                if (ThreadLocalRandom.current().nextInt(0, 2) == 0) {
                    whereClause.append(" and ");
                } else {
                    whereClause.append(" or ");
                }
            }
            if (positionsFieldsNumber >= i && !POSITION_FIELD_NAMES.get(i).equals("securityid")) {
                whereClause.append("p.");
                whereClause.append(POSITION_FIELD_NAMES.get(i));
                whereClause.append("=");
                Collections.shuffle(positionsRowSample);
                Row positionsRow = positionsRowSample.get(0);
                whereClause.append("\"");
                String fieldValue = positionsRow.get(positionsRow.fieldIndex(POSITION_FIELD_NAMES.get(i))).toString();
                whereClause.append(fieldValue);
                whereClause.append("\"");
                if (ThreadLocalRandom.current().nextInt(0, 2) == 0) {
                    whereClause.append(" and ");
                } else {
                    whereClause.append(" or ");
                }
            }
        }
        whereClause.deleteCharAt(whereClause.length() - 1);
        whereClause.delete(whereClause.lastIndexOf(" ") + 1, whereClause.length());

        StringBuilder groupByClause = new StringBuilder("group by ");
        for (int i = 0; i < Math.max(securitiesFieldsNumber, positionsFieldsNumber); i++) {
            if (securitiesFieldsNumber >= i) {
                groupByClause.append("s.");
                groupByClause.append(SECURITIES_FIELD_NAMES.get(i));
                groupByClause.append(", ");
            }
            if (positionsFieldsNumber >= i) {
                groupByClause.append("p.");
                groupByClause.append(POSITION_FIELD_NAMES.get(i));
                groupByClause.append(", ");
            }
        }
        groupByClause.deleteCharAt(groupByClause.length() - 2);
        return selectClause + fromClause + whereClause + groupByClause;
    }


}
