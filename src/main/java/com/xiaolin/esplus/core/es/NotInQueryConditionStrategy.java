package com.xiaolin.esplus.core.es;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import com.xiaolin.esplus.utils.EsToolsUtil;


public class NotInQueryConditionStrategy implements EsQueryCondition {
    private static volatile EsQueryCondition singleton;

    private NotInQueryConditionStrategy(){}

    public static EsQueryCondition getInstance() {
        if (singleton == null) {
            synchronized (NotInQueryConditionStrategy.class) {
                if (singleton == null) {
                    singleton = new NotInQueryConditionStrategy();
                }
            }
        }
        return singleton;
    }

    @Override
    public Object execute(Query.Builder queryBuilder, String fieldName, String keyword, Float boost, Object... values) {
        Object value = values.length > 0 ? values[0] : null;
        boolean isKeywordField = true;
        if (value instanceof Iterable<?> iterable) {
            if (isKeywordField) {
                queryBuilder.bool(bb -> bb
                        .mustNot(mnb -> mnb
                                .terms(tb -> tb
                                        .field(fieldName)
                                        .terms(tsb -> tsb //
                                                .value(EsToolsUtil.toFieldValueList(iterable)))))
                        .boost(boost));
            } else {
                queryBuilder
                        .queryString(qsb -> qsb
                                .fields(fieldName)
                                .query("NOT(" + EsToolsUtil.orQueryString(iterable) + ')')
                                .boost(boost));
            }
        }
        return isKeywordField;
    }


}
