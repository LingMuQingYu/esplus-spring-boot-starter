package com.xiaolin.esplus.core.es;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.json.JsonData;

public class BetweenQueryConditionStrategy implements EsQueryCondition {
    private static volatile EsQueryCondition singleton;

    private BetweenQueryConditionStrategy(){}

    public static EsQueryCondition getInstance() {
        if (singleton == null) {
            synchronized (BetweenQueryConditionStrategy.class) {
                if (singleton == null) {
                    singleton = new BetweenQueryConditionStrategy();
                }
            }
        }
        return singleton;
    }

    @Override
    public Object execute(Query.Builder queryBuilder, String fieldName, String keyword, Float boost, Object... values) {
        if (values.length == 2) {
            queryBuilder
                    .range(rb -> {
                        rb.field(fieldName);
                        rb.gte(JsonData.of(values[0]));
                        rb.lte(JsonData.of(values[1]));
                        rb.boost(boost);
                        return rb;
                    });
        }
        return null;
    }


}
