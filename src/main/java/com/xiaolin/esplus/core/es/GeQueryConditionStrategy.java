package com.xiaolin.esplus.core.es;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.json.JsonData;

public class GeQueryConditionStrategy implements EsQueryCondition {
    private static volatile EsQueryCondition singleton;

    private GeQueryConditionStrategy(){}

    public static EsQueryCondition getInstance() {
        if (singleton == null) {
            synchronized (GeQueryConditionStrategy.class) {
                if (singleton == null) {
                    singleton = new GeQueryConditionStrategy();
                }
            }
        }
        return singleton;
    }

    @Override
    public Object execute(Query.Builder queryBuilder, String fieldName, String keyword, Float boost, Object... values) {
        Object value = values.length > 0 ? values[0] : null;
        queryBuilder
                .range(rb -> rb
                        .field(fieldName)
                        .gte(JsonData.of(value))
                        .boost(boost));
        return null;
    }


}
