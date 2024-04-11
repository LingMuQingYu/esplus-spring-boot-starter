package com.xiaolin.esplus.core.es;

import co.elastic.clients.elasticsearch._types.query_dsl.Operator;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.json.JsonData;
import com.xiaolin.esplus.utils.EscapeUtil;

import static org.springframework.data.elasticsearch.client.elc.Queries.queryStringQuery;

public class GtQueryConditionStrategy implements EsQueryCondition {
    private static volatile EsQueryCondition singleton;

    private GtQueryConditionStrategy(){}

    public static EsQueryCondition getInstance() {
        if (singleton == null) {
            synchronized (GtQueryConditionStrategy.class) {
                if (singleton == null) {
                    singleton = new GtQueryConditionStrategy();
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
                        .gt(JsonData.of(value))
                        .boost(boost));
        return null;
    }


}
