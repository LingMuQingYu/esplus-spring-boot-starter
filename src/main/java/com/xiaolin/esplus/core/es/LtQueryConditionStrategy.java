package com.xiaolin.esplus.core.es;

import co.elastic.clients.elasticsearch._types.query_dsl.Operator;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.json.JsonData;
import com.xiaolin.esplus.utils.EscapeUtil;

import static org.springframework.data.elasticsearch.client.elc.Queries.queryStringQuery;

public class LtQueryConditionStrategy implements EsQueryCondition {
    private static volatile EsQueryCondition singleton;

    private LtQueryConditionStrategy(){}

    public static EsQueryCondition getInstance() {
        if (singleton == null) {
            synchronized (LtQueryConditionStrategy.class) {
                if (singleton == null) {
                    singleton = new LtQueryConditionStrategy();
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
                        .lt(JsonData.of(value))
                        .boost(boost));
        return null;
    }


}
