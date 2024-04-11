package com.xiaolin.esplus.core.es;

import co.elastic.clients.elasticsearch._types.query_dsl.Operator;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.json.JsonData;
import com.xiaolin.esplus.utils.EscapeUtil;

import static org.springframework.data.elasticsearch.client.elc.Queries.matchQuery;
import static org.springframework.data.elasticsearch.client.elc.Queries.queryStringQuery;

public class MqQueryConditionStrategy implements EsQueryCondition {
    private static volatile EsQueryCondition singleton;

    private MqQueryConditionStrategy(){}

    public static EsQueryCondition getInstance() {
        if (singleton == null) {
            synchronized (MqQueryConditionStrategy.class) {
                if (singleton == null) {
                    singleton = new MqQueryConditionStrategy();
                }
            }
        }
        return singleton;
    }

    @Override
    public Object execute(Query.Builder queryBuilder, String fieldName, String keyword, Float boost, Object... values) {
        Object value = values.length > 0 ? values[0] : null;
        queryBuilder.match(matchQuery(fieldName, String.valueOf(value), Operator.Or, boost));
        return null;
    }


}
