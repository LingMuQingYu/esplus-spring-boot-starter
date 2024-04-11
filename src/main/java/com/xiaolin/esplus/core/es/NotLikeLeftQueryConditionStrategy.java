package com.xiaolin.esplus.core.es;

import co.elastic.clients.elasticsearch._types.query_dsl.Operator;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import com.xiaolin.esplus.utils.EscapeUtil;

import static org.springframework.data.elasticsearch.client.elc.Queries.queryStringQuery;

public class NotLikeLeftQueryConditionStrategy implements EsQueryCondition {
    private static volatile EsQueryCondition singleton;

    private NotLikeLeftQueryConditionStrategy(){}

    public static EsQueryCondition getInstance() {
        if (singleton == null) {
            synchronized (NotLikeLeftQueryConditionStrategy.class) {
                if (singleton == null) {
                    singleton = new NotLikeLeftQueryConditionStrategy();
                }
            }
        }
        return singleton;
    }

    @Override
    public Object execute(Query.Builder queryBuilder, String fieldName, String keyword, Float boost, Object... values) {
        Object value = values.length > 0 ? values[0] : null;
        String searchText = value != null ? EscapeUtil.escape(String.valueOf(value)) : "UNKNOWN_VALUE";
        queryBuilder.queryString(queryStringQuery(fieldName, searchText, Operator.And, boost));
        return null;
    }


}
