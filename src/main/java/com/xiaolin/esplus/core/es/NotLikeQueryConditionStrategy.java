package com.xiaolin.esplus.core.es;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import com.xiaolin.esplus.utils.EsToolsUtil;

import static org.springframework.data.elasticsearch.client.elc.Queries.queryStringQuery;

public class NotLikeQueryConditionStrategy implements EsQueryCondition {
    private static volatile EsQueryCondition singleton;

    private NotLikeQueryConditionStrategy(){}

    public static EsQueryCondition getInstance() {
        if (singleton == null) {
            synchronized (NotLikeQueryConditionStrategy.class) {
                if (singleton == null) {
                    singleton = new NotLikeQueryConditionStrategy();
                }
            }
        }
        return singleton;
    }

    @Override
    public Object execute(Query.Builder queryBuilder, String fieldName, String keyword, Float boost, Object... values) {
        Object value = values.length > 0 ? values[0] : null;
        String searchText = value != null ? EsToolsUtil.escape(String.valueOf(value)) : "UNKNOWN_VALUE";
        queryBuilder.bool(b -> b.mustNot(q -> q.queryString(queryStringQuery(fieldName, '*' + searchText, true, boost))));
        return null;
    }


}
