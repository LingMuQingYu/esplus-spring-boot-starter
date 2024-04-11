package com.xiaolin.esplus.core.es;

import co.elastic.clients.elasticsearch._types.query_dsl.Operator;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.json.JsonData;
import com.xiaolin.esplus.utils.EscapeUtil;

import static org.springframework.data.elasticsearch.client.elc.Queries.queryStringQuery;

public class NotBetweenQueryConditionStrategy implements EsQueryCondition {
    private static volatile EsQueryCondition singleton;

    private NotBetweenQueryConditionStrategy(){}

    public static EsQueryCondition getInstance() {
        if (singleton == null) {
            synchronized (NotBetweenQueryConditionStrategy.class) {
                if (singleton == null) {
                    singleton = new NotBetweenQueryConditionStrategy();
                }
            }
        }
        return singleton;
    }

    @Override
    public Object execute(Query.Builder queryBuilder, String fieldName, String keyword, Float boost, Object... values) {
        if (values.length == 2) {
            queryBuilder.bool(b -> b.mustNot(n ->
                    n.range(rb -> {
                        rb.field(fieldName);
                        rb.gte(JsonData.of(values[0]));
                        rb.lte(JsonData.of(values[1]));
                        rb.boost(boost);
                        return rb;
                    })
            ));
        }
        return null;
    }


}
