package com.xiaolin.esplus.core.es;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;

public class DefaultQueryConditionStrategy implements EsQueryCondition {
    private static volatile EsQueryCondition singleton;

    private DefaultQueryConditionStrategy(){}

    public static EsQueryCondition getInstance() {
        if (singleton == null) {
            synchronized (DefaultQueryConditionStrategy.class) {
                if (singleton == null) {
                    singleton = new DefaultQueryConditionStrategy();
                }
            }
        }
        return singleton;
    }

    @Override
    public Object execute(Query.Builder queryBuilder, String fieldName, String keyword, Float boost, Object... values) {
        return null;
    }


}
