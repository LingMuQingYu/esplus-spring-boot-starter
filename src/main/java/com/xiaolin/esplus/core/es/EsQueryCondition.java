package com.xiaolin.esplus.core.es;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;

public interface EsQueryCondition {

    Object execute(Query.Builder queryBuilder, String fieldName, String keyword, Float boost, Object... values);

}
