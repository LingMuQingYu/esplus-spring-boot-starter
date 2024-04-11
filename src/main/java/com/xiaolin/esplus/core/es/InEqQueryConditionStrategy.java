package com.xiaolin.esplus.core.es;

import co.elastic.clients.elasticsearch._types.query_dsl.Operator;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import com.xiaolin.esplus.utils.EscapeUtil;

import static com.xiaolin.esplus.utils.EsQueryUtil.orQueryString;
import static com.xiaolin.esplus.utils.EsQueryUtil.toFieldValueList;
import static org.springframework.data.elasticsearch.client.elc.Queries.queryStringQuery;

public class InEqQueryConditionStrategy implements EsQueryCondition {
    private static volatile EsQueryCondition singleton;

    private InEqQueryConditionStrategy(){}

    public static EsQueryCondition getInstance() {
        if (singleton == null) {
            synchronized (InEqQueryConditionStrategy.class) {
                if (singleton == null) {
                    singleton = new InEqQueryConditionStrategy();
                }
            }
        }
        return singleton;
    }

    @Override
    public Object execute(Query.Builder queryBuilder, String fieldName, String keyword, Float boost, Object... values) {
        Object value = values.length > 0 ? values[0] : null;
        boolean isKeywordField = true;
        if (value instanceof Iterable<?> iterable) {
            if (isKeywordField) {
                queryBuilder.bool(bb -> bb
                        .must(mb -> mb
                                .terms(tb -> tb //
                                        .field(fieldName)
                                        .terms(tsb -> tsb //
                                                .value(toFieldValueList(iterable)))))
                        .boost(boost));
            } else {
                queryBuilder
                        .queryString(qsb -> qsb
                                .fields(fieldName)
                                .query(orQueryString(iterable))
                                .boost(boost));
            }
        }
        return isKeywordField;
    }


}
