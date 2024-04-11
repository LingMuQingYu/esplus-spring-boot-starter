package com.xiaolin.esplus.core.es;

import co.elastic.clients.elasticsearch._types.query_dsl.Operator;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import com.xiaolin.esplus.utils.EscapeUtil;

import static com.xiaolin.esplus.utils.EsQueryUtil.orQueryString;
import static com.xiaolin.esplus.utils.EsQueryUtil.toFieldValueList;
import static org.springframework.data.elasticsearch.client.elc.Queries.queryStringQuery;

public class NotInQueryConditionStrategy implements EsQueryCondition {
    private static volatile EsQueryCondition singleton;

    private NotInQueryConditionStrategy(){}

    public static EsQueryCondition getInstance() {
        if (singleton == null) {
            synchronized (NotInQueryConditionStrategy.class) {
                if (singleton == null) {
                    singleton = new NotInQueryConditionStrategy();
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
                        .mustNot(mnb -> mnb
                                .terms(tb -> tb
                                        .field(fieldName)
                                        .terms(tsb -> tsb //
                                                .value(toFieldValueList(iterable)))))
                        .boost(boost));
            } else {
                queryBuilder
                        .queryString(qsb -> qsb
                                .fields(fieldName)
                                .query("NOT(" + orQueryString(iterable) + ')')
                                .boost(boost));
            }
        }
        return isKeywordField;
    }


}
