package com.xiaolin.esplus.utils;

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.Operator;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.json.JsonData;
import com.xiaolin.esplus.constant.ConditionConst;
import com.xiaolin.esplus.core.EsQueryConditionFactory;
import com.xiaolin.esplus.core.es.EsQueryCondition;

import java.util.ArrayList;
import java.util.List;

import static org.springframework.data.elasticsearch.client.elc.Queries.matchQuery;
import static org.springframework.data.elasticsearch.client.elc.Queries.queryStringQuery;

public class EsQueryUtil {
    /**
     * 添加条件
     */
    public static void addCondition(Query.Builder queryBuilder, String fieldName, String keyword, Float boost, Object... values) {
        if (!ConditionConst.NU.equals(keyword)
                && !ConditionConst.NN.equals(keyword)
                && (values == null || values.length == 0)) {
            return;
        }
        EsQueryCondition beanQueryCondition = EsQueryConditionFactory.getBeanQueryCondition(keyword);
        beanQueryCondition.execute(queryBuilder,fieldName,keyword,boost,values);
    }

    public static List<FieldValue> toFieldValueList(Iterable<?> iterable) {
        List<FieldValue> list = new ArrayList<>();
        for (Object item : iterable) {
            list.add(item != null ? FieldValue.of(item.toString()) : null);
        }
        return list;
    }

    public static String orQueryString(Iterable<?> iterable) {
        StringBuilder sb = new StringBuilder();
        for (Object item : iterable) {
            if (item != null) {
                if (!sb.isEmpty()) {
                    sb.append(' ');
                }
                sb.append('"');
                sb.append(EscapeUtil.escape(item.toString()));
                sb.append('"');
            }
        }
        return sb.toString();
    }
}
