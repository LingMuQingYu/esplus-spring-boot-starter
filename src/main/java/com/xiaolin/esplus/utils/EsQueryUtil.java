package com.xiaolin.esplus.utils;

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.Operator;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.json.JsonData;
import com.xiaolin.esplus.constant.ConditionConst;

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
        boolean isKeywordField = true;
        Object value = values.length > 0 ? values[0] : null;
        String searchText = value != null ? EscapeUtil.escape(String.valueOf(value)) : "UNKNOWN_VALUE";
        switch (keyword) {
            case ConditionConst.EQ:
                queryBuilder.queryString(queryStringQuery(fieldName, searchText, Operator.And, boost));
                break;
            case ConditionConst.NE:
                queryBuilder.bool(b -> b.mustNot(q -> q.queryString(queryStringQuery(fieldName, searchText, Operator.And, boost))));
                break;
            case ConditionConst.LK:
                queryBuilder.queryString(queryStringQuery(fieldName, '*' + searchText + '*', true, boost));
                break;
            case ConditionConst.LLK:
                queryBuilder.queryString(queryStringQuery(fieldName, '*' + searchText, true, boost));
                break;
            case ConditionConst.RLK:
                queryBuilder.queryString(queryStringQuery(fieldName, searchText + '*', true, boost));
                break;
            case ConditionConst.NC:
                queryBuilder.bool(b -> b.mustNot(q -> q.queryString(queryStringQuery(fieldName, '*' + searchText + '*', true, boost))));
                break;
            case ConditionConst.NEL:
                queryBuilder.bool(b -> b.mustNot(q -> q.queryString(queryStringQuery(fieldName, '*' + searchText, true, boost))));
                break;
            case ConditionConst.NBL:
                queryBuilder.bool(b -> b.mustNot(q -> q.queryString(queryStringQuery(fieldName, searchText + '*', true, boost))));
                break;
            case ConditionConst.IN:
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
                break;
            case ConditionConst.NIN:
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
                break;
            case ConditionConst.GT:
                queryBuilder
                        .range(rb -> rb
                                .field(fieldName)
                                .gt(JsonData.of(value))
                                .boost(boost));
                break;
            case ConditionConst.GE:
                queryBuilder
                        .range(rb -> rb
                                .field(fieldName)
                                .gte(JsonData.of(value))
                                .boost(boost));
                break;
            case ConditionConst.LT:
                queryBuilder
                        .range(rb -> rb
                                .field(fieldName)
                                .lt(JsonData.of(value))
                                .boost(boost));
                break;
            case ConditionConst.LE:
                queryBuilder
                        .range(rb -> rb
                                .field(fieldName)
                                .lte(JsonData.of(value))
                                .boost(boost));
                break;
            case ConditionConst.BT:
                if (values.length == 2) {
                    queryBuilder
                            .range(rb -> {
                                rb.field(fieldName);
                                rb.gte(JsonData.of(values[0]));
                                rb.lte(JsonData.of(values[1]));
                                rb.boost(boost);
                                return rb;
                            });
                }
                break;
            case ConditionConst.NBT:
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
                break;
            case ConditionConst.NU:
                queryBuilder
                        .bool(bb -> bb
                                .must(mb -> mb
                                        .exists(eb -> eb
                                                .field(fieldName)
                                        ))
                                .mustNot(mnb -> mnb
                                        .wildcard(wb -> wb
                                                .field(fieldName)
                                                .wildcard("*")))
                                .boost(boost));
                break;
            case ConditionConst.NN:
                queryBuilder
                        .wildcard(wb -> wb
                                .field(fieldName)
                                .wildcard("*")
                                .boost(boost));
                break;
            case ConditionConst.MQ:
                queryBuilder.match(matchQuery(fieldName, String.valueOf(value), Operator.Or, boost));
                break;
            case ConditionConst.NM:
                queryBuilder.bool(b -> b.mustNot(mnb -> mnb.match(matchQuery(fieldName, String.valueOf(value), Operator.Or, boost))));
                break;
            default:
                break;
        }
    }

    private static List<FieldValue> toFieldValueList(Iterable<?> iterable) {
        List<FieldValue> list = new ArrayList<>();
        for (Object item : iterable) {
            list.add(item != null ? FieldValue.of(item.toString()) : null);
        }
        return list;
    }

    private static String orQueryString(Iterable<?> iterable) {
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
