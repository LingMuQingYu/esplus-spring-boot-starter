package com.xiaolin.esplus.utils;

import co.elastic.clients.elasticsearch._types.aggregations.*;
import com.xiaolin.esplus.wrapper.EsWrapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.elasticsearch.client.elc.Aggregation;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchAggregation;
import org.springframework.data.elasticsearch.core.AggregationsContainer;
import org.springframework.data.elasticsearch.core.SearchHits;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EsGroupUtil {
    public static <T> List<Map<String, Object>> groupCountList(SearchHits<T> hits, EsWrapper wrapper, Class<T> clazz) {
        List<Map<String, Object>> list = new ArrayList<>();
        AggregationsContainer<?> aggregations = hits.getAggregations();
        if (aggregations != null) {
            Map<String, String> fieldMap = new HashMap<>();
            setFieldMap(fieldMap, wrapper.getAggregations());
            List<ElasticsearchAggregation> esAggregations = (List<ElasticsearchAggregation>) aggregations.aggregations();
            for (ElasticsearchAggregation ea : esAggregations) {
                if (ea != null) {
                    Aggregation aggregation = ea.aggregation();
                    Aggregate aggregate = aggregation.getAggregate();
                    groupTermsMap(aggregation.getName(), aggregate, new HashMap<>(), fieldMap, list);
                }
            }
        }
        return list;
    }

    private static void setFieldMap(Map<String, String> fieldMap, Map<String, co.elastic.clients.elasticsearch._types.aggregations.Aggregation> aggremap) {
        aggremap.forEach((k, v) -> {
            if (v._get() instanceof TermsAggregation termsAggregation) {
                String field = termsAggregation.field();
                if (StringUtils.isNotBlank(field)) {
                    fieldMap.put(k, field);
                }
            }
            if (v.aggregations() != null && !v.aggregations().isEmpty()) {
                setFieldMap(fieldMap, v.aggregations());
            }
        });
    }

    private static void groupTermsMap(String aggName, Aggregate aggregate, Map<String, Object> parentMap, Map<String, String> fieldMap, List<Map<String, Object>> list) {
        String field = fieldMap.get(aggName);
        Aggregate.Kind kind = aggregate._kind();
        if (Aggregate.Kind.Sterms.equals(kind)) {
            getStermsBuckets(field, aggregate.sterms().buckets(), parentMap, fieldMap, list);
        } else if (Aggregate.Kind.Lterms.equals(kind)) {
            getLtermsBuckets(field, aggregate.lterms().buckets(), parentMap, fieldMap, list);
        } else if (Aggregate.Kind.Dterms.equals(kind)) {
            getDtermsBuckets(field, aggregate.dterms().buckets(), parentMap, fieldMap, list);
        }
    }

    private static void getDtermsBuckets(String field, Buckets<DoubleTermsBucket> buckets, Map<String, Object> parentMap, Map<String, String> fieldMap, List<Map<String, Object>> list) {
        List<DoubleTermsBucket> bucketBases = buckets.array();
        bucketBases.forEach(ba -> {
            // 获取键key值
            String key = String.valueOf(ba.key());
            // 获取文档数
            long count = ba.docCount();
            // 设置本级聚合信息
            buildItemMap(field, key, count, ba.aggregations(), parentMap, fieldMap, list);
        });
    }

    private static void getLtermsBuckets(String field, Buckets<LongTermsBucket> buckets, Map<String, Object> parentMap, Map<String, String> fieldMap, List<Map<String, Object>> list) {
        List<LongTermsBucket> bucketBases = buckets.array();
        bucketBases.forEach(ba -> {
            // 获取键key值
            String key = String.valueOf(ba.key());
            // 获取文档数
            long count = ba.docCount();
            // 设置本级聚合信息
            buildItemMap(field, key, count, ba.aggregations(), parentMap, fieldMap, list);
        });
    }

    private static void getStermsBuckets(String field, Buckets<StringTermsBucket> buckets, Map<String, Object> parentMap, Map<String, String> fieldMap, List<Map<String, Object>> list) {
        List<StringTermsBucket> bucketBases = buckets.array();
        bucketBases.forEach(ba -> {
            // 获取键key值
            String key = ba.key().stringValue();
            // 获取文档数
            long count = ba.docCount();
            // 设置本级聚合信息
            buildItemMap(field, key, count, ba.aggregations(), parentMap, fieldMap, list);
        });
    }

    private static void buildItemMap(String field, String key, Long count, Map<String, Aggregate> aggregations, Map<String, Object> parentMap, Map<String, String> fieldMap, List<Map<String, Object>> list) {
        HashMap<String, Object> itemMap = new HashMap<>(parentMap);
        itemMap.put(field, key);
        if (aggregations.isEmpty()) {
            // 获取文档数
            itemMap.put("count", count);
            list.add(itemMap);
        } else {
            // 递归获取聚合map
            aggregations.forEach((name, value) -> groupTermsMap(name, value, itemMap, fieldMap, list));
        }
    }
}
