package com.xiaolin.esplus.base;

import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.aggregations.AggregationBuilders;
import co.elastic.clients.util.NamedValue;
import com.xiaolin.esplus.utils.LambdaUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class EsAggregation {
    private String name;
    private String fieldName;

    private Integer size;

    private final List<NamedValue<SortOrder>> sortOrderList = new ArrayList<>();

    private final List<EsAggregation> subAggregations = new ArrayList<>();

    public EsAggregation terms(String name, String fieldName) {
        this.name = name;
        this.fieldName = fieldName;
        return this;
    }

    public <T> EsAggregation terms(String name, EFunction<T, ?> column) {
        this.name = name;
        this.fieldName = LambdaUtil.getFieldName(column);
        return this;
    }

    public EsAggregation subGroup(Consumer<EsAggregation> fun) {
        EsAggregation esAggregation = new EsAggregation();
        subAggregations.add(esAggregation);
        fun.accept(esAggregation);
        return this;
    }

    public String getName() {
        return name;
    }


    public List<EsAggregation> getSubAggregations() {
        return subAggregations;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Aggregation getAggregation() {
        // TODO 需要解决order问题，目前使用order会报错
        Aggregation terms;
//            if (this.sortOrderList.isEmpty()) {
//                terms = AggregationBuilders.terms(t -> t.field(this.fieldName).size(this.size).order(this.sortOrderList));
//            } else {
        terms = AggregationBuilders.terms(t -> t.field(this.fieldName).size(this.size));
//            }
        subAggregations.forEach(e -> terms.aggregations().put(e.getName(), e.getAggregation()));
        return terms;
    }

    public EsAggregation order(String name, boolean isAsc) {
        sortOrderList.add(NamedValue.of(name, isAsc ? SortOrder.Asc : SortOrder.Desc));
        return this;
    }

    public EsAggregation size(int size) {
        this.size = size;
        return this;
    }
}