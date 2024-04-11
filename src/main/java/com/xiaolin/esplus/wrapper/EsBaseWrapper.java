package com.xiaolin.esplus.wrapper;


import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.aggregations.AggregationBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import com.xiaolin.esplus.base.EFunction;
import com.xiaolin.esplus.base.EsAggregation;
import com.xiaolin.esplus.base.SortParam;
import com.xiaolin.esplus.constant.ConditionConst;
import com.xiaolin.esplus.utils.EsQueryUtil;
import com.xiaolin.esplus.utils.LambdaUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.client.elc.NativeQueryBuilder;

import java.util.*;
import java.util.function.Consumer;

class EsBaseWrapper {
    public final String AND = "AND";
    public final String OR = "OR";
    public static final String MQ = "mq";
    public static final String NM = "notMq";
    public static final String NESTED_EQ = "nestedEq";
    public static final String NESTED_IN = "nestedIn";

    /**
     * 当前层级条件集
     */
    protected final List<Query> withQueryList = new ArrayList<>();

    protected final Map<String, Aggregation> aggregations = new LinkedHashMap<>();

    protected String condition = AND;

    protected final List<String> selectFields = new ArrayList<>();

    protected final List<SortParam> sorts = new ArrayList<>();

    protected PageRequest pageable;

    protected String nestedPath;

    protected Object updateEntity;

    protected final Map<String, Object> updateMap = new LinkedHashMap<>();

    public static EsWrapper builder() {
        return new EsWrapper();
    }

    /**
     * 指定当前层级条件为OR，默认为AND
     */
    public EsWrapper or() {
        this.condition = OR;
        return (EsWrapper) this;
    }

    /**
     * 嵌套条件，当前层级条件不变，子条件为OR
     */
    public EsWrapper or(Consumer<EsWrapper> consumer) {
        EsWrapper esWrapper = builder().or();
        consumer.accept(esWrapper);
        withQueryList.add(esWrapper.getQuery());
        return (EsWrapper) this;
    }

    /**
     * 嵌套条件，当前层级条件不变，子条件为AND
     */
    public EsWrapper and(Consumer<EsWrapper> consumer) {
        EsWrapper esWrapper = builder();
        consumer.accept(esWrapper);
        withQueryList.add(esWrapper.getQuery());
        return (EsWrapper) this;
    }

    /**
     * 内嵌套关联查询
     */
    public <T> EsWrapper nested(EFunction<T, ?> column, Consumer<EsWrapper> fun) {
        return nested(LambdaUtil.getFieldName(column), fun);
    }

    /**
     * 内嵌套关联查询
     */
    public EsWrapper nested(String path, Consumer<EsWrapper> fun) {
        EsWrapper builder = builder();
        builder.nestedPath = path;
        fun.accept(builder);
        withQueryList.add(QueryBuilders.nested(n -> n.query(builder.getQuery()).path(path)));
        return (EsWrapper) this;
    }

    /**
     * 匹配
     */
    public EsWrapper eq(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.EQ, value);
    }


    /**
     * 不匹配
     */
    public EsWrapper ne(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.NE, value);
    }


    /**
     * 分词匹配
     */
    public EsWrapper mq(String fieldName, Object value) {
        return addCondition(fieldName, MQ, value);
    }

    /**
     * 分词不匹配
     */
    public EsWrapper notMq(String fieldName, Object value) {
        return addCondition(fieldName, NM, value);
    }

    /**
     * 短语查询
     */
    public EsWrapper like(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.LK, value);
    }

    /**
     * 以什么结尾
     */
    public EsWrapper likeLeft(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.LLK, value);
    }

    /**
     * 以什么开头
     */
    public EsWrapper likeRight(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.RLK, value);
    }

    /**
     * 不包含
     */
    public EsWrapper notLike(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.NC, value);
    }

    /**
     * 不以什么结尾
     */
    public EsWrapper notLikeLeft(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.NEL, value);
    }

    /**
     * 不以什么开头
     */
    public EsWrapper notLikeRight(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.NBL, value);
    }

    /**
     * 在其中
     */
    public EsWrapper in(String fieldName, Iterable<?> value) {
        return addCondition(fieldName, ConditionConst.IN, value);
    }

    /**
     * 在其中
     */
    public EsWrapper in(String fieldName, Object... value) {
        return addCondition(fieldName, ConditionConst.IN, Arrays.asList(value));
    }

    /**
     * 不在其中
     */
    public EsWrapper notIn(String fieldName, Iterable<?> value) {
        return addCondition(fieldName, ConditionConst.NIN, value);
    }

    /**
     * 不在其中
     */
    public EsWrapper notIn(String fieldName, Object... value) {
        return addCondition(fieldName, ConditionConst.NIN, Arrays.asList(value));
    }

    /**
     * 大于
     */
    public EsWrapper gt(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.GT, value);
    }

    /**
     * 大于等于
     */
    public EsWrapper ge(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.GE, value);
    }

    /**
     * 小于
     */
    public EsWrapper lt(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.LT, value);
    }

    /**
     * 小于等于
     */
    public EsWrapper le(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.LE, value);
    }

    /**
     * 在XXX之间
     */
    public EsWrapper between(String fieldName, Object value1, Object value2) {
        return addCondition(fieldName, ConditionConst.BT, value1, value2);
    }

    /**
     * 不在XXX之间
     */
    public EsWrapper notBetween(String fieldName, Object value1, Object value2) {
        return addCondition(fieldName, ConditionConst.NBT, value1, value2);
    }

    /**
     * 为空
     */
    public EsWrapper isNull(String fieldName) {
        return addCondition(fieldName, ConditionConst.NU);
    }

    /**
     * 不为空
     */
    public EsWrapper isNotNull(String fieldName) {
        return addCondition(fieldName, ConditionConst.NN);
    }

    /**
     * 添加条件
     */
    public EsWrapper addCondition(String fieldName, String keyword, Object... values) {
        return addCondition(fieldName, keyword, null, values);
    }

    /**
     * 添加条件
     */
    public EsWrapper addCondition(String fieldName, String keyword, Float boost, Object... values) {
        Query.Builder queryBuilder = new Query.Builder();
        if (StringUtils.isNotEmpty(nestedPath)) {
            fieldName = nestedPath + "." + fieldName;
        }
        EsQueryUtil.addCondition(queryBuilder, fieldName, keyword, boost, values);
        withQueryList.add(queryBuilder.build());
        return (EsWrapper) this;
    }


    /**
     * 排序
     */
    public EsWrapper orderBy(SortParam sortParam) {
        sorts.add(sortParam);
        return (EsWrapper) this;
    }

    /**
     * 排序
     */
    public EsWrapper orderBy(String name) {
        return orderBy(name, true);
    }

    /**
     * 排序
     */
    public EsWrapper orderBy(String name, boolean asc) {
        SortParam sortParam = new SortParam();
        sortParam.setName(name);
        sortParam.setAsc(asc);
        return orderBy(sortParam);
    }

    /**
     * 指定更新对象
     */
    public EsWrapper setEntity(Object entity) {
        updateEntity = entity;
        return (EsWrapper) this;
    }

    public Object getEntity() {
        return updateEntity;
    }

    public Map<String, Object> getUpdateMap() {
        return updateMap;
    }

    /**
     * 更新指定字段值
     */
    public EsWrapper set(String fieldName, Object value) {
        updateMap.put(fieldName, value);
        return (EsWrapper) this;
    }

    /**
     * 查询指定字段
     */
    public EsWrapper select(String... fieldNames) {
        this.selectFields.addAll(Arrays.asList(fieldNames));
        return (EsWrapper) this;
    }

    /**
     * 查询指定字段
     */
    public EsWrapper select(Collection<String> fieldNames) {
        selectFields.addAll(fieldNames);
        return (EsWrapper) this;
    }

    public Query getQuery() {
        if (OR.equals(condition)) {
            return QueryBuilders.bool().should(this.withQueryList).build()._toQuery();
        }
        return QueryBuilders.bool().must(this.withQueryList).build()._toQuery();
    }

    public NativeQuery nativeBuild() {
        return build();
    }

    /**
     * 构建Spring Data条件构造器
     */
    public NativeQuery build() {
        return build(queryBuilder -> {
        });
    }

    public EsWrapper group(String name, String fieldName) {
        aggregations.put(name, AggregationBuilders.terms(t -> t.field(fieldName)));
        return (EsWrapper) this;
    }

    /**
     * 多层分组尚未自测，谨慎使用
     *
     * @param consumer 嵌套分组
     * @return es查询条件
     */
    public EsWrapper group(Consumer<EsAggregation> consumer) {
        EsAggregation esAggregation = new EsAggregation();
        consumer.accept(esAggregation);
        aggregations.put(esAggregation.getName(), esAggregation.getAggregation());
        return (EsWrapper) this;
    }

    public EsWrapper pageable(Number pageNumber, Number pageSize) {
        this.pageable = PageRequest.of(pageNumber.intValue() - 1, pageSize.intValue());
        return (EsWrapper) this;
    }

    public NativeQuery build(Consumer<NativeQueryBuilder> consumer) {
        NativeQueryBuilder nativeQueryBuilder = new NativeQueryBuilder();
        nativeQueryBuilder.withQuery(this.getQuery());
        sorts.forEach(sortParam -> {
            Sort sort = Sort.by(sortParam.getName());
            if (sortParam.isAsc()) {
                sort.ascending();
            } else {
                sort.descending();
            }
            nativeQueryBuilder.withSort(sort);
        });
        if (!selectFields.isEmpty()) {
            nativeQueryBuilder.withFields(selectFields);
        }
        if (!aggregations.isEmpty()) {
            aggregations.forEach(nativeQueryBuilder::withAggregation);
            // 只做一条数据的查询
            nativeQueryBuilder.withPageable(PageRequest.of(0, 1));
        }
        if (Objects.nonNull(pageable)) {
            nativeQueryBuilder.withPageable(pageable);
        }
        if (Objects.nonNull(consumer)) {
            consumer.accept(nativeQueryBuilder);
        }
        return nativeQueryBuilder.build();
    }

    public Map<String, Aggregation> getAggregations() {
        return aggregations;
    }

}