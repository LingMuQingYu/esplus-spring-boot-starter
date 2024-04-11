package com.xiaolin.esplus.wrapper;

import co.elastic.clients.elasticsearch._types.aggregations.AggregationBuilders;
import com.byd.ecm.common.core.base.ConditionConst;
import com.byd.ecm.common.es.base.EFunction;
import com.byd.ecm.common.es.base.ESupplier;
import com.byd.ecm.common.es.utils.LambdaUtil;

import java.util.Arrays;

class EsLambdaWrapper extends EsBaseWrapper {

    /**
     * 匹配
     */
    public EsWrapper eq(ESupplier<?> column) {
        return addCondition(column, ConditionConst.EQ);
    }

    /**
     * 匹配
     */
    public <T> EsWrapper eq(EFunction<T, ?> column, Object value) {
        return addCondition(column, ConditionConst.EQ, value);
    }


    /**
     * 不匹配
     */
    public EsWrapper ne(ESupplier<?> column) {
        return addCondition(column, ConditionConst.NE);
    }

    /**
     * 不匹配
     */
    public <T> EsWrapper ne(EFunction<T, ?> column, Object value) {
        return addCondition(column, ConditionConst.NE, value);
    }

    /**
     * 分词匹配
     */
    public EsWrapper mq(ESupplier<?> column) {
        return addCondition(column, MQ);
    }

    /**
     * 分词匹配
     */
    public <T> EsWrapper mq(EFunction<T, ?> column, Object value) {
        return addCondition(column, MQ, value);
    }

    /**
     * 分词不匹配
     */
    public EsWrapper notMq(ESupplier<?> column) {
        return addCondition(column, NM);
    }

    /**
     * 分词不匹配
     */
    public <T> EsWrapper notMq(EFunction<T, ?> column, Object value) {
        return addCondition(column, NM, value);
    }

    /**
     * 短语查询
     */
    public EsWrapper like(ESupplier<?> column) {
        return addCondition(column, ConditionConst.LK);
    }

    /**
     * 短语查询
     */
    public <T> EsWrapper like(EFunction<T, ?> column, Object value) {
        return addCondition(column, ConditionConst.LK, value);
    }

    /**
     * 以什么结尾
     */
    public EsWrapper likeLeft(ESupplier<?> column) {
        return addCondition(column, ConditionConst.LLK);
    }

    /**
     * 以什么结尾
     */
    public <T> EsWrapper likeLeft(EFunction<T, ?> column, Object value) {
        return addCondition(column, ConditionConst.LLK, value);
    }

    /**
     * 以什么开头
     */
    public EsWrapper likeRight(ESupplier<?> column) {
        return addCondition(column, ConditionConst.RLK);
    }

    /**
     * 以什么开头
     */
    public <T> EsWrapper likeRight(EFunction<T, ?> column, Object value) {
        return addCondition(column, ConditionConst.RLK, value);
    }

    /**
     * 不包含
     */
    public <T> EsWrapper notLike(ESupplier<?> column) {
        return addCondition(column, ConditionConst.NC);
    }

    /**
     * 不包含
     */
    public <T> EsWrapper notLike(EFunction<T, ?> column, Object value) {
        return addCondition(column, ConditionConst.NC, value);
    }

    /**
     * 不以什么结尾
     */
    public EsWrapper notLikeLeft(ESupplier<?> column) {
        return addCondition(column, ConditionConst.NEL);
    }

    /**
     * 不以什么结尾
     */
    public <T> EsWrapper notLikeLeft(EFunction<T, ?> column, Object value) {
        return addCondition(column, ConditionConst.NEL, value);
    }

    /**
     * 不以什么开头
     */
    public EsWrapper notLikeRight(ESupplier<?> column) {
        return addCondition(column, ConditionConst.NBL);
    }

    /**
     * 不以什么开头
     */
    public <T> EsWrapper notLikeRight(EFunction<T, ?> column, Object value) {
        return addCondition(column, ConditionConst.NBL, value);
    }

    /**
     * 在其中
     */
    public <T> EsWrapper in(EFunction<T, ?> column, Iterable<?> value) {
        return addCondition(column, ConditionConst.IN, value);
    }

    /**
     * 在其中
     */
    public <T> EsWrapper in(EFunction<T, ?> column, Object... value) {
        return addCondition(column, ConditionConst.IN, Arrays.asList(value));
    }

    /**
     * 不在其中
     */
    public <T> EsWrapper notIn(EFunction<T, ?> column, Iterable<?> value) {
        return addCondition(column, ConditionConst.NIN, value);
    }

    /**
     * 不在其中
     */
    public <T> EsWrapper notIn(EFunction<T, ?> column, Object... value) {
        return addCondition(column, ConditionConst.NIN, Arrays.asList(value));
    }

    /**
     * 大于
     */
    public EsWrapper gt(ESupplier<?> column) {
        return addCondition(column, ConditionConst.GT);
    }

    /**
     * 大于
     */
    public <T> EsWrapper gt(EFunction<T, ?> column, Object value) {
        return addCondition(column, ConditionConst.GT, value);
    }

    /**
     * 大于等于
     */
    public EsWrapper ge(ESupplier<?> column) {
        return addCondition(column, ConditionConst.GE);
    }

    /**
     * 大于等于
     */
    public <T> EsWrapper ge(EFunction<T, ?> column, Object value) {
        return addCondition(column, ConditionConst.GE, value);
    }

    /**
     * 小于
     */
    public EsWrapper lt(ESupplier<?> column) {
        return addCondition(column, ConditionConst.LT);
    }

    /**
     * 小于
     */
    public <T> EsWrapper lt(EFunction<T, ?> column, Object value) {
        return addCondition(column, ConditionConst.LT, value);
    }

    /**
     * 小于等于
     */
    public EsWrapper le(ESupplier<?> column) {
        return addCondition(column, ConditionConst.LE);
    }

    /**
     * 小于等于
     */
    public <T> EsWrapper le(EFunction<T, ?> column, Object value) {
        return addCondition(column, ConditionConst.LE, value);
    }

    /**
     * 在XXX之间
     */
    public <T> EsWrapper between(EFunction<T, ?> column, Object value1, Object value2) {
        return addCondition(column, ConditionConst.BT, value1, value2);
    }

    /**
     * 不在XXX之间
     */
    public <T> EsWrapper notBetween(EFunction<T, ?> column, Object value1, Object value2) {
        return addCondition(column, ConditionConst.NBT, value1, value2);
    }

    /**
     * 为空
     */
    public EsWrapper isNull(ESupplier<?> column) {
        return addCondition(column, ConditionConst.NU);
    }

    /**
     * 为空
     */
    public <T> EsWrapper isNull(EFunction<T, ?> column) {
        return addCondition(column, ConditionConst.NU);
    }

    /**
     * 不为空
     */
    public EsWrapper isNotNull(ESupplier<?> column) {
        return addCondition(column, ConditionConst.NN);
    }

    /**
     * 不为空
     */
    public <T> EsWrapper isNotNull(EFunction<T, ?> column) {
        return addCondition(column, ConditionConst.NN);
    }

    /**
     * 添加条件
     */
    public EsWrapper addCondition(ESupplier<?> column, String keyword) {
        return addCondition(LambdaUtil.getFieldName(column), keyword, column.get());
    }

    /**
     * 添加条件
     */
    public <T> EsWrapper addCondition(EFunction<T, ?> column, String keyword, Object... values) {
        return addCondition(LambdaUtil.getFieldName(column), keyword, values);
    }

    /**
     * 排序
     */
    public EsWrapper orderBy(ESupplier<?> column) {
        return orderBy(LambdaUtil.getFieldName(column), true);
    }

    /**
     * 排序
     */
    public <T> EsWrapper orderBy(EFunction<T, ?> column) {
        return orderBy(LambdaUtil.getFieldName(column), true);
    }

    /**
     * 排序
     */
    public <T> EsWrapper orderBy(EFunction<T, ?> column, boolean asc) {
        return orderBy(LambdaUtil.getFieldName(column), asc);
    }

    /**
     * 更新指定字段值
     */
    public EsWrapper set(ESupplier<?> column) {
        updateMap.put(LambdaUtil.getFieldName(column), column.get());
        return (EsWrapper) this;
    }

    /**
     * 更新指定字段值
     */
    public <T> EsWrapper set(EFunction<T, ?> column, Object value) {
        updateMap.put(LambdaUtil.getFieldName(column), value);
        return (EsWrapper) this;
    }

    /**
     * 查询指定字段
     */
    @SafeVarargs
    public final <T> EsWrapper select(EFunction<T, ?>... column) {
        this.selectFields.addAll(Arrays.stream(column).map(LambdaUtil::getFieldName).toList());
        return (EsWrapper) this;
    }

    public <T> EsWrapper group(String name, EFunction<T, ?> column) {
        aggregations.put(name, AggregationBuilders.terms(t -> t.field(LambdaUtil.getFieldName(column))));
        return (EsWrapper) this;
    }

}
