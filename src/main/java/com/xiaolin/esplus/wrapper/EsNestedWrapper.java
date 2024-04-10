package com.xiaolin.esplus.wrapper;

import com.xiaolin.esplus.base.EFunction;
import com.xiaolin.esplus.base.ESupplier;
import com.xiaolin.esplus.constant.ConditionConst;
import com.xiaolin.esplus.utils.LambdaUtil;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.data.elasticsearch.core.query.SimpleField;

import java.util.Arrays;
import java.util.function.Consumer;

class EsNestedWrapper extends EsLambdaWrapper {

    private boolean nestedWrapper = false;

    private Criteria nestedCriteria = null;

    /**
     * 内嵌套关联查询
     */
    public EsWrapper nested(Consumer<EsWrapper> fun) {
        EsWrapper builder = builder();
        builder.nestedWrapper();
        fun.accept(builder);
        criteriaList.add(builder.getNestedCriteria());
        return (EsWrapper) this;
    }

    protected void nestedWrapper() {
        this.nestedWrapper = true;
    }

    public Criteria getNestedCriteria() {
        return nestedCriteria;
    }

    /**
     * 匹配
     */
    public EsWrapper eq(String fieldName, String subFieldName, Object value) {
        return addCondition(fieldName, subFieldName, ConditionConst.EQ, value);
    }

    /**
     * 匹配
     */
    public <T> EsWrapper eq(EFunction<T, ?> column, ESupplier<?> subColumn) {
        return addCondition(column, subColumn, ConditionConst.EQ);
    }

    /**
     * 匹配
     */
    public <T, S> EsWrapper eq(EFunction<T, ?> column, EFunction<S, ?> subColumn, Object value) {
        return addCondition(column, subColumn, ConditionConst.EQ, value);
    }

    /**
     * 不匹配
     */
    public EsWrapper ne(String fieldName, String subFieldName, Object value) {
        return addCondition(fieldName, subFieldName, ConditionConst.NE, value);
    }

    /**
     * 不匹配
     */
    public <T> EsWrapper ne(EFunction<T, ?> column, ESupplier<?> subColumn) {
        return addCondition(column, subColumn, ConditionConst.NE);
    }

    /**
     * 不匹配
     */
    public <T, S> EsWrapper ne(EFunction<T, ?> column, EFunction<S, ?> subColumn, Object value) {
        return addCondition(column, subColumn, ConditionConst.NE, value);
    }

    /**
     * 分词匹配
     */
    public EsWrapper mq(String fieldName, String subFieldName, Object value) {
        return addCondition(fieldName, subFieldName, MQ, value);
    }

    /**
     * 分词匹配
     */
    public <T> EsWrapper mq(EFunction<T, ?> column, ESupplier<?> subColumn) {
        return addCondition(column, subColumn, MQ);
    }

    /**
     * 分词匹配
     */
    public <T, S> EsWrapper mq(EFunction<T, ?> column, EFunction<S, ?> subColumn, Object value) {
        return addCondition(column, subColumn, MQ, value);
    }

    /**
     * 分词不匹配
     */
    public EsWrapper notMq(String fieldName, String subFieldName, Object value) {
        return addCondition(fieldName, subFieldName, NM, value);
    }

    /**
     * 分词不匹配
     */
    public <T> EsWrapper notMq(EFunction<T, ?> column, ESupplier<?> subColumn) {
        return addCondition(column, subColumn, NM);
    }

    /**
     * 分词不匹配
     */
    public <T, S> EsWrapper notMq(EFunction<T, ?> column, EFunction<S, ?> subColumn, Object value) {
        return addCondition(column, subColumn, NM, value);
    }

    /**
     * 短语查询
     */
    public EsWrapper like(String fieldName, String subFieldName, Object value) {
        return addCondition(fieldName, subFieldName, ConditionConst.LK, value);
    }

    /**
     * 短语查询
     */
    public <T> EsWrapper like(EFunction<T, ?> column, ESupplier<?> subColumn) {
        return addCondition(column, subColumn, ConditionConst.LK);
    }

    /**
     * 短语查询
     */
    public <T, S> EsWrapper like(EFunction<T, ?> column, EFunction<S, ?> subColumn, Object value) {
        return addCondition(column, subColumn, ConditionConst.LK, value);
    }

    /**
     * 以什么结尾
     */
    public EsWrapper likeLeft(String fieldName, String subFieldName, Object value) {
        return addCondition(fieldName, subFieldName, ConditionConst.LLK, value);
    }

    /**
     * 以什么结尾
     */
    public <T> EsWrapper likeLeft(EFunction<T, ?> column, ESupplier<?> subColumn) {
        return addCondition(column, subColumn, ConditionConst.LLK);
    }

    /**
     * 以什么结尾
     */
    public <T, S> EsWrapper likeLeft(EFunction<T, ?> column, EFunction<S, ?> subColumn, Object value) {
        return addCondition(column, subColumn, ConditionConst.LLK, value);
    }

    /**
     * 以什么开头
     */
    public EsWrapper likeRight(String fieldName, String subFieldName, Object value) {
        return addCondition(fieldName, subFieldName, ConditionConst.RLK, value);
    }

    /**
     * 以什么开头
     */
    public <T> EsWrapper likeRight(EFunction<T, ?> column, ESupplier<?> subColumn) {
        return addCondition(column, subColumn, ConditionConst.RLK);
    }

    /**
     * 以什么开头
     */
    public <T, S> EsWrapper likeRight(EFunction<T, ?> column, EFunction<S, ?> subColumn, Object value) {
        return addCondition(column, subColumn, ConditionConst.RLK, value);
    }

    /**
     * 不包含
     */
    public EsWrapper notLike(String fieldName, String subFieldName, Object value) {
        return addCondition(fieldName, subFieldName, ConditionConst.NC, value);
    }

    /**
     * 不包含
     */
    public <T> EsWrapper notLike(EFunction<T, ?> column, ESupplier<?> subColumn) {
        return addCondition(column, subColumn, ConditionConst.NC);
    }

    /**
     * 不包含
     */
    public <T, S> EsWrapper notLike(EFunction<T, ?> column, EFunction<S, ?> subColumn, Object value) {
        return addCondition(column, subColumn, ConditionConst.NC, value);
    }

    /**
     * 不以什么结尾
     */
    public EsWrapper notLikeLeft(String fieldName, String subFieldName, Object value) {
        return addCondition(fieldName, subFieldName, ConditionConst.NEL, value);
    }

    /**
     * 不以什么结尾
     */
    public <T> EsWrapper notLikeLeft(EFunction<T, ?> column, ESupplier<?> subColumn) {
        return addCondition(column, subColumn, ConditionConst.NEL);
    }

    /**
     * 不以什么结尾
     */
    public <T, S> EsWrapper notLikeLeft(EFunction<T, ?> column, EFunction<S, ?> subColumn, Object value) {
        return addCondition(column, subColumn, ConditionConst.NEL, value);
    }

    /**
     * 不以什么开头
     */
    public EsWrapper notLikeRight(String fieldName, String subFieldName, Object value) {
        return addCondition(fieldName, subFieldName, ConditionConst.NBL, value);
    }

    /**
     * 不以什么开头
     */
    public <T> EsWrapper notLikeRight(EFunction<T, ?> column, ESupplier<?> subColumn) {
        return addCondition(column, subColumn, ConditionConst.NBL);
    }

    /**
     * 不以什么开头
     */
    public <T, S> EsWrapper notLikeRight(EFunction<T, ?> column, EFunction<S, ?> subColumn, Object value) {
        return addCondition(column, subColumn, ConditionConst.NBL, value);
    }

    /**
     * 在其中
     */
    public EsWrapper in(String fieldName, String subFieldName, Iterable<?> value) {
        return addCondition(fieldName, subFieldName, ConditionConst.IN, value);
    }

    /**
     * 在其中
     */
    public <T, S> EsWrapper in(EFunction<T, ?> column, EFunction<S, ?> subColumn, Iterable<?> value) {
        return addCondition(column, subColumn, ConditionConst.IN, value);
    }

    /**
     * 在其中
     */
    public EsWrapper in(String fieldName, String subFieldName, Object... value) {
        return addCondition(fieldName, subFieldName, ConditionConst.IN, Arrays.asList(value));
    }

    /**
     * 在其中
     */
    public <T, S> EsWrapper in(EFunction<T, ?> column, EFunction<S, ?> subColumn, Object... value) {
        return addCondition(column, subColumn, ConditionConst.IN, Arrays.asList(value));
    }

    /**
     * 不在其中
     */
    public EsWrapper notIn(String fieldName, String subFieldName, Iterable<?> value) {
        return addCondition(fieldName, subFieldName, ConditionConst.NIN, value);
    }

    /**
     * 不在其中
     */
    public <T, S> EsWrapper notIn(EFunction<T, ?> column, EFunction<S, ?> subColumn, Iterable<?> value) {
        return addCondition(column, subColumn, ConditionConst.NIN, value);
    }

    /**
     * 不在其中
     */
    public EsWrapper notIn(String fieldName, String subFieldName, Object... value) {
        return addCondition(fieldName, subFieldName, ConditionConst.NIN, Arrays.asList(value));
    }

    /**
     * 不在其中
     */
    public <T, S> EsWrapper notIn(EFunction<T, ?> column, EFunction<S, ?> subColumn, Object... value) {
        return addCondition(column, subColumn, ConditionConst.NIN, Arrays.asList(value));
    }

    /**
     * 大于
     */
    public EsWrapper gt(String fieldName, String subFieldName, Object value) {
        return addCondition(fieldName, subFieldName, ConditionConst.GT, value);
    }

    /**
     * 大于
     */
    public <T> EsWrapper gt(EFunction<T, ?> column, ESupplier<?> subColumn) {
        return addCondition(column, subColumn, ConditionConst.GT);
    }

    /**
     * 大于
     */
    public <T, S> EsWrapper gt(EFunction<T, ?> column, EFunction<S, ?> subColumn, Object value) {
        return addCondition(column, subColumn, ConditionConst.GT, value);
    }

    /**
     * 大于等于
     */
    public EsWrapper ge(String fieldName, String subFieldName, Object value) {
        return addCondition(fieldName, subFieldName, ConditionConst.GE, value);
    }

    /**
     * 大于等于
     */
    public <T> EsWrapper ge(EFunction<T, ?> column, ESupplier<?> subColumn) {
        return addCondition(column, subColumn, ConditionConst.GE);
    }

    /**
     * 大于等于
     */
    public <T, S> EsWrapper ge(EFunction<T, ?> column, EFunction<S, ?> subColumn, Object value) {
        return addCondition(column, subColumn, ConditionConst.GE, value);
    }

    /**
     * 小于
     */
    public EsWrapper lt(String fieldName, String subFieldName, Object value) {
        return addCondition(fieldName, subFieldName, ConditionConst.LT, value);
    }

    /**
     * 小于
     */
    public <T> EsWrapper lt(EFunction<T, ?> column, ESupplier<?> subColumn) {
        return addCondition(column, subColumn, ConditionConst.LT);
    }

    /**
     * 小于
     */
    public <T, S> EsWrapper lt(EFunction<T, ?> column, EFunction<S, ?> subColumn, Object value) {
        return addCondition(column, subColumn, ConditionConst.LT, value);
    }

    /**
     * 小于等于
     */
    public EsWrapper le(String fieldName, String subFieldName, Object value) {
        return addCondition(fieldName, subFieldName, ConditionConst.LE, value);
    }

    /**
     * 小于等于
     */
    public <T> EsWrapper le(EFunction<T, ?> column, ESupplier<?> subColumn) {
        return addCondition(column, subColumn, ConditionConst.LE);
    }

    /**
     * 小于等于
     */
    public <T, S> EsWrapper le(EFunction<T, ?> column, EFunction<S, ?> subColumn, Object value) {
        return addCondition(column, subColumn, ConditionConst.LE, value);
    }

    /**
     * 在XXX之间
     */
    public EsWrapper between(String fieldName, String subFieldName, Object value1, Object value2) {
        return addCondition(fieldName, subFieldName, ConditionConst.BT, value1, value2);
    }

    /**
     * 在XXX之间
     */
    public <T, S> EsWrapper between(EFunction<T, ?> column, EFunction<S, ?> subColumn, Object value1, Object value2) {
        return addCondition(column, subColumn, ConditionConst.BT, value1, value2);
    }

    /**
     * 不在XXX之间
     */
    public EsWrapper notBetween(String fieldName, String subFieldName, Object value1, Object value2) {
        return addCondition(fieldName, subFieldName, ConditionConst.NBT, value1, value2);
    }

    /**
     * 不在XXX之间
     */
    public <T, S> EsWrapper notBetween(EFunction<T, ?> column, EFunction<S, ?> subColumn, Object value1, Object value2) {
        return addCondition(column, subColumn, ConditionConst.NBT, value1, value2);
    }

    /**
     * 为空
     */
    public EsWrapper isNull(String fieldName, String subFieldName) {
        return addCondition(fieldName, subFieldName, ConditionConst.NU);
    }

    /**
     * 为空
     */
    public <T, S> EsWrapper isNull(EFunction<T, ?> column, EFunction<S, ?> subColumn) {
        return addCondition(column, subColumn, ConditionConst.NU);
    }

    /**
     * 不为空
     */
    public EsWrapper isNotNull(String fieldName, String subFieldName) {
        return addCondition(fieldName, subFieldName, ConditionConst.NN);
    }

    /**
     * 不为空
     */
    public <T, S> EsWrapper isNotNull(EFunction<T, ?> column, EFunction<S, ?> subColumn) {
        return addCondition(column, subColumn, ConditionConst.NN);
    }

    /**
     * 添加条件
     */
    public <T> EsWrapper addCondition(EFunction<T, ?> column, ESupplier<?> subColumn, String keyword) {
        return addCondition(LambdaUtil.getFieldName(column), LambdaUtil.getFieldName(subColumn), keyword, subColumn.get());
    }

    /**
     * 添加条件
     */
    public <T, S> EsWrapper addCondition(EFunction<T, ?> column, EFunction<S, ?> subColumn, String keyword, Object... values) {
        return addCondition(LambdaUtil.getFieldName(column), LambdaUtil.getFieldName(subColumn), keyword, values);
    }

    /**
     * 添加条件
     */
    public EsWrapper addCondition(String fieldName, String subFieldName, String keyword, Object... values) {
        fieldName = fieldName + "." + subFieldName;
        if (nestedWrapper) {
            if (nestedCriteria == null) {
                nestedCriteria = Criteria.where(fieldName);
            }
            EsWrapper wrapper = addCondition(nestedCriteria, keyword, values);
            Criteria.CriteriaEntry criteriaEntry = nestedCriteria.getQueryCriteriaEntries().get(nestedCriteria.getQueryCriteriaEntries().size() - 1);
            criteriaEntry.setField(new SimpleField(fieldName));
            return wrapper;
        }
        return addCondition(fieldName, keyword, values);
    }

}
