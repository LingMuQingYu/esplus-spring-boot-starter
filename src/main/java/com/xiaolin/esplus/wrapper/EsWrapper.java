package com.xiaolin.esplus.wrapper;

import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.aggregations.AggregationBuilders;
import com.xiaolin.esplus.base.EFunction;
import com.xiaolin.esplus.base.ESupplier;
import com.xiaolin.esplus.base.EsAggregation;
import com.xiaolin.esplus.constant.ConditionConst;
import com.xiaolin.esplus.utils.LambdaUtil;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.client.elc.NativeQueryBuilder;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;
import org.springframework.data.elasticsearch.core.query.CriteriaQueryBuilder;

import java.util.*;
import java.util.function.Consumer;

public class EsWrapper {
    private final String AND = "AND";
    private final String OR = "OR";

    /**
     * 当前层级条件集
     */
    List<Criteria> criteriaList = new ArrayList<>();

    private final Map<String, Aggregation> aggregations = new LinkedHashMap<>();

    private String condition = AND;

    private final List<String> selectFields = new ArrayList<>();

    private final List<Sort> sorts = new ArrayList<>();

    private Object updateEntity;

    private final Map<String, Object> updateMap = new LinkedHashMap<>();

    public static EsWrapper builder() {
        return new EsWrapper();
    }

    /**
     * 指定当前层级条件为OR，默认为AND
     */
    public EsWrapper or() {
        this.condition = OR;
        return this;
    }

    /**
     * 嵌套条件，当前层级条件不变，子条件为OR
     */
    public EsWrapper or(Consumer<EsWrapper> consumer) {
        EsWrapper esWrapper = builder().or();
        consumer.accept(esWrapper);
        criteriaList.add(esWrapper.getCriteria());
        return this;
    }

    /**
     * 嵌套条件，当前层级条件不变，子条件为AND
     */
    public EsWrapper and(Consumer<EsWrapper> consumer) {
        EsWrapper esWrapper = builder();
        consumer.accept(esWrapper);
        criteriaList.add(esWrapper.getCriteria());
        return this;
    }

    /**
     * 匹配
     */
    public EsWrapper eq(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.EQ, value);
    }

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
    public EsWrapper ne(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.NE, value);
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
    public EsWrapper mq(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.MQ, value);
    }

    /**
     * 分词匹配
     */
    public EsWrapper mq(ESupplier<?> column) {
        return addCondition(column, ConditionConst.MQ);
    }

    /**
     * 分词匹配
     */
    public <T> EsWrapper mq(EFunction<T, ?> column, Object value) {
        return addCondition(column, ConditionConst.MQ, value);
    }

    /**
     * 分词不匹配
     */
    public EsWrapper notMq(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.NM, value);
    }

    /**
     * 分词不匹配
     */
    public EsWrapper notMq(ESupplier<?> column) {
        return addCondition(column, ConditionConst.NM);
    }

    /**
     * 分词不匹配
     */
    public <T> EsWrapper notMq(EFunction<T, ?> column, Object value) {
        return addCondition(column, ConditionConst.NM, value);
    }

    /**
     * 短语查询
     */
    public EsWrapper like(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.LK, value);
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
    public EsWrapper likeLeft(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.LLK, value);
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
    public EsWrapper likeRight(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.RLK, value);
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
    public <T> EsWrapper notLike(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.NC, value);
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
    public EsWrapper notLikeLeft(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.NEL, value);
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
    public EsWrapper notLikeRight(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.NBL, value);
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
    public EsWrapper in(String fieldName, Iterable<?> value) {
        return addCondition(fieldName, ConditionConst.IN, value);
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
    public EsWrapper in(String fieldName, Object... value) {
        return addCondition(fieldName, ConditionConst.IN, Arrays.asList(value));
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
    public EsWrapper notIn(String fieldName, Iterable<?> value) {
        return addCondition(fieldName, ConditionConst.NIN, value);
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
    public EsWrapper notIn(String fieldName, Object... value) {
        return addCondition(fieldName, ConditionConst.NIN, Arrays.asList(value));
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
    public EsWrapper gt(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.GT, value);
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
    public EsWrapper ge(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.GE, value);
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
    public EsWrapper lt(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.LT, value);
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
    public EsWrapper le(String fieldName, Object value) {
        return addCondition(fieldName, ConditionConst.LE, value);
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
    public EsWrapper between(String fieldName, Object value1, Object value2) {
        return addCondition(fieldName, ConditionConst.BT, value1, value2);
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
    public EsWrapper notBetween(String fieldName, Object value1, Object value2) {
        return addCondition(fieldName, ConditionConst.NBT, value1, value2);
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
    public EsWrapper isNull(String fieldName) {
        return addCondition(fieldName, ConditionConst.NU);
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
    public EsWrapper isNotNull(String fieldName) {
        return addCondition(fieldName, ConditionConst.NN);
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
     * 添加条件
     */
    public EsWrapper addCondition(String fieldName, String keyword, Object... values) {
        if (values == null || values.length == 0) {
            return this;
        }
        String[] fieldArray;
        Object value = values[0];
        switch (keyword) {
            case ConditionConst.EQ:
                criteriaList.add(Criteria.where(fieldName).is(value));
                break;
            case ConditionConst.NE:
                criteriaList.add(Criteria.where(fieldName).not().is(value));
                break;
            case ConditionConst.LK:
                criteriaList.add(Criteria.where(fieldName).contains(String.valueOf(value)));
                break;
            case ConditionConst.LLK:
                criteriaList.add(Criteria.where(fieldName).endsWith(String.valueOf(value)));
                break;
            case ConditionConst.RLK:
                criteriaList.add(Criteria.where(fieldName).startsWith(String.valueOf(value)));
                break;
            case ConditionConst.NC:
                criteriaList.add(Criteria.where(fieldName).not().contains(String.valueOf(value)));
                break;
            case ConditionConst.NEL:
                criteriaList.add(Criteria.where(fieldName).not().endsWith(String.valueOf(value)));
                break;
            case ConditionConst.NBL:
                criteriaList.add(Criteria.where(fieldName).not().startsWith(String.valueOf(value)));
                break;
            case ConditionConst.IN:
                if (value instanceof Iterable<?>) {
                    criteriaList.add(Criteria.where(fieldName).in((Iterable<?>) value));
                }
                break;
            case ConditionConst.NIN:
                if (value instanceof Iterable<?>) {
                    criteriaList.add(Criteria.where(fieldName).not().in((Iterable<?>) value));
                }
                break;
            case ConditionConst.GT:
                criteriaList.add(Criteria.where(fieldName).greaterThan(getNumberValue(value)));
                break;
            case ConditionConst.GE:
                criteriaList.add(Criteria.where(fieldName).greaterThanEqual(getNumberValue(value)));
                break;
            case ConditionConst.LT:
                criteriaList.add(Criteria.where(fieldName).lessThan(getNumberValue(value)));
                break;
            case ConditionConst.LE:
                criteriaList.add(Criteria.where(fieldName).lessThanEqual(getNumberValue(value)));
                break;
            case ConditionConst.BT:
                if (values.length == 2) {
                    criteriaList.add(Criteria.where(fieldName).between(getNumberValue(values[0]), getNumberValue(values[1])));
                }
                break;
            case ConditionConst.NBT:
                if (values.length == 2) {
                    criteriaList.add(Criteria.where(fieldName).not().between(getNumberValue(values[0]), getNumberValue(values[1])));
                }
                break;
            case ConditionConst.NU:
                criteriaList.add(Criteria.where(fieldName).exists());
                break;
            case ConditionConst.NN:
                criteriaList.add(Criteria.where(fieldName).not().exists());
                break;
            case ConditionConst.MQ:
                criteriaList.add(Criteria.where(fieldName).matches(value));
                break;
            case ConditionConst.NM:
                criteriaList.add(Criteria.where(fieldName).not().matches(value));
                break;
            case ConditionConst.NESTED_EQ:
                fieldArray = fieldName.split("\\.");
                if (fieldArray.length == 2) {
                    criteriaList.add(Criteria.where(fieldArray[0]).subCriteria(Criteria.where(fieldArray[1]).is(value)));
                }
                break;
            case ConditionConst.NESTED_IN:
                fieldArray = fieldName.split("\\.");
                if (fieldArray.length == 2) {
                    Criteria firstFieldCriteria = Criteria.where(fieldArray[0]);
                    if (value instanceof Iterable<?>) {
                        firstFieldCriteria.subCriteria(Criteria.where(fieldArray[1]).in((Iterable<?>) value));
                    }
                    criteriaList.add(firstFieldCriteria);
                }
                break;
            default:
                break;
        }
        return this;
    }

    /**
     * 排序
     */
    public EsWrapper orderBy(Sort sortParam) {
        sorts.add(sortParam);
        return this;
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
    public EsWrapper orderBy(String name, boolean asc) {
        Sort sort = Sort.by(name);
        if (asc) {
            sort.ascending();
        } else {
            sort.descending();
        }
        return orderBy(sort);
    }

    /**
     * 排序
     */
    public <T> EsWrapper orderBy(EFunction<T, ?> column, boolean asc) {
        return orderBy(LambdaUtil.getFieldName(column), asc);
    }

    /**
     * 指定更新对象
     */
    public EsWrapper setEntity(Object entity) {
        updateEntity = entity;
        return this;
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
        return this;
    }

    /**
     * 更新指定字段值
     */
    public EsWrapper set(ESupplier<?> column) {
        updateMap.put(LambdaUtil.getFieldName(column), column.get());
        return this;
    }

    /**
     * 更新指定字段值
     */
    public <T> EsWrapper set(EFunction<T, ?> column, Object value) {
        updateMap.put(LambdaUtil.getFieldName(column), value);
        return this;
    }

    /**
     * 查询指定字段
     */
    public EsWrapper select(String... fieldNames) {
        this.selectFields.addAll(Arrays.asList(fieldNames));
        return this;
    }

    /**
     * 查询指定字段
     */
    public <T> EsWrapper select(EFunction<T, ?>... column) {
        this.selectFields.addAll(Arrays.stream(column).map(LambdaUtil::getFieldName).toList());
        return this;
    }

    /**
     * 查询指定字段
     */
    public EsWrapper select(Collection<String> fieldNames) {
        selectFields.addAll(fieldNames);
        return this;
    }

    public Object getNumberValue(Object value) {
        if (value instanceof Date date) {
            return date.getTime();
        }
        return value;
    }

    /**
     * 获取当前层级Spring Data条件构造器
     */
    public Criteria getCriteria() {
        Criteria criteria = null;
        for (Criteria v : criteriaList) {
            if (criteria == null) {
                criteria = v;
            } else {
                criteria.and(v);
            }
        }
        if (OR.equals(condition)) {
            return criteria.or(criteria);
        }
        if (criteria == null)
            criteria = new Criteria();
        return criteria;
    }

    /**
     * 构建Spring Data条件构造器
     */
    public CriteriaQuery build() {
        return build(queryBuilder -> {
        });
    }

    public EsWrapper group(String name, String fieldName) {
        aggregations.put(name, AggregationBuilders.terms(t -> t.field(fieldName)));
        return this;
    }

    public <T> EsWrapper group(String name, EFunction<T, ?> column) {
        aggregations.put(name, AggregationBuilders.terms(t -> t.field(LambdaUtil.getFieldName(column))));
        return this;
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
        return this;
    }

    public NativeQuery nativeBuild() {
        NativeQueryBuilder nativeQueryBuilder = new NativeQueryBuilder();
        aggregations.forEach(nativeQueryBuilder::withAggregation);
        NativeQuery nativeQuery = nativeQueryBuilder.build();
        CriteriaQuery build = this.build();
        // 只做一条数据的查询
        build.setPageable(PageRequest.of(0, 1));
        nativeQuery.setSpringDataQuery(this.build());
        return nativeQuery;
    }

    public Map<String, Aggregation> getAggregations() {
        return aggregations;
    }

    /**
     * 构建Spring Data条件构造器
     */
    public CriteriaQuery build(Consumer<CriteriaQueryBuilder> consumer) {
        Criteria criteria = getCriteria();
        CriteriaQueryBuilder queryBuilder = CriteriaQuery.builder(criteria);
        consumer.accept(queryBuilder);
        CriteriaQuery criteriaQuery = queryBuilder.build();
        sorts.forEach(criteriaQuery::addSort);
        criteriaQuery.addFields(selectFields.toArray(new String[0]));
        return criteriaQuery;
    }

}
