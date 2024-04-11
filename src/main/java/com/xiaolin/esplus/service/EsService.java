package com.xiaolin.esplus.service;

import com.xiaolin.esplus.base.QueryRequest;
import com.xiaolin.esplus.utils.Es;
import com.xiaolin.esplus.wrapper.EsWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.SearchScrollHits;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class EsService<T> {
    protected Logger log = LoggerFactory.getLogger(getClass());
    protected final Class<T> entityClass = getTypeClass();

    /**
     * 保存数据
     */
    public void save(T entity) {
        Es.save(entity);
    }

    /**
     * 批量保存
     */
    public void saveBatch(Collection<T> entities) {
        Es.save(entities);
    }

    /**
     * 更新数据
     *
     * @param entity 实体
     */
    public void updateById(T entity) {
        Es.updateById(entity);
    }

    /**
     * 更新数据
     */
    public void updateBatchById(Collection<T> entities) {
        Es.updateBatchById(entities);
    }

    /**
     * 条件更新
     *
     * @param wrapper 条件包装器
     */
    public void update(EsWrapper wrapper) {
        Es.update(wrapper, this.entityClass);
    }

    /**
     * 条件更新（指定实体）
     *
     * @param wrapper 条件包装器
     */
    public void update(T entity, EsWrapper wrapper) {
        Es.update(entity, wrapper);
    }

    /**
     * 根据ID删除
     */
    public void removeById(Serializable id) {
        Es.removeById(String.valueOf(id), this.entityClass);
    }

    /**
     * 根据ID集合删除
     */
    public void removeByIds(Collection<? extends Serializable> ids) {
        Es.removeByIds(ids, this.entityClass);
    }

    /**
     * 根据ID删除
     */
    public void removeById(T entity) {
        Es.removeById(entity);
    }

    /**
     * 条件删除
     *
     * @param wrapper 条件包装器
     */
    public void remove(EsWrapper wrapper) {
        Es.remove(wrapper, this.entityClass);
    }

    /**
     * 根据ID获取单条数据
     */
    public T get(Serializable id) {
        return Es.get(id, this.entityClass);
    }

    /**
     * 条件获取单条数据
     */
    public T get(EsWrapper wrapper) {
        return Es.get(wrapper, this.entityClass);
    }

    /**
     * 根据ID集合获取多条数据
     */
    public List<T> list(Collection<? extends Serializable> ids) {
        return Es.list(ids, this.entityClass);
    }

    /**
     * 根据条件获取多条数据
     */
    public List<T> list(EsWrapper wrapper) {
        return Es.list(wrapper, this.entityClass);
    }


    /**
     * 根据条件分页获取多条数据（不返回总数）
     */
    public List<T> list(Number pageNum, Number pageSize, EsWrapper wrapper) {
        return Es.list(pageNum, pageSize, wrapper, this.entityClass);
    }

    /**
     * 分页查询
     */
    public SearchHits<T> page(Number pageNum, Number pageSize) {
        return Es.page(pageNum, pageSize, this.entityClass);
    }

    /**
     * 条件分页查询
     */
    public SearchHits<T> page(Number pageNum, Number pageSize, EsWrapper wrapper) {
        return Es.page(pageNum, pageSize, wrapper, this.entityClass);
    }

    /**
     * ES滚动分页查询
     */
    public SearchScrollHits<T> pageScrollStart(Number pageSize, EsWrapper wrapper) {
        return Es.pageScrollStart(pageSize, wrapper, this.entityClass);
    }

    /**
     * ES滚动分页查询
     */
    public SearchScrollHits<T> pageScrollStart(Number pageSize, EsWrapper wrapper, long scrollTimeInMillis) {
        return Es.pageScrollStart(pageSize, wrapper, this.entityClass, scrollTimeInMillis);
    }

    /**
     * ES滚动分页查询
     */
    public SearchScrollHits<T> pageScrollContinue(String scrollId) {
        return Es.pageScrollContinue(scrollId, this.entityClass);
    }

    /**
     * ES滚动分页查询
     */
    public SearchScrollHits<T> pageScrollContinue(String scrollId, long scrollTimeInMillis) {
        return Es.pageScrollContinue(scrollId, this.entityClass, scrollTimeInMillis);
    }

    /**
     * 清理ES滚动分页查询
     */
    public void pageScrollClear(String scrollId) {
        Es.pageScrollClear(scrollId);
    }

    /**
     * 通用分页查询
     */
    public SearchHits<T> pageQuery(QueryRequest queryRequest) {
        return Es.pageQuery(queryRequest, this.entityClass);
    }

    public List<Map<String, Object>> groupCountList(EsWrapper wrapper) {
        return Es.groupCountList(wrapper, this.entityClass);
    }

    @SuppressWarnings("unchecked")
    public Class<T> getTypeClass() {
        Type type = getClass().getGenericSuperclass();
        if (!(type instanceof ParameterizedType parameterizedType)) {
            throw new IllegalStateException("Type must be a parameterized type");
        }
        // 获取泛型的具体类型  这里是单泛型
        Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
        if (null == actualTypeArguments || actualTypeArguments.length < 1) {
            throw new IllegalStateException("Number of type arguments must be 1");
        }
        return (Class<T>) actualTypeArguments[0];
    }

}
