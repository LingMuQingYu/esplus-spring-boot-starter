package com.xiaolin.esplus.service;

import com.xiaolin.esplus.base.QueryRequest;
import com.xiaolin.esplus.constant.ConditionConst;
import com.xiaolin.esplus.wrapper.EsWrapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.SearchScrollHits;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class EsService<T> {
    protected Logger log = LoggerFactory.getLogger(getClass());
    private final long scrollTimeInMillis = 300000;
    @Autowired
    protected ElasticsearchTemplate esTemplate;
    protected ElasticsearchConverter elasticsearchConverter;
    protected final Class<T> entityClass = getTypeClass();

    /**
     * 保存数据
     */
    public void save(T entity) {
        esTemplate.save(entity);
    }

    /**
     * 批量保存
     */
    public void saveBatch(Collection<T> entities) {
        esTemplate.save(entities);
    }

    /**
     * 更新数据
     *
     * @param entity 实体
     */
    public void updateById(T entity) {
        esTemplate.update(entity);
    }

    /**
     * 更新数据
     */
    public void updateBatchById(Collection<T> entities) {
        entities.forEach(this::updateById);
    }

    /**
     * 条件更新
     *
     * @param wrapper 条件包装器
     */
    public void update(EsWrapper wrapper) {
        Document document;
        if (Objects.nonNull(wrapper.getEntity())) {
            document = this.getElasticsearchConverter().mapObject(wrapper.getEntity());
            esTemplate.update(UpdateQuery.builder(wrapper.build()).withDocument(document).build());
        } else if (!wrapper.getUpdateMap().isEmpty()) {
            document = this.getElasticsearchConverter().mapObject(wrapper.getUpdateMap());
            IndexCoordinates indexCoordinatesFor = esTemplate.getIndexCoordinatesFor(this.entityClass);
            esTemplate.update(UpdateQuery.builder(wrapper.build()).withDocument(document).build(), indexCoordinatesFor);
        } else {
            throw new RuntimeException("Update content or entity cannot be empty！");
        }
    }

    /**
     * 条件更新（指定实体）
     *
     * @param wrapper 条件包装器
     */
    public void update(T entity, EsWrapper wrapper) {
        esTemplate.update(
                UpdateQuery.builder(wrapper.build())
                        .withDocument(this.getElasticsearchConverter().mapObject(entity))
                        .build()
        );

    }

    /**
     * 根据ID删除
     */
    public void removeById(Serializable id) {
        esTemplate.delete(String.valueOf(id), this.entityClass);
    }

    /**
     * 根据ID集合删除
     */
    public void removeByIds(Collection<? extends Serializable> ids) {
        esTemplate.delete(esTemplate.idsQuery(ids.stream().map(String::valueOf).toList()), this.entityClass);
    }

    /**
     * 根据ID删除
     */
    public void removeById(T entity) {
        esTemplate.delete(entity);
    }

    /**
     * 条件删除
     *
     * @param wrapper 条件包装器
     */
    public void remove(EsWrapper wrapper) {
        esTemplate.delete(wrapper.build(), this.entityClass);
    }

    /**
     * 根据ID获取单条数据
     */
    public T get(Serializable id) {
        return esTemplate.get(String.valueOf(id), this.entityClass);
    }

    /**
     * 条件获取单条数据
     */
    public T get(EsWrapper wrapper) {
        SearchHit<T> searchHit = esTemplate.searchOne(wrapper.build(), this.entityClass);
        if (Objects.isNull(searchHit)) {
            return null;
        }
        return searchHit.getContent();
    }

    /**
     * 根据ID集合获取多条数据
     */
    public List<T> list(Collection<? extends Serializable> ids) {
        SearchHits<T> hits = esTemplate.search(esTemplate.idsQuery(ids.stream().map(String::valueOf).toList()), this.entityClass);
        return hits.stream().map(SearchHit::getContent).toList();
    }

    /**
     * 根据条件获取多条数据
     */
    public List<T> list(EsWrapper wrapper) {
        SearchHits<T> hits = esTemplate.search(wrapper.build(), this.entityClass);
        return hits.stream().map(SearchHit::getContent).toList();
    }


    /**
     * 根据条件分页获取多条数据（不返回总数）
     */
    public List<T> list(Number pageNum, Number pageSize, EsWrapper wrapper) {
        return page(pageNum, pageSize, wrapper).getSearchHits().stream().map(SearchHit::getContent).toList();
    }

    /**
     * 分页查询
     */
    public SearchHits<T> page(Number pageNum, Number pageSize) {
        return page(pageNum, pageSize, EsWrapper.builder());
    }

    /**
     * 条件分页查询
     */
    public SearchHits<T> page(Number pageNum, Number pageSize, EsWrapper wrapper) {
        return esTemplate.search(setPageable(pageNum, pageSize, wrapper), this.entityClass);
    }

    /**
     * ES滚动分页查询
     */
    public SearchScrollHits<T> pageScrollStart(Number pageSize, EsWrapper wrapper) {
        return pageScrollStart(pageSize, wrapper, scrollTimeInMillis);
    }

    /**
     * ES滚动分页查询
     */
    public SearchScrollHits<T> pageScrollStart(Number pageSize, EsWrapper wrapper, long scrollTimeInMillis) {
        Query query = setPageable(1, pageSize, wrapper);
        IndexCoordinates indexCoordinatesFor = esTemplate.getIndexCoordinatesFor(this.entityClass);
        return esTemplate.searchScrollStart(scrollTimeInMillis, query, this.entityClass, indexCoordinatesFor);
    }

    /**
     * ES滚动分页查询
     */
    public SearchScrollHits<T> pageScrollContinue(String scrollId) {
        return pageScrollContinue(scrollId, scrollTimeInMillis);
    }

    /**
     * ES滚动分页查询
     */
    public SearchScrollHits<T> pageScrollContinue(String scrollId, long scrollTimeInMillis) {
        IndexCoordinates indexCoordinatesFor = esTemplate.getIndexCoordinatesFor(this.entityClass);
        return esTemplate.searchScrollContinue(scrollId, scrollTimeInMillis, this.entityClass, indexCoordinatesFor);
    }

    /**
     * 清理ES滚动分页查询
     */
    public void pageScrollClear(String scrollId) {
        esTemplate.searchScrollClear(scrollId);
    }

    /**
     * 通用分页查询
     */
    public SearchHits<T> pageQuery(QueryRequest queryRequest) {
        return page(queryRequest.getPage(), queryRequest.getLimit(), buildEsWrapper(queryRequest));
    }

    /**
     * 通用分页查询构建通用查询构造器
     */
    public EsWrapper buildEsWrapper(QueryRequest queryRequest) {
        EsWrapper wrapper = EsWrapper.builder();
        if (queryRequest.getKeywordFields() != null && !StringUtils.isEmpty(queryRequest.getKeywordText())) {
            wrapper.or(w -> Arrays.asList(queryRequest.getKeywordFields()).forEach(e -> w.mq(e, queryRequest.getKeywordText())));
        }
        queryRequest.getQueryParams().forEach(q -> {
            if (ConditionConst.IN.equals(q.getOp()) || ConditionConst.NIN.equals(q.getOp())) {
                wrapper.addCondition(q.getName(), q.getOp(), q.getValues());
            } else {
                wrapper.addCondition(q.getName(), q.getOp(), q.getValue1(), q.getValue2());
            }
        });
        queryRequest.getSorts().forEach(e -> wrapper.orderBy(e.getName(), e.isAsc()));
        return wrapper;
    }

    private Query setPageable(Number pageNum, Number pageSize, EsWrapper wrapper) {
        return wrapper.build().setPageable(PageRequest.of(pageNum.intValue(), pageSize.intValue()));
    }

    private ElasticsearchConverter getElasticsearchConverter() {
        if (this.elasticsearchConverter == null) {
            this.elasticsearchConverter = this.esTemplate.getElasticsearchConverter();
        }
        return this.elasticsearchConverter;
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
