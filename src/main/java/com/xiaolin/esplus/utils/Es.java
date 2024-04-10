package com.xiaolin.esplus.utils;

import com.xiaolin.esplus.base.QueryRequest;
import com.xiaolin.esplus.constant.ConditionConst;
import com.xiaolin.esplus.wrapper.EsWrapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ScriptType;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.SearchScrollHits;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class Es {
    private static final long scrollTimeInMillis = 300000;
    private static ElasticsearchTemplate esTemplate;
    private static ElasticsearchConverter elasticsearchConverter;

    /**
     * 保存数据
     */
    public static <T> void save(T entity) {
        getEsTemplate().save(entity);
    }

    /**
     * 批量保存
     */
    public static <T> void saveBatch(Collection<T> entities) {
        getEsTemplate().save(entities);
    }

    /**
     * 更新数据
     *
     * @param entity 实体
     */
    public static <T> void updateById(T entity) {
        getEsTemplate().update(entity);
    }

    /**
     * 更新数据
     */
    public static <T> void updateBatchById(Collection<T> entities) {
        entities.forEach(Es::updateById);
    }

    /**
     * 条件更新
     *
     * @param wrapper 条件包装器
     */
    public static <T> void update(EsWrapper wrapper, Class<T> clazz) {
        Document document;
        if (Objects.nonNull(wrapper.getEntity())) {
            document = Es.getElasticsearchConverter().mapObject(wrapper.getEntity());
        } else if (!wrapper.getUpdateMap().isEmpty()) {
            document = Es.getElasticsearchConverter().mapObject(wrapper.getUpdateMap());
        } else {
            throw new RuntimeException("Update content or entity cannot be empty！");
        }
        IndexCoordinates indexCoordinatesFor = getEsTemplate().getIndexCoordinatesFor(clazz);
        UpdateQuery build = UpdateQuery.builder(wrapper.build())
                .withScript(document.keySet().stream().map(k -> "ctx._source.%s = params.%s".formatted(k, k)).collect(Collectors.joining(";")))
                .withParams(document)
                .withScriptType(ScriptType.INLINE)
                .build();
        getEsTemplate().updateByQuery(build, indexCoordinatesFor);
    }

    /**
     * 条件更新（指定实体）
     *
     * @param wrapper 条件包装器
     */
    public static <T> void update(T entity, EsWrapper wrapper) {
        getEsTemplate().update(
                UpdateQuery.builder(wrapper.build())
                        .withDocument(Es.getElasticsearchConverter().mapObject(entity))
                        .build()
        );

    }

    /**
     * 根据ID删除
     */
    public static <T> void removeById(Serializable id, Class<T> clazz) {
        getEsTemplate().delete(String.valueOf(id), clazz);
    }

    /**
     * 根据ID集合删除
     */
    public static <T> void removeByIds(Collection<? extends Serializable> ids, Class<T> clazz) {
        getEsTemplate().delete(getEsTemplate().idsQuery(ids.stream().map(String::valueOf).toList()), clazz);
    }

    /**
     * 根据ID删除
     */
    public static <T> void removeById(T entity) {
        getEsTemplate().delete(entity);
    }

    /**
     * 条件删除
     *
     * @param wrapper 条件包装器
     */
    public static <T> void remove(EsWrapper wrapper, Class<T> clazz) {
        getEsTemplate().delete(wrapper.build(), clazz);
    }

    /**
     * 根据ID获取单条数据
     */
    public static <T> T get(Serializable id, Class<T> clazz) {
        return getEsTemplate().get(String.valueOf(id), clazz);
    }

    /**
     * 条件获取单条数据
     */
    public static <T> T get(EsWrapper wrapper, Class<T> clazz) {
        SearchHit<T> searchHit = getEsTemplate().searchOne(wrapper.build(), clazz);
        if (Objects.isNull(searchHit)) {
            return null;
        }
        return searchHit.getContent();
    }

    /**
     * 根据ID集合获取多条数据
     */
    public static <T> List<T> list(Collection<? extends Serializable> ids, Class<T> clazz) {
        SearchHits<T> hits = getEsTemplate().search(getEsTemplate().idsQuery(ids.stream().map(String::valueOf).toList()), clazz);
        return hits.stream().map(SearchHit::getContent).toList();
    }

    /**
     * 根据条件获取多条数据
     */
    public static <T> List<T> list(EsWrapper wrapper, Class<T> clazz) {
        SearchHits<T> hits = getEsTemplate().search(wrapper.build(), clazz);
        return hits.stream().map(SearchHit::getContent).toList();
    }


    /**
     * 根据条件分页获取多条数据（不返回总数）
     */
    public static <T> List<T> list(Number pageNum, Number pageSize, EsWrapper wrapper, Class<T> clazz) {
        return page(pageNum, pageSize, wrapper, clazz).getSearchHits().stream().map(SearchHit::getContent).toList();
    }

    /**
     * 分页查询
     */
    public static <T> SearchHits<T> page(Number pageNum, Number pageSize, Class<T> clazz) {
        return page(pageNum, pageSize, EsWrapper.builder(), clazz);
    }

    /**
     * 条件分页查询
     */
    public static <T> SearchHits<T> page(Number pageNum, Number pageSize, EsWrapper wrapper, Class<T> clazz) {
        return getEsTemplate().search(setPageable(pageNum, pageSize, wrapper), clazz);
    }

    /**
     * ES滚动分页查询
     */
    public static <T> SearchScrollHits<T> pageScrollStart(Number pageSize, EsWrapper wrapper, Class<T> clazz) {
        return pageScrollStart(pageSize, wrapper, clazz, scrollTimeInMillis);
    }

    /**
     * ES滚动分页查询
     */
    public static <T> SearchScrollHits<T> pageScrollStart(Number pageSize, EsWrapper wrapper, Class<T> clazz, long scrollTimeInMillis) {
        Query query = setPageable(1, pageSize, wrapper);
        IndexCoordinates indexCoordinatesFor = getEsTemplate().getIndexCoordinatesFor(clazz);
        return getEsTemplate().searchScrollStart(scrollTimeInMillis, query, clazz, indexCoordinatesFor);
    }

    /**
     * ES滚动分页查询
     */
    public static <T> SearchScrollHits<T> pageScrollContinue(String scrollId, Class<T> clazz) {
        return pageScrollContinue(scrollId, clazz, scrollTimeInMillis);
    }

    /**
     * ES滚动分页查询
     */
    public static <T> SearchScrollHits<T> pageScrollContinue(String scrollId, Class<T> clazz, long scrollTimeInMillis) {
        IndexCoordinates indexCoordinatesFor = getEsTemplate().getIndexCoordinatesFor(clazz);
        return getEsTemplate().searchScrollContinue(scrollId, scrollTimeInMillis, clazz, indexCoordinatesFor);
    }

    /**
     * 清理ES滚动分页查询
     */
    public static void pageScrollClear(String scrollId) {
        getEsTemplate().searchScrollClear(scrollId);
    }

    /**
     * 通用分页查询
     */
    public static <T> SearchHits<T> pageQuery(QueryRequest queryRequest, Class<T> clazz) {
        return page(queryRequest.getPage(), queryRequest.getLimit(), buildEsWrapper(queryRequest), clazz);
    }

    public static <T> List<Map<String, Object>> groupCountList(EsWrapper wrapper, Class<T> clazz) {
        SearchHits<T> hits = esTemplate.search(wrapper.nativeBuild(), clazz);
        return EsGroupUtil.groupCountList(hits, wrapper, clazz);
    }

    /**
     * 通用分页查询构建通用查询构造器
     */
    public static EsWrapper buildEsWrapper(QueryRequest queryRequest) {
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

    private static Query setPageable(Number pageNum, Number pageSize, EsWrapper wrapper) {
        return wrapper.build().setPageable(PageRequest.of(pageNum.intValue() - 1, pageSize.intValue()));
    }

    private static ElasticsearchConverter getElasticsearchConverter() {
        if (Es.elasticsearchConverter == null) {
            Es.elasticsearchConverter = Es.getEsTemplate().getElasticsearchConverter();
        }
        return Es.elasticsearchConverter;
    }

    private static ElasticsearchTemplate getEsTemplate() {
        if (Es.esTemplate == null) {
            Es.esTemplate = SpringUtil.getBean(ElasticsearchTemplate.class);
        }
        return Es.esTemplate;
    }

}
