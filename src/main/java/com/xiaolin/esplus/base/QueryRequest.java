package com.xiaolin.esplus.base;

import com.xiaolin.esplus.constant.ConditionConst;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Description:
 * @author: jianfeng.zheng
 * @since: 2024/2/26 17:36
 * @history: 1.2024/2/26 created by jianfeng.zheng
 */
public class QueryRequest {
    private List<QueryParam> queryParams;
    private String keywordText;
    private String[] keywordFields;
    private Integer page;
    private Integer limit;
    private Integer offset;

    private List<SortParam> sorts;

    public List<QueryParam> getQueryParams() {
        return queryParams == null ? queryParams = new ArrayList<>() : queryParams;
    }

    public QueryParam getQueryParam(String name) {
        QueryParam result = null;
        for (QueryParam param : this.getQueryParams()) {
            if (name.equals(param.getName())) {
                result = param;
                break;
            }
        }
        return result;
    }

    public String getParamValueAndRemove(String name) {
        String result = null;
        int index = -1;
        List<QueryParam> queryParams = this.getQueryParams();
        for (int i = 0; i < queryParams.size(); i++) {
            QueryParam param = queryParams.get(i);
            if (name.equals(param.getName())) {
                result = param.getValue1();
                index = i;
                break;
            }
        }
        if (index != -1) {
            queryParams.remove(index);
        }
        return result;
    }

    public String getParamsValue(String name) {
        List<QueryParam> params = this.getQueryParams();
        String value = null;
        for (QueryParam param : params) {
            if (name.equals(param.getName())) {
                value = param.getValue1();
                break;
            }
        }
        return value;
    }

    public String removeParameter(String name) {
        String paramsValue = this.getParamsValue(name);
        int index = -1;
        for (int i = 0; i < queryParams.size(); ++i) {
            if (queryParams.get(i).getName().equals(name)) {
                index = i;
                break;
            }
        }
        if (index != -1) {
            queryParams.remove(index);
        }
        return paramsValue;
    }

    public void setQueryParams(List<QueryParam> queryParams) {
        this.queryParams = queryParams;
    }

    public String getKeywordText() {
        return keywordText;
    }

    public void setKeywordText(String keywordText) {
        this.keywordText = keywordText;
    }

    public String[] getKeywordFields() {
        return keywordFields;
    }

    public void setKeywordFields(String[] keywordFields) {
        this.keywordFields = keywordFields;
    }

    public Integer getPage() {
        return page == null ? page = 1 : page;
    }

    public void setPage(Integer page) {
        this.page = page;
    }

    public Integer getLimit() {
        return limit == null ? limit = 10 : limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public List<SortParam> getSorts() {
        return sorts == null ? sorts = new ArrayList<>() : sorts;
    }

    public void setSorts(List<SortParam> sorts) {
        this.sorts = sorts;
    }

    public void addParameter(String name, String value, String op) {
        if (queryParams == null) {
            queryParams = new ArrayList<>();
        }
        QueryParam param = new QueryParam();
        param.setName(name);
        param.setValue1(value);
        param.setOp(op);
        this.queryParams.add(param);
    }

    public void addParameter(String name, String value) {
        this.addParameter(name, value, "eq");
    }

    public void addParameter(String name, String[] value, String op) {
        if (queryParams == null) {
            queryParams = new ArrayList<>(10);
        }
        QueryParam param = new QueryParam();
        param.setName(name);
        param.setValues(Arrays.stream(value).toList());
        param.setOp(op);
        this.queryParams.add(param);
    }

    public void addParameter(String name, List<String> values, String op) {
        if (queryParams == null) {
            queryParams = new ArrayList<>(10);
        }
        QueryParam param = new QueryParam();
        param.setName(name);
        param.setValues(values);
        param.setOp(op);
        this.queryParams.add(param);
    }

    public void addParameter(String name, String[] value) {
        if (queryParams == null) {
            queryParams = new ArrayList<>(10);
        }
        QueryParam param = new QueryParam();
        param.setName(name);
        param.setValues(Arrays.stream(value).toList());
        param.setOp(ConditionConst.IN);
        this.queryParams.add(param);
    }

    public void addParameter(String name, List<String> values) {
        if (queryParams == null) {
            queryParams = new ArrayList<>(10);
        }
        QueryParam param = new QueryParam();
        param.setName(name);
        param.setValues(values);
        param.setOp(ConditionConst.IN);
        this.queryParams.add(param);
    }

    public void addSort(String name, boolean isAsc) {
        if (sorts == null) {
            sorts = new ArrayList<>();
        }
        SortParam param = new SortParam();
        param.setName(name);
        param.setAsc(isAsc);
        this.sorts.add(param);
    }

    public void addSort(String name) {
        addSort(name, true);
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

}
