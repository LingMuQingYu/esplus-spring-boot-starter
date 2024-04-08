package com.xiaolin.esplus.base;

import java.util.List;

/**
 * @Description:
 * @author: jianfeng.zheng
 * @since: 2023/3/20 18:41
 * @history: 1.2023/3/20 created by jianfeng.zheng
 */
public class QueryParam {
    private String name;
    private String value1;
    private String value2;
    private List<String> values;
    private String op;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue1() {
        return value1;
    }

    public void setValue1(String value1) {
        this.value1 = value1;
    }

    public String getValue2() {
        return value2;
    }

    public void setValue2(String value2) {
        this.value2 = value2;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }
}
