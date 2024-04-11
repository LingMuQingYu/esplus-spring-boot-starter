package com.xiaolin.esplus.core;

import com.xiaolin.esplus.constant.ConditionConst;
import com.xiaolin.esplus.core.es.EsQueryCondition;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;

import static org.junit.jupiter.api.Assertions.*;

class EsQueryConditionFactoryTest {

    @Test
    void getEqQueryCondition(){
        EsQueryConditionFactory esQueryConditionFactory = new EsQueryConditionFactory();
        EsQueryCondition beanQueryCondition = esQueryConditionFactory.getBeanQueryCondition(ConditionConst.EQ);
        beanQueryCondition.execute(null,null,null,null,"");
        System.out.println(beanQueryCondition);
    }
    @Test
    void getInQueryCondition(){
        EsQueryConditionFactory esQueryConditionFactory = new EsQueryConditionFactory();
        EsQueryCondition beanQueryCondition = esQueryConditionFactory.getBeanQueryCondition(ConditionConst.IN);
        beanQueryCondition.execute(null,null,null,null,"");
        System.out.println(beanQueryCondition);
    }

    @Test
    void getBetweenQueryCondition(){
        EsQueryConditionFactory esQueryConditionFactory = new EsQueryConditionFactory();
        EsQueryCondition beanQueryCondition = esQueryConditionFactory.getBeanQueryCondition(ConditionConst.BT);
        beanQueryCondition.execute(null,null,null,null,"");
        System.out.println(beanQueryCondition);
    }
}