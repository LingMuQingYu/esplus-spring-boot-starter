package com.xiaolin.esplus.core;

import com.xiaolin.esplus.constant.ConditionConst;
import com.xiaolin.esplus.core.es.EsQueryCondition;
import com.xiaolin.esplus.core.pojo.EsTestEntity;
import com.xiaolin.esplus.wrapper.EsWrapper;
import org.junit.jupiter.api.Test;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;

import java.lang.reflect.InvocationTargetException;

import static org.junit.jupiter.api.Assertions.*;

class EsQueryConditionFactoryTest {

    @Test
    void getEqQueryCondition(){
        EsQueryCondition beanQueryCondition = EsQueryConditionFactory.getBeanQueryCondition(ConditionConst.EQ);
        beanQueryCondition.execute(null,null,null,null,"");
        System.out.println(beanQueryCondition);
    }
    @Test
    void getInQueryCondition(){
        EsQueryCondition beanQueryCondition = EsQueryConditionFactory.getBeanQueryCondition(ConditionConst.IN);
        beanQueryCondition.execute(null,null,null,null,"");
        System.out.println(beanQueryCondition);
    }

    @Test
    void getBetweenQueryCondition(){
        EsQueryCondition beanQueryCondition = EsQueryConditionFactory.getBeanQueryCondition(ConditionConst.BT);
        beanQueryCondition.execute(null,null,null,null,"");
        System.out.println(beanQueryCondition);
    }

    @Test
    void testEsWrapper() {
        EsWrapper esWrapper = EsWrapper.builder().eq(EsTestEntity::getName, "你好")
                .or(o -> o.eq(EsTestEntity::getPath, "home").ge(EsTestEntity::getNum, 1));
        NativeQuery build = esWrapper.build();
        System.out.println(build.getQuery());
    }
}