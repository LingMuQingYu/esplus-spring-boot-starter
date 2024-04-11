package com.xiaolin.esplus.core;

import com.xiaolin.esplus.core.es.DefaultQueryConditionStrategy;
import com.xiaolin.esplus.core.es.EsQueryCondition;
import com.xiaolin.esplus.utils.EsToolsUtil;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

public class EsQueryConditionFactory  {


    public static final String conditionPkg = "com.xiaolin.esplus.core.es";
    public static final String conditionSuffix = "QueryConditionStrategy";

    public static EsQueryCondition getBeanQueryCondition(String conditionKeyword)  {
        conditionKeyword = EsToolsUtil.getFirstUpperCaseString(conditionKeyword);
        String conditionPkgPath = conditionPkg + "." + conditionKeyword + conditionSuffix;
        try {
            Class<?> clazz = Class.forName(conditionPkgPath);
            Method getInstanceMethod = clazz.getMethod("getInstance");
            Object queryCondition = getInstanceMethod.invoke(null);
            if (queryCondition instanceof EsQueryCondition) {
                return (EsQueryCondition)queryCondition;
            }
        } catch (Exception e) {
            LoggerFactory.getLogger(EsQueryConditionFactory.class).info("LoadClass: '{}' Exception,Default LoadClass DefaultQueryConditionStrategy",conditionPkgPath);
        }
        //读取不到对应的EsQueryCondition则走默认处理
        return DefaultQueryConditionStrategy.getInstance();
    }

}
