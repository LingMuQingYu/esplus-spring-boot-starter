package com.xiaolin.esplus.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class SpringUtil {

    private static ApplicationContext context;

    @Autowired
    public SpringUtil(ApplicationContext context) {
        SpringUtil.context = context;
    }

    public static <T> T getBean(Class<T> beanClass) {
        return context.getBean(beanClass);
    }
}
