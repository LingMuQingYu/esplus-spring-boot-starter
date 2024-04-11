package com.xiaolin.esplus.utils;

import java.io.Serializable;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandleProxies;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Locale;

public class LambdaUtil {
    public static String getFieldName(Serializable column) {
        if (column instanceof Proxy) {
            MethodHandle dmh = MethodHandleProxies.wrapperInstanceTarget(column);
            Executable executable = MethodHandles.reflectAs(Executable.class, dmh);
            return methodToProperty(executable.getName());
        } else {
            try {
                Method method = column.getClass().getDeclaredMethod("writeReplace");
                method.setAccessible(true);
                return methodToProperty(((SerializedLambda) method.invoke(column)).getImplMethodName());
            } catch (Throwable ignored) {
            }
        }
        return null;
    }

    public static String methodToProperty(String name) {
        if (name.startsWith("is")) {
            name = name.substring(2);
        } else {
            if (!name.startsWith("get") && !name.startsWith("set")) {
                throw new RuntimeException("Error parsing property name '" + name + "'.  Didn't start with 'is', 'get' or 'set'.");
            }
            name = name.substring(3);
        }
        return getFirstUpperCaseString(name);
    }

    /**
     * 首字母变大写
     * @param value
     * @return
     */
    public static String getFirstUpperCaseString(String value){

        if (value.length() == 1 || value.length() > 1 && !Character.isUpperCase(value.charAt(1))) {
            char[] chars = value.toCharArray();
            if (chars[0] >= 'a' && chars[0] <= 'z') {
                chars[0] = (char) (chars[0] - 32);
            }
            return new String(chars);
        }
        return null;
    }

}
