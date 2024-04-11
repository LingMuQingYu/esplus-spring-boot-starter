package com.xiaolin.esplus.utils;

import co.elastic.clients.elasticsearch._types.FieldValue;

import java.util.ArrayList;
import java.util.List;

public class EsToolsUtil {
    public static String escape(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            // These characters are part of the query syntax and must be escaped
            if (c == '\\' || c == '+' || c == '-' || c == '!' || c == '(' || c == ')' || c == ':' || c == '^' || c == '['
                    || c == ']' || c == '\"' || c == '{' || c == '}' || c == '~' || c == '*' || c == '?' || c == '|' || c == '&'
                    || c == '/') {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * 首字母变小写
     */
    public static String getFirstLowerCaseString(String value) {
        if (value.length() == 1 || value.length() > 1 && !Character.isUpperCase(value.charAt(1))) {
            char[] chars = value.toCharArray();
            if (chars[0] >= 'A' && chars[0] <= 'Z') {
                chars[0] = (char) (chars[0] + 32);
            }
            return new String(chars);
        }
        return value;
    }

    /**
     * 首字母变大写
     */
    public static String getFirstUpperCaseString(String value) {
        if (value.length() == 1 || value.length() > 1 && !Character.isUpperCase(value.charAt(1))) {
            char[] chars = value.toCharArray();
            if (chars[0] >= 'a' && chars[0] <= 'z') {
                chars[0] = (char) (chars[0] - 32);
            }
            return new String(chars);
        }
        return value;
    }

    public static List<FieldValue> toFieldValueList(Iterable<?> iterable) {
        List<FieldValue> list = new ArrayList<>();
        for (Object item : iterable) {
            list.add(item != null ? FieldValue.of(item.toString()) : null);
        }
        return list;
    }

    public static String orQueryString(Iterable<?> iterable) {
        StringBuilder sb = new StringBuilder();
        for (Object item : iterable) {
            if (item != null) {
                if (!sb.isEmpty()) {
                    sb.append(' ');
                }
                sb.append('"');
                sb.append(escape(item.toString()));
                sb.append('"');
            }
        }
        return sb.toString();
    }
}
