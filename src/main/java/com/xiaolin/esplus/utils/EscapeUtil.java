package com.xiaolin.esplus.utils;

public class EscapeUtil {
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
}
