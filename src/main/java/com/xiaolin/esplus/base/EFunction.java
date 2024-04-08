package com.xiaolin.esplus.base;

import java.io.Serializable;
import java.util.function.Function;

@FunctionalInterface
public interface EFunction<T, R> extends Function<T, R>, Serializable {
}