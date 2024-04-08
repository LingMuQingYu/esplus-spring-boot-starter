package com.xiaolin.esplus.base;

import java.io.Serializable;
import java.util.function.Supplier;

@FunctionalInterface
public interface ESupplier<T> extends Supplier<T>, Serializable {
}