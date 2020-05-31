package com.actiontech.dble.route.parser.direct;

public abstract class Value {

    public enum Type {
        PROPERTY,
        PLAIN,
        INLIST,
        BETWEEN,
        SUBQUERY;
    }

    public abstract Type type();

    public abstract Value copy();

}
