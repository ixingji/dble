package com.actiontech.dble.route.parser.direct;

import lombok.Getter;
import lombok.ToString;

import java.util.Objects;

@ToString
public class PropertyValue extends Value {

    @Getter
    private String value;

    public PropertyValue(String value) {
        this.value = value.toLowerCase();
    }

    @Override
    public Type type() {
        return Type.PROPERTY;
    }

    @Override
    public Value copy() {
        return new PropertyValue(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PropertyValue)) return false;
        PropertyValue that = (PropertyValue) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

}
