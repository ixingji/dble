package com.actiontech.dble.route.parser.direct;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Objects;

@ToString
public class PlainValue extends Value {

    @Getter
    private String value;

    @Getter
    @JSONField(serialize = false)
    private boolean numeric = false;

    @Getter
    @Setter
    private boolean not = false;

    public PlainValue(String value) {
        this.value = value;
    }

    public PlainValue(int value) {
        this(String.valueOf(value), true);
    }

    public PlainValue(String value, boolean numeric) {
        this.value = value;
        this.numeric = numeric;
    }

    @Override
    public Type type() {
        return Type.PLAIN;
    }

    @Override
    public PlainValue copy() {
        return new PlainValue(value, numeric);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PlainValue)) return false;
        PlainValue that = (PlainValue) o;
        return numeric == that.numeric &&
                not == that.not &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, numeric, not);
    }

}
