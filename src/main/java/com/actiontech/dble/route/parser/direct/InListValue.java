package com.actiontech.dble.route.parser.direct;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

public class InListValue extends Value {

    @Getter
    private List<PlainValue> values;

    @Getter
    private boolean not;

    public InListValue(List<PlainValue> values) {
        this.values = values;
    }

    public InListValue(List<PlainValue> values, boolean not) {
        this.values = values;
        this.not = not;
    }

    @Override
    public Type type() {
        return Type.INLIST;
    }

    @Override
    public Value copy() {
        List<PlainValue> valuesCopy = new ArrayList<>();
        for (PlainValue value : values) {
            valuesCopy.add(value.copy());
        }
        return new InListValue(valuesCopy, not);
    }

}
