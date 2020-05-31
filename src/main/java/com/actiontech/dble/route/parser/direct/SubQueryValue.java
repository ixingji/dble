package com.actiontech.dble.route.parser.direct;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;

public class SubQueryValue extends Value {

    @Getter
    @JSONField(serialize = false)
    private MySqlSelectQueryBlock value;

    @Getter
    private String valueText;

    public SubQueryValue(MySqlSelectQueryBlock value) {
        this.value = value;
        this.valueText = value.toString();
    }

    @Override
    public Type type() {
        return Type.SUBQUERY;
    }

    @Override
    public Value copy() {
        return new SubQueryValue(value);
    }

}
