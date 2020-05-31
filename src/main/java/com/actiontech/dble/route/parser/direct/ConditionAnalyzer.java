package com.actiontech.dble.route.parser.direct;

import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.sqlserver.ast.expr.SQLServerExpr;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConditionAnalyzer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConditionAnalyzer.class);

    @Setter
    private Map<String, String> aliasMap = new HashMap<>();

    @Getter
    private ConditionNode conditionNode = new ConditionNode(true);

    private SQLTableSource tableSource;

    public ConditionAnalyzer(MySqlSelectQueryBlock selectQueryBlock,
                             Map<String, String> aliasMap) {
        this.aliasMap = aliasMap;
        this.tableSource = selectQueryBlock.getFrom();
        analyze(selectQueryBlock);
    }

    public void analyze(MySqlSelectQueryBlock selectQueryBlock) {
        if (!handleSpecialExpr(selectQueryBlock.getWhere(), conditionNode)) {

        }

        if (selectQueryBlock.getFrom() instanceof SQLJoinTableSource) {
            SQLJoinTableSource joinTableSource = (SQLJoinTableSource) (selectQueryBlock.getFrom());
            mergeJoin(conditionNode, joinTableSource);
        }
    }

    private void mergeJoin(ConditionNode node, SQLJoinTableSource joinTableSource) {
        SQLBinaryOpExpr conditionExpr = (SQLBinaryOpExpr) (joinTableSource.getCondition());
        if (conditionExpr != null) {
            mergeJoin(node, conditionExpr);
        }

        if (joinTableSource.getLeft() != null
                && joinTableSource.getLeft() instanceof SQLJoinTableSource) {
            mergeJoin(node, (SQLJoinTableSource) (joinTableSource.getLeft()));
        }

        if (joinTableSource.getRight() != null
                && joinTableSource.getRight() instanceof SQLJoinTableSource) {
            mergeJoin(node, (SQLJoinTableSource) (joinTableSource.getRight()));
        }
    }

    private void mergeJoin(ConditionNode node, SQLBinaryOpExpr conditionExpr) {
        if (conditionExpr == null) {
            return;
        }

        switch (conditionExpr.getOperator()) {
            case BooleanAnd:
                if (!handleSpecialExpr(conditionExpr.getLeft(), node)) {
                    mergeJoin(node, (SQLBinaryOpExpr) (conditionExpr.getLeft()));
                }

                if (!handleSpecialExpr(conditionExpr.getRight(), node)) {
                    mergeJoin(node, (SQLBinaryOpExpr) (conditionExpr.getRight()));
                }
                break;
            case BooleanOr:
                ConditionNode orNode = new ConditionNode(true);
                buildNode(orNode, conditionExpr);
                mergeOr(node, orNode);
                break;
            case Equality:
                List<Value> relation = new ArrayList<>();
                Value leftValue = analyzeSQLExpr(conditionExpr.getLeft());
                Value rightValue = analyzeSQLExpr(conditionExpr.getRight());
                relation.add(leftValue);
                relation.add(rightValue);
                node.addEqRelation(relation);
                break;
            default:
                break;
        }
    }

    private void buildNode(ConditionNode node, SQLBinaryOpExpr conditionExpr) {
        if (conditionExpr == null) {
            return;
        }

        SQLBinaryOperator conditionOperator = conditionExpr.getOperator();
        if (conditionExpr.isBracket()
                && conditionOperator == SQLBinaryOperator.BooleanOr) {
            // 有括号包起来并且是or节点
            node = node.newChild();
        }

        switch (conditionOperator) {
            case BooleanOr:
                ConditionNode leftChild = node.isHead() ? node.getParent().newChild() : node;
                if (!handleSpecialExpr(conditionExpr.getLeft(), leftChild)) {
                    buildNode(leftChild, (SQLBinaryOpExpr) conditionExpr.getLeft());
                }

                ConditionNode rightChild = node.isHead() ? node.getParent().newChild() : node;
                if (!handleSpecialExpr(conditionExpr.getRight(), rightChild)) {
                    buildNode(rightChild, (SQLBinaryOpExpr) conditionExpr.getRight());
                }
                break;
            case BooleanAnd:
                if (!handleSpecialExpr(conditionExpr.getLeft(), node)) {
                    buildNode(node, (SQLBinaryOpExpr) conditionExpr.getLeft());
                }

                if (!handleSpecialExpr(conditionExpr.getRight(), node)) {
                    buildNode(node, (SQLBinaryOpExpr) conditionExpr.getRight());
                }
                break;
            case Equality:
                List<Value> eqRelation = new ArrayList<>(2);
                Value leftEqValue = analyzeSQLExpr(conditionExpr.getLeft());
                Value rightEqValue = analyzeSQLExpr(conditionExpr.getRight());
                eqRelation.add(leftEqValue);
                eqRelation.add(rightEqValue);
                node.addEqRelation(eqRelation);
                break;
            case NotEqual:
                List<Value> notEqRelation = new ArrayList<>(2);
                Value leftNotEqValue = analyzeSQLExpr(conditionExpr.getLeft());
                Value rightNotEqValue = analyzeSQLExpr(conditionExpr.getRight());
                if (leftNotEqValue != null && leftNotEqValue.type() == Value.Type.PLAIN) {
                    ((PlainValue) leftNotEqValue).setNot(true);
                }
                if (rightNotEqValue != null && rightNotEqValue.type() == Value.Type.PLAIN) {
                    ((PlainValue) rightNotEqValue).setNot(true);
                }
                notEqRelation.add(leftNotEqValue);
                notEqRelation.add(rightNotEqValue);
                node.addEqRelation(notEqRelation);
                break;
            default:
                break;
        }

    }

    /**
     * merge or node
     * 放在指定node叶子节点的孩子节点中
     *
     * @param node   目标节点
     * @param orNode or节点
     */
    private void mergeOr(ConditionNode node, ConditionNode orNode) {
        if (node.getChildes().size() > 0) {
            for (ConditionNode child : node.getChildes()) {
                mergeOr(child, orNode);
            }
        } else {
            node.addChild(orNode.copy());
        }
    }

    private boolean handleSpecialExpr(SQLExpr sqlExpr, ConditionNode node) {
        if (sqlExpr instanceof SQLInListExpr) {
            SQLInListExpr sqlInListExpr = (SQLInListExpr) sqlExpr;

            List<Value> relation = new ArrayList<>();
            List<SQLExpr> exprList = sqlInListExpr.getTargetList();

            List<PlainValue> values = new ArrayList<>();
            for (SQLExpr expr : exprList) {
                values.add((PlainValue) analyzeSQLExpr(expr));
            }

            relation.add(analyzeSQLExpr(sqlInListExpr.getExpr()));
            relation.add(new InListValue(values, sqlInListExpr.isNot()));

            node.addEqRelation(relation);
            return true;
        } else if (sqlExpr instanceof SQLBetweenExpr) {
            // do nothing
            return true;
        } else if (sqlExpr instanceof SQLInSubQueryExpr) {
            // do nothing
            return true;
        }
        return false;
    }

    private Value analyzeSQLExpr(SQLExpr sqlExpr) {
        if (sqlExpr instanceof SQLPropertyExpr) {
            SQLPropertyExpr propertyExpr = (SQLPropertyExpr) sqlExpr;
            String ownerName = ((SQLIdentifierExpr) propertyExpr.getOwner()).getLowerName();
            String fieldName = propertyExpr.getName();
            String tableName = aliasMap.get(ownerName);
            if (tableName == null) {
                tableName = aliasMap.get(StringUtil.removeBackQuote(ownerName));
            }
            String value = StringUtil.removeBackQuote(RouterUtil.removeSchema(tableName))
                    + "."
                    + StringUtil.removeBackQuote(fieldName);
            return new PropertyValue(value);
        } else if (sqlExpr instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr identifierExpr = (SQLIdentifierExpr) sqlExpr;
            String tableName = ((SQLExprTableSource) tableSource).getExpr().toString();
            return new PropertyValue(RouterUtil.removeSchema(tableName) + "." + identifierExpr.getName());
        } else if (sqlExpr instanceof SQLIntegerExpr) {
            SQLIntegerExpr integerExpr = (SQLIntegerExpr) sqlExpr;
            int value = integerExpr.getNumber().intValue();
            return new PlainValue(value);
        } else if (sqlExpr instanceof SQLCharExpr) {
            SQLCharExpr charExpr = (SQLCharExpr) sqlExpr;
            String value = charExpr.getText();
            return new PlainValue(value);
        } else if (sqlExpr instanceof SQLQueryExpr) {
            SQLQueryExpr queryExpr = (SQLQueryExpr) sqlExpr;
            MySqlSelectQueryBlock selectQueryBlock = (MySqlSelectQueryBlock) queryExpr.getSubQuery().getQuery();
            return new SubQueryValue(selectQueryBlock);
        }
        return null;
    }

    public static class ConditionNode {

        @Getter
        @Setter
        private MultiKeyEqList<Value> eqRelations = new MultiKeyEqList<>();

        /**
         * 多个子节点 or
         */
        @Getter
        @Setter
        private List<ConditionNode> childes = new ArrayList<>(4);

        @Getter
        @Setter
        @JSONField(serialize = false)
        private ConditionNode parent;

        @Getter
        @Setter
        private boolean head;

        public ConditionNode(boolean head) {
            if (head) {
                this.setParent(this);
            }
            this.head = head;
        }

        public ConditionNode newChild() {
            ConditionNode child = new ConditionNode(false);
            child.setParent(this);
            this.childes.add(child);
            return child;
        }

        public void addChild(ConditionNode child) {
            child.setParent(this);
            this.childes.add(child);
        }

        public boolean canMergeDown() {
            return childes.size() > 0;
        }

        public void mergeDown() {
            if (!canMergeDown()) {
                return;
            }

            for (ConditionNode child : this.childes) {
                MultiKeyEqList<Value> childEqRelations = child.getEqRelations();
                childEqRelations.merge(this.eqRelations);
            }
        }

        public void addEqRelation(List<Value> values) {
            this.eqRelations.add(values);
        }

        public ConditionNode copy() {
            ConditionNode node = new ConditionNode(this.head);

            // copy eqRelations
            MultiKeyEqList<Value> eqRelationsCopy = new MultiKeyEqList<>();

            List<List<Value>> valuesList = eqRelations.getValuesList();
            List<List<Value>> valuesListCopy = new ArrayList<>();

            for (List<Value> values : valuesList) {
                List<Value> valuesCopy = new ArrayList<>();
                for (Value value : values) {
                    valuesCopy.add(value.copy());
                }
                valuesListCopy.add(valuesCopy);
            }

            eqRelationsCopy.addAll(valuesListCopy);

            node.setEqRelations(eqRelationsCopy);

            // copy childes
            List<ConditionNode> childesCopy = new ArrayList<>();
            for (ConditionNode child : childes) {
                ConditionNode childCopy = child.copy();
                childCopy.setParent(node);
                childesCopy.add(childCopy);
            }

            node.setChildes(childesCopy);

            return node;
        }

        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }

    }

    public static class MultiKeyEqList<E> {

        @Getter
        @Setter
        private List<List<E>> valuesList = new ArrayList<>();

        private final Map<E, List<E>> valuesMap = new HashMap<>();

        /**
         * 浅拷贝
         *
         * @param valuesList
         */
        public void addAll(List<List<E>> valuesList) {
            for (List<E> values : valuesList) {
                this.valuesList.addAll(valuesList);
                for (E value : values) {
                    this.valuesMap.put(value, values);
                }
            }
        }

        public void add(List<E> values) {
            if (values.size() != 2) {
                LOGGER.error("only support add two values");
                return;
            }

            E firstValue = values.get(0);
            E secondValue = values.get(1);

            List<E> firstHitValues = this.valuesMap.get(firstValue);
            List<E> secondHitValues = this.valuesMap.get(secondValue);

            if (firstHitValues != null && secondHitValues != null) {
                if (firstValue != secondValue) { // 两次命中了不同的values，需要合并
                    firstHitValues.addAll(secondHitValues);
                    for (E value : secondHitValues) {
                        this.valuesMap.put(value, firstHitValues);
                    }
                    this.valuesList.remove(secondHitValues);

                    firstHitValues.addAll(values);
                } else {
                    firstHitValues.add(firstValue);
                    secondHitValues.add(secondValue);
                }
            } else if (firstHitValues == null && secondHitValues == null) { // 两次都没命中
                List<E> newValues = new ArrayList<>(values);
                this.valuesList.add(newValues);
                for (E value : newValues) {
                    this.valuesMap.put(value, newValues);
                }
            } else { // 只有一次命中
                if (firstHitValues != null) { // 第一次命中
                    firstHitValues.addAll(values);
                    this.valuesMap.put(secondValue, firstHitValues);
                } else { // 第二次命中
                    secondHitValues.addAll(values);
                    this.valuesMap.put(firstValue, secondHitValues);
                }
            }
        }

        public boolean merge(MultiKeyEqList<E> eqList) {
            List<E> tmpValues = new ArrayList<>(2);
            for (List<E> values : eqList.getValuesList()) {
                for (int i = 0; i < values.size(); i += 2) {
                    tmpValues.add(values.get(i));
                    if (i + 1 < values.size()) {
                        tmpValues.add(values.get(i + 1));
                    }
                    add(tmpValues);
                    tmpValues.clear();
                }
            }
            return true;
        }

        public boolean isEqual(List<E> values) {
            if (values.size() < 2) {
                LOGGER.warn("param must more than 2 values");
                return false;
            }
            List<E> valuesCopy = new ArrayList<>(values);

            E firstValue = values.get(0);
            List<E> eqValues = this.valuesMap.get(firstValue);

            int count = 0;
            if (eqValues != null && eqValues.size() > 0) {
                for (E value : eqValues) {
                    if (valuesCopy.size() == 0) {
                        return true;
                    } else if (valuesCopy.remove(value)) {
                        count++;
                    }
                }
            } else {
                return false;
            }
            return values.size() == count;
        }

        public List<E> getValues(E value) {
            return this.valuesMap.get(value);
        }

        public int size() {
            return this.valuesMap.size();
        }

    }

}
