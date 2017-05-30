package cz.vutbr.fit.knot.corpproc.queryserver;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class Constraint {

    private String index;
    private Integer num;
    private OPERATION op;
    private String value;
    private Constraint rvalue;

    public Constraint(Integer num, String index, OPERATION op, String value) {
        this.index = index;
        this.num = num;
        this.value = value;
        this.op = op;
        this.rvalue = null;
    }

    public Constraint(Integer num, String index, OPERATION op, Constraint rvalue) {
        this.index = index;
        this.num = num;
        this.value = null;
        this.op = op;
        this.rvalue = rvalue;
    }

    public List<Integer> getNumbers() {
        return rvalue == null
                ? Arrays.asList(new Integer[]{num})
                : Arrays.asList(new Integer[]{num, rvalue.getNum()});
    }

    public String getIndex() {
        return index;
    }

    public Integer getNum() {
        return num;
    }

    public boolean resolve(List<ConstraintHolder> matchingRules) {
        if (rvalue == null) {
            for (ConstraintHolder holder : matchingRules) {
                if (op == OPERATION.NOT_EQUAL) {
                    return holder.getMatches().stream().noneMatch(match -> equal(match.get(index), value));
                } else if (holder.getMatches().stream().anyMatch(match -> compare(match.get(index), value))) {
                    return true;
                }
            }
            return false;
        }

        for (ConstraintHolder leftHolder : matchingRules) {
            for (ConstraintHolder rightHolder : matchingRules) {
                if (!Objects.equals(leftHolder.getNumber(), num) || !Objects.equals(rightHolder.getNumber(), rvalue.getNum())) {
                    continue;
                }

                if (op != OPERATION.NOT_EQUAL) {
                    for (HashMap<String, String> leftMatch : leftHolder.getMatches()) {
                        for (HashMap<String, String> rightMatch : rightHolder.getMatches()) {
                            //don't compare with itself
                            if (leftMatch.get("pos").equals(rightMatch.get("pos"))) {
                                continue;
                            }
                            if (compare(leftMatch.get(index), rightMatch.get(rvalue.getIndex()))) {
                                return true;
                            }
                        }
                    }
                } else {
                    //no rule can match
                    for (HashMap<String, String> leftMatch : leftHolder.getMatches()) {
                        for (HashMap<String, String> rightMatch : rightHolder.getMatches()) {
                            //don't compare with itself
                            if (leftMatch.get("pos").equals(rightMatch.get("pos"))) {
                                continue;
                            }
                            if (equal(leftMatch.get(index), rightMatch.get(rvalue.getIndex()))) {
                                return false;
                            }
                        }
                    }
                    return true;
                }
            }
        }
        return false;
    }

    private boolean equal(String left, String right) {
        if (left == null && right == null) {
            return true;
        } else if (left == null || right == null) {
            return false;
        }
        Stream<String> lefts = Arrays.stream(left.split("\\|"));
        Stream<String> rights = Arrays.stream(right.split("\\|"));
        return lefts.anyMatch(lv -> rights.anyMatch(rv -> lv.equals(rv)));
    }

    private boolean compare(String leftValue, String rightValue) {
        if (op == OPERATION.EQUAL) {
            return equal(leftValue, rightValue);
        }
        if (op == OPERATION.NOT_EQUAL) {
            return !equal(leftValue, rightValue);
        }

        int leftInt, rightInt;
        try {
            leftInt = Integer.parseInt(leftValue);
            rightInt = Integer.parseInt(rightValue);
        } catch (NumberFormatException nfe) {
            return false;
        }
        switch (op) {
            case LESS_THAN:
                return leftInt < rightInt;
            case LESS_OR_EQUAL:
                return leftInt <= rightInt;
            case GREATER_THAN:
                return leftInt > rightInt;
            case GREATER_OR_EQUAL:
                return leftInt >= rightInt;
            default:
                break;
        }
        return false;
    }
}
