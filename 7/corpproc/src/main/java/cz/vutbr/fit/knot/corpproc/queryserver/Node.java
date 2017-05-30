package cz.vutbr.fit.knot.corpproc.queryserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Node {

    protected Node right;
    protected Node left;
    protected Constraint constraint;
    protected OPERATOR op;

    public Node() {
        left = null;
        right = null;
        op = null;
        constraint = null;
    }

    public Node(Constraint constraint) {
        this.constraint = constraint;
    }

    public Node(Constraint constraint, OPERATOR op, Node right) {
        left = new Node(constraint);
        this.op = op;
        this.right = right;
    }

    public Node(Node left, OPERATOR op, Node right) {
        this.left = left;
        this.op = op;
        this.right = right;
    }

    public boolean resolve(List<ConstraintHolder> rules) {
        if (constraint != null) {
            List<ConstraintHolder> matchingRules = new ArrayList<>();
            rules.forEach(ch -> {
                constraint.getNumbers().forEach(i -> {
                    if (Objects.equals(ch.getNumber(), i)) {
                        matchingRules.add(ch);
                    }
                });
            });
            return constraint.resolve(matchingRules);
        }
        if (op == OPERATOR.OR) {
            return left.resolve(rules) || right.resolve(rules);
        } else {
            return left.resolve(rules) && right.resolve(rules);
        }
    }
}
