package cz.vutbr.fit.knot.corpproc.queryserver;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.codehaus.jparsec.Parser;
import org.codehaus.jparsec.Parser.Reference;
import static org.codehaus.jparsec.Parsers.or;
import static org.codehaus.jparsec.Parsers.sequence;
import org.codehaus.jparsec.Scanners;
import static org.codehaus.jparsec.Scanners.isChar;
import static org.codehaus.jparsec.Scanners.string;
import org.codehaus.jparsec.functors.Pair;

public class ConstraintParser {

    private static final Parser<Integer> NUMBER
            = sequence(Scanners.among("123456789"), Scanners.among("0123456789").many())
            .source()
            .map(s -> Integer.valueOf(s, 10));

    private static final Parser<String> WORD = Scanners.notAmong("=><.!():&| ").many1().source();

    private static final Parser<Void> SKIP = Scanners.among(" \n\r").skipMany();

    private static final Reference<String> INDEX = Parser.newReference();
    private static final Parser<Pair<Integer, String>> REFERENCE = sequence(
            NUMBER, SKIP.next(isChar('.')).next(SKIP), INDEX.lazy(),
            (a, b, c) -> new Pair<Integer, String>(a, c));

    private static final Parser<OPERATION> REL_OP = or(
            isChar('=').retn(OPERATION.EQUAL),
            string("!=").retn(OPERATION.NOT_EQUAL),
            isChar('>').retn(OPERATION.GREATER_THAN),
            string(">=").retn(OPERATION.GREATER_OR_EQUAL),
            isChar('<').retn(OPERATION.LESS_THAN),
            string("<=").retn(OPERATION.LESS_OR_EQUAL));

    private static final Parser<OPERATOR> BOOL_OP = or(
            or(isChar('&'), Scanners.stringCaseInsensitive("and")).retn(OPERATOR.AND),
            or(isChar('|'), Scanners.stringCaseInsensitive("or")).retn(OPERATOR.OR));

    private static final Parser<Constraint> CONSTRAINT = or(
            sequence(SKIP.next(REFERENCE), SKIP.next(REL_OP), SKIP.next(REFERENCE),
                    (a, op, b) -> new Constraint(a.a, a.b, op, new Constraint(b.a, b.b, OPERATION.NOP, ""))),
            sequence(SKIP.next(REFERENCE), SKIP.next(REL_OP), SKIP.next(WORD),
                    (a, op, b) -> new Constraint(a.a, a.b, op, b)));

    private static final Reference<Node> NODE = Parser.newReference();

    private static final Parser<Node> TERM = or(
            NODE.lazy().between(isChar('(').next(SKIP), SKIP.next(isChar(')'))),
            CONSTRAINT.map(Node::new));

    static {
        NODE.set(sequence(TERM, SKIP.next(BOOL_OP).followedBy(SKIP), TERM, Node::new).or(TERM));
    }

    public static Node parse(List<String> indices, String constraints) {
        INDEX.set(or(indices.stream().map(x -> string(x)).collect(Collectors.toList())).source());
        return NODE.get().parse(constraints);
    }
}
