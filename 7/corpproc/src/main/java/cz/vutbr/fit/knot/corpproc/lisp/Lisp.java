package cz.vutbr.fit.knot.corpproc.lisp;

import static cz.vutbr.fit.knot.corpproc.lisp.Primitives.NIL;
import static cz.vutbr.fit.knot.corpproc.lisp.Symbol.makeSymbol;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.codehaus.jparsec.Parser;
import org.codehaus.jparsec.Parser.Reference;
import org.codehaus.jparsec.Parsers;
import org.codehaus.jparsec.Scanners;
import static org.codehaus.jparsec.Scanners.isChar;
import org.codehaus.jparsec.Terminals;
import org.codehaus.jparsec.error.ParserException;
import org.codehaus.jparsec.pattern.CharPredicate;
import org.codehaus.jparsec.pattern.CharPredicates;

public class Lisp {

    private static final Parser<Void> WHITESPACE = Scanners.isChar(CharPredicates.IS_WHITESPACE).skipMany();
    private static final Parser<Void> COMMENT = Scanners.lineComment(";");
    private static final Parser<Void> SKIP = Parsers.or(WHITESPACE, COMMENT);

    private static final Parser<LispString> STRING
            = Terminals.StringLiteral.DOUBLE_QUOTE_TOKENIZER.map(LispString::new);
    
    private static final Parser<LispChar> CHAR
            = Scanners.string("#\\").next(Scanners.ANY_CHAR).source().map(s -> new LispChar(s.charAt(2)));

    private static final CharPredicate SYMBOL_CONSTITUENT = CharPredicates.or(
            CharPredicates.IS_LETTER, CharPredicates.among("0123456789!$%&*+-./:<=>?@[]^_{}~"));
    private static final Parser<Symbol> SYMBOL = Parsers.or(
            Scanners.isChar(SYMBOL_CONSTITUENT).source().map(String::toUpperCase),
            Scanners.isChar(SYMBOL_CONSTITUENT).or(WHITESPACE).many().source().between(isChar('|'), isChar('|')))
            .many1().map(xs -> makeSymbol(String.join("", xs)));

    private static final Reference<LispObject> LIST = Parser.newReference();
    private static final Reference<LispObject> QUOTED = Parser.newReference();

    private static final Parser<LispObject> TERM = Parsers.or(STRING, CHAR, SYMBOL, LIST.lazy(), QUOTED.lazy());

    static {
        LIST.set(TERM.between(SKIP, SKIP).many()
                .map(Cons::fromList)
                .between(isChar('('), isChar(')')));

        QUOTED.set(isChar('\'').next(TERM).between(SKIP, SKIP)
                .map(x -> new Cons(makeSymbol("QUOTE"), new Cons(x, NIL))));
    }

    public static List<LispObject> parse(String input) {
        try {
            return TERM.between(SKIP, SKIP).many().parse(input);
        } catch (ParserException e) {
            return Arrays.asList(new Error(e.getMessage()));
        }
    }

    public static List<LispObject> run(String s) {
        Environment env = new Environment();
        Primitives.register(env);
        return run(s, env);
    }
    
    public static List<LispObject> run(String s, Environment env) {
        return parse(s).stream().map(x -> x.eval(env)).collect(Collectors.toList());
    }
}
