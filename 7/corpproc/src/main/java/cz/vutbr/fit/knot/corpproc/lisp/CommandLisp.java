/**
 * Class is used in case of 'lisp' job.
 */
package cz.vutbr.fit.knot.corpproc.lisp;

import com.beust.jcommander.Parameters;
import cz.vutbr.fit.knot.corpproc.config.ConfigAndPreformate;

import static cz.vutbr.fit.knot.corpproc.lisp.Lisp.parse;

import java.util.Scanner;

@Parameters(commandDescription = "Run a Lisp REPL (very elementary functionality for now).")
public class CommandLisp {

    public void run(ConfigAndPreformate configAndPreformate) {
        Scanner scanner = new Scanner(System.in);

        Environment env = new Environment();
        Primitives.register(env);

        System.out.println("List Processing Language 0.1");
        while (true) {
            System.out.print("> ");
            String s = scanner.nextLine().trim();
            if (s.equals(":q")) {
                break;
            }

            parse(s).forEach(x -> System.out.println(x.eval(env).toString()));
        }

        System.out.println("Leaving...");
    }
}
