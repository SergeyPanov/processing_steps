/**
 * Main class. This class selecting a job which will be ran based on input parameters.
 */

package cz.vutbr.fit.knot.corpproc.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import cz.vutbr.fit.knot.corpproc.config.ConfigAndPreformate;
import cz.vutbr.fit.knot.corpproc.config.ConfigHolder;
import cz.vutbr.fit.knot.corpproc.indexer.CommandIndex;
import cz.vutbr.fit.knot.corpproc.lisp.CommandLisp;
import cz.vutbr.fit.knot.corpproc.queryserver.CommandServe;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Cli {

    public static void main(String[] args) {
        CommandMain main = new CommandMain();
        JCommander jc = new JCommander(main);

        CommandIndex index = new CommandIndex();
        CommandRepl repl = new CommandRepl();
        CommandServe serve = new CommandServe();
        CommandLisp lisp = new CommandLisp();
        CommandUpdateCollection update = new CommandUpdateCollection();
        jc.addCommand("index", index);
        jc.addCommand("repl", repl);
        jc.addCommand("serve", serve);
        jc.addCommand("lisp", lisp);
        jc.addCommand("update-collection", update);


        try {
            jc.parse(args);
            if (main.help || (jc.getParsedCommand() == null && main.configFile == null && !main.checkConfig && !main.initConfig)) {
                jc.usage();
                System.exit(0);
            }
            if (main.initConfig) {
                InputStream in = Cli.class.getClassLoader().getResourceAsStream("config.yaml");
                Path out = Paths.get(main.configFile != null ? main.configFile : "./config.yaml");
                try {
                    Files.copy(in, out);
                } catch (IOException e) {
                    throw new IOException("Couldn't write configAndPreformate file '" + out.normalize() + "': " + e.getMessage());
                }
                System.out.println("Wrote configAndPreformate file '" + out.normalize() + "'");
                System.exit(0);
            }

            if (main.configFile == null) {
                if (!Files.exists(Paths.get("./config.yaml"))) {
                    throw new IOException("Couldn't find a configAndPreformate file in the current"
                            + " directory. Either specify one with --configAndPreformate CONFIG-FILE,"
                            + " or create one in the current directory with --init-configAndPreformate.");
                }

                main.configFile = "./config.yaml";
            }
            if (!Files.exists(Paths.get(main.configFile))) {
                throw new IOException("Couldn't find the specified configAndPreformate file. Please verify if it exists.");
            }
            ConfigAndPreformate configAndPreformate;
            try {
                configAndPreformate = ConfigAndPreformate.readFile(main.configFile);
            } catch (IOException e) {
                throw new IOException("Couldn't read configAndPreformate file: " + e.getMessage());
            }

            if (main.checkConfig) {
                System.out.println(configAndPreformate);
                System.exit(0);
            }


            configAndPreformate.processMap();
            ConfigHolder.setConfiguration(configAndPreformate);  //Keeps origin configuration. Cannot be changed
            ConfigHolder.setClonedConfiguration(Clonner.clone(configAndPreformate));  //Keeps working copy of configuration. Can be changed


            switch (jc.getParsedCommand()) {
                case "index":
                    index.run(configAndPreformate);
                    break;
                case "repl":
                    repl.run(configAndPreformate);
                    break;
                case "serve":
                    serve.run(configAndPreformate);
                    break;
                case "lisp":
                    lisp.run(configAndPreformate);
                    break;
                case "update-collection":
                    update.run(configAndPreformate);
                    break;
            }
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            jc.usage();
            System.exit(1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
}
