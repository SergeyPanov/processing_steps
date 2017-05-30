package cz.vutbr.fit.knot.corpproc.cli;

import com.beust.jcommander.Parameter;

public class CommandMain {
    
    @Parameter(names = {"-c", "--config"}, description = "YAML config file")
    String configFile = null;
    
    @Parameter(names = {"-h", "--help"}, description = "Show help")
    boolean help;
    
    @Parameter(names = {"-i", "--init-config"}, description = "Create an initial config file in the current directory.")
    boolean initConfig;
    
    @Parameter(names = {"-n", "--check-config"}, description = "Only parse and print out the config.")
    boolean checkConfig;
}
