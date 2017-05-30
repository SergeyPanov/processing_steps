package cz.vutbr.fit.knot.corpproc.config;


/**
 * Created by Sergey on 10/5/2016.
 * This class is singleton
 */
public abstract class ConfigHolder {

    private static ConfigAndPreformate configuration;

    private static ConfigAndPreformate clonedConfiguration;


    public static ConfigAndPreformate getConfiguration() {
        return configuration;
    }

    public static void setConfiguration(ConfigAndPreformate configAndPreformate) {
        configuration = configAndPreformate;
    }


    public static ConfigAndPreformate getClonedConfiguration() {
        return clonedConfiguration;
    }

    public static void setClonedConfiguration(ConfigAndPreformate clonedConfiguration) {
        ConfigHolder.clonedConfiguration = clonedConfiguration;
    }
}