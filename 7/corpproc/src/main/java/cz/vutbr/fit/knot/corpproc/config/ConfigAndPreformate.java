/**
 * Class Holds configuration from the YAML file.
 */

package cz.vutbr.fit.knot.corpproc.config;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import cz.vutbr.fit.knot.corpproc.indexer.CorpprocDocumentFactory;
import cz.vutbr.fit.knot.corpproc.lisp.Environment;
import cz.vutbr.fit.knot.corpproc.lisp.Lisp;
import cz.vutbr.fit.knot.corpproc.lisp.LispObject;
import cz.vutbr.fit.knot.corpproc.lisp.LispString;
import cz.vutbr.fit.knot.corpproc.lisp.Primitives;
import static cz.vutbr.fit.knot.corpproc.lisp.Primitives.NIL;
import static cz.vutbr.fit.knot.corpproc.lisp.Symbol.makeSymbol;
import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.di.big.mg4j.document.ReplicatedDocumentFactory;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jparsec.error.ParserException;

public class ConfigAndPreformate implements Serializable{

    @JsonProperty
    public List<String> fields;

    @JsonProperty
    protected EntityMap entities;

    @JsonProperty
    protected String nertagField;

    @JsonProperty
    private String remapTags;

    @JsonProperty
    private String remapTagsTo;

    @JsonProperty
    private Map<String, Map<String, String>> mapping;

    @JsonProperty
    private String style;

    @JsonProperty
    private Map<String, String> dye;

    @JsonProperty
    private String field;

    @JsonProperty
    private List<String> nonDuplicateFields;

    private Map<String, String> modifMapping;




    public void processMap()
    {
        Map<String, String> newMap = new HashMap<>();

        mapping.forEach((key, value) -> {

            mapping.get(key).forEach((k, v) -> {

                newMap.put(key + "." + k, v);

            });

        });

        this.modifMapping = newMap;
    }

    public Map<String, Map<String, String>> getMapping(){
        return this.mapping;
    }



    public String getRemapTags() {
        return remapTags;
    }

    public String getRemapTagsTo() {
        return remapTagsTo;
    }

    public Map<String, String> getModifMapping() {
        return modifMapping;
    }

    public String getStyle() {
        return style;
    }

    public void setStyle(String style) {
        this.style = style;
    }

    public Map<String, String> getDye() {
        return dye;
    }


    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }



    public List<String> getNonDuplicateFields() {
        return nonDuplicateFields;
    }


    protected static class EntityMap implements Serializable {

        protected Map<String, ImmutableMap<String, LispObject>> entityMap = new HashMap<>();

        @JsonAnySetter
        public void setDynamicProperty(String name, Map<String, String> map) {
            ImmutableMap.Builder<String, LispObject> builder = ImmutableMap.builder();

            for (Entry<String, String> entry : map.entrySet()) {
                List<LispObject> values;
                try {
                    values = Lisp.parse(entry.getValue());
                } catch (ParserException e) {
                    throw new IllegalArgumentException(
                            "Parse error in field '" + name + "/" + entry.getKey()
                            + "': " + e.getMessage());
                }
                if (values.size() != 1) {
                    throw new IllegalArgumentException(
                            "Field '" + name + "/" + entry.getKey()
                            + "' must contain exactly one expression.");
                }
                builder.put(entry.getKey(), values.get(0));
            }
            entityMap.put(name, builder.build());
        }

        @Override
        public String toString() {
            return "EntityMap(entities: " + entityMap.toString() + ")";
        }
    }

    public List<String> getFields() {
        return fields;
    }

    public DocumentFactory getFactory() {
        return ReplicatedDocumentFactory.getFactory(new CorpprocDocumentFactory(), fields.size(), fields.toArray(new String[fields.size()]));
    }

    public List<String> getEntityProps() {
        List<String> x = new ArrayList<>();
        entities.entityMap.forEach((entity, props) -> props.keySet().forEach(k -> x.add(k.replace("data-", ""))));
        return x;
    }

    /**
     * Create map with attributes for non entity.
     * @param actuals
     * @param format
     * @return
     */
    public Map<String, String> readNonEntiry(Map<String, String> actuals, Map<String, Object> format){
        Map<String, String> props = new HashMap<>();
        if (format != null
                && format.size() > 0
                && format.get("attributes") != null
                && ((List<String>)format.get("attributes")).size() > 0){
            actuals.forEach((key, value) -> {
                if (((List<String >)format.get("attributes")).contains(key) || ((List<String >)format.get("attributes")).contains("all")){
                    props.put("data-" + key, value);
                }
            });
        }
        return props;
    }

    /**
     * Create map with attributes for entity.
     * @param content
     * @param entity
     * @param format
     * @return
     */
    public Map<String, String> readEntity(String content, Map<String, String> entity, Map<String, Object> format) {
        Map<String, String> props = new HashMap<>();

        Environment env = new Environment();
        Primitives.register(env);
        env.bindValue(makeSymbol("CONTENT"), new LispString(content));
        fields.forEach(field -> {
            String v = entity.getOrDefault(field, "").replace("¶", "").replace("§", "");
            LispObject obj = v.isEmpty() || "0".equals(v) ? NIL : new LispString(v);
            env.bindValue(makeSymbol(field.toUpperCase()), obj);
        });

        BiConsumer<String, LispObject> process = (k, v) -> {
            String s = v.eval(env).asString(env);
            if (!s.isEmpty()) {

                if (((HashMap) format.get("nertags")).size() > 0
                        && format.get("nertags") != null
                        && ((HashMap) format.get("nertags")).size() > 0){

                    Pattern dataPattern = Pattern.compile("data-");
                    Matcher dataMatcher = dataPattern.matcher(k);
                    String keyWithoutDataPref = dataMatcher.replaceAll("");

                    if (((HashMap) format.get("nertags")).keySet().contains("all")){
                        props.put(k, s);
                    }else {
                        if (((HashMap) format.get("nertags")).keySet().contains(entity.get("nertag"))){
                            if ( ((ArrayList)((HashMap)format.get("nertags")).get(entity.get("nertag"))).contains("all")
                                    || ((ArrayList)((HashMap)format.get("nertags")).get(entity.get("nertag"))).contains(keyWithoutDataPref))
                                props.put(k, s);
                        }
                    }
                }


            }

        };


        Map<String, ImmutableMap<String, LispObject>> entityMap = entities.entityMap;

        if (entityMap.containsKey("common") && format != null && format.size() != 0 && format.get("nertags") != null) {
            entityMap.get("common").forEach(process);
        }

        if (entityMap.containsKey("entity") && format != null && format.size() != 0 && format.get("nertags") != null) {
            entityMap.get("entity").forEach(process);
        }
        if (entity.containsKey(nertagField) && entityMap.containsKey(entity.get(nertagField))
                && format != null
                && format.size() != 0
                && format.get("nertags") != null && ((HashMap) format.get("nertags")).size() > 0) {
            entityMap.get(entity.get(nertagField)).forEach(process);
        }

        return props;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("fields: " + fields.toString() + "\n");
        builder.append("entities:\n");
        entities.entityMap.forEach((entity, props) -> {
            builder.append("    ").append(entity).append("\n");
            props.forEach((k, v) -> {
                builder.append("        ").append(k).append(": ").append(v).append("\n");
            });
        });
        return builder.toString();
    }

    public static ConfigAndPreformate read(String input) throws IOException {
        return new YAMLMapper().readValue(input, ConfigAndPreformate.class);
    }

    public static ConfigAndPreformate readFile(String filename) throws IOException {
        return read(Files.toString(new File(filename), Charset.defaultCharset()));
    }
}
