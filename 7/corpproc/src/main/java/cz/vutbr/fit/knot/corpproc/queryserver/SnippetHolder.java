/**
 * Holds snippets and process them.
 */
package cz.vutbr.fit.knot.corpproc.queryserver;

import cz.vutbr.fit.knot.corpproc.config.ConfigAndPreformate;
import cz.vutbr.fit.knot.corpproc.config.ConfigHolder;
import cz.vutbr.fit.knot.corpproc.indexer.TokenIterator;
import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentCollection;
import it.unimi.di.big.mg4j.query.MarkingMutableString;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.TextMarker;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SnippetHolder {

    public final static TextMarker EMPTY = new TextMarker("", "", "<block>", "</block>", "", "");

    private final Pattern pattern = Pattern.compile("<block>(.*?)</block>");

    private final ConfigAndPreformate configAndPreformate;

    private Map<String, Object> format;  //Map holds nertags and information about each nertag the client is interesting in.

    List<List<List<Map<String, String>>>> snippetsHodler;


    /**
     * Prepare 'snippetsHolder' variable for future work. This variable holds fields in 'special' structure.
     * @param snippets
     */
    private void initSnippetsHolder(List<String[]> snippets){
        for (int snippetIndex = 0; snippetIndex < snippets.size(); ++snippetIndex){

            List<List<Map<String, String>>> parts = new ArrayList<List<Map<String, String>>>();

            for (int partIndex = 0; partIndex < snippets.get(snippetIndex).length; ++partIndex){
                List<Map<String, String>> field = new ArrayList<>();
                field.add(new HashMap<String, String>());
                parts.add(field);
            }

            this.snippetsHodler.add(parts);
        }
    }

    /**
     * Merge parts of multi-word entity into one part
     */
    private void mergeParts(){
        int nerLength = -1;
        for (int snippetIndex = 0; snippetIndex < this.snippetsHodler.size(); ++snippetIndex){
            for (int partIndex = this.snippetsHodler.get(snippetIndex).size() - 1; partIndex > 0 ; --partIndex){

                if (nerLength <= 0 ){
                    try{
                        if (!this.snippetsHodler.get(snippetIndex).get(partIndex).get(0).get("nertag").equals("0") &&
                                this.snippetsHodler.get(snippetIndex).get(partIndex).get(0).get("nerlength").length() > 0 &&
                                StringUtils.isNumeric(this.snippetsHodler.get(snippetIndex).get(partIndex).get(0).get("nerlength"))){
                            nerLength = Integer.parseInt(this.snippetsHodler.get(snippetIndex).get(partIndex).get(0).get("nerlength")) - 1;

                        }else{
                            nerLength = 0;
                        }
                    }catch (Exception e){
                        nerLength = 0;
                    }

                }

                if (nerLength > 0){
                    this.snippetsHodler.get(snippetIndex).get(partIndex - 1).get(0).put("nertag", this.snippetsHodler.get(snippetIndex).get(partIndex).get(this.snippetsHodler.get(snippetIndex).get(partIndex).size() - 1).get("nertag"));
                    this.snippetsHodler.get(snippetIndex).get(partIndex - 1).addAll(this.snippetsHodler.get(snippetIndex).get(partIndex));
                    this.snippetsHodler.get(snippetIndex).remove(partIndex);
                    --nerLength;
                }

            }
        }
    }

    /**
     * Constructor used in case of getting list of snippets.
     * @param configAndPreformate
     * @param intervals
     * @param collection
     * @param document
     * @param format
     * @throws IOException
     */
    public SnippetHolder(ConfigAndPreformate configAndPreformate, SelectedInterval[] intervals, DocumentCollection collection, Document document, Map<String, Object> format) throws IOException {
        this.configAndPreformate = configAndPreformate;
        snippetsHodler = new ArrayList<>();

        this.format = format;

        for (String index : configAndPreformate.getFields()) {
            int fieldIndex = collection.factory().fieldIndex(index);
            Reader reader = ((TokenIterator) document.content(fieldIndex)).getContent();

            /**
             * Reading all tokens from the column defined by index.
             */
            MarkingMutableString s = new MarkingMutableString(EMPTY);
            s.startField(intervals);
            s.appendAndMark(document.wordReader(fieldIndex).setReader(reader));
            s.endField();

            List<String[]> snippets = new ArrayList<>();

            Matcher m = pattern.matcher(s.toString());
            while (m.find()) {
                String[] tokens = m.group(1).trim().split(" ");
                snippets.add(tokens);
            }
            if (this.snippetsHodler.size() <= 0){
                this.initSnippetsHolder(snippets);
            }
            for(int snippetIndex= 0; snippetIndex < snippets.size(); ++snippetIndex){
                for(int partIndex = 0; partIndex < snippets.get(snippetIndex).length; ++partIndex){
                    String val = "";
                    Pattern ampPtrn = Pattern.compile("[¶§]");
                    Matcher mtr = ampPtrn.matcher(snippets.get(snippetIndex)[partIndex]);
                    val = mtr.replaceAll("");

                    if (index.equals("token")){
                        Pattern ptrn = Pattern.compile("\\|G\\S*");
                        Matcher mtch = ptrn.matcher(val);
                        val = mtch.replaceAll("");
                    }
                    try{
                        this.snippetsHodler.get(snippetIndex).get(partIndex).get(0).put(index, val);
                    }catch (Exception e){
                        List<String[]> aux = new ArrayList<>();
                        aux.add(snippets.get(snippetIndex));
                        this.initSnippetsHolder(aux);
                        this.snippetsHodler.get(snippetIndex).get(partIndex).get(0).put(index, val);
                    }
                }
            }
        }
        mergeParts();
    }

    /**
     * Constructor is used in case of getting whole document
     * @param configAndPreformate
     * @param collection
     * @param document
     * @param format
     * @throws IOException
     */

    public SnippetHolder(ConfigAndPreformate configAndPreformate, DocumentCollection collection, Document document, Map<String, Object> format) throws IOException {
        this.configAndPreformate = configAndPreformate;
        this.format = format;

        snippetsHodler = new ArrayList<>();

        for (String index : configAndPreformate.getFields()) {
            int fieldIndex = collection.factory().fieldIndex(index);
            Reader reader = ((TokenIterator) document.content(fieldIndex)).getContent();

            List<String[]> snippets = new ArrayList<>();
            for (String paragraph : new Scanner(reader).useDelimiter("\\Z").next().split("§")) {
                String[] tokens = paragraph.split(" ");
                snippets.add(tokens);
            }
            if (this.snippetsHodler.size() <= 0){
                this.initSnippetsHolder(snippets);
            }

            for(int snippetIndex= 0; snippetIndex < snippets.size(); ++snippetIndex){
                for(int partIndex = 0; partIndex < snippets.get(snippetIndex).length; ++partIndex){
                    String val = "";
                    Pattern ampPtrn = Pattern.compile("[¶§]");
                    Matcher mtr = ampPtrn.matcher(snippets.get(snippetIndex)[partIndex]);
                    val = mtr.replaceAll("");

                    if (index.equals("token")){
                        Pattern ptrn = Pattern.compile("\\|G\\S*");
                        Matcher mtch = ptrn.matcher(val);
                        val = mtch.replaceAll("");
                    }
                    this.snippetsHodler.get(snippetIndex).get(partIndex).get(0).put(index, val);
                }
            }
        }
        mergeParts();
    }


    /**
     * After processing of snippet wrap it into <block></block> tag.
     * @param node
     * @return
     * @throws UnsupportedEncodingException
     */

    public List<String> makeResult(Node node) throws UnsupportedEncodingException {
        List<String> processedSnippets = new ArrayList<>();
        for (int x = 0; x < snippetsHodler.size(); x++) {

            String processedSnippet = processSnippet(x, node);
            if (processedSnippet.length() > 0)
                processedSnippets.add(processedSnippet
                        .replaceAll("<\\n+", "<")
                        .replaceAll("</\\n+", "</")
                        .replaceAll("\\n+>", ">")
                        .replaceAll(">\\n+", ">"))
                        ;
        }
        return processedSnippets;
    }

    /**
     * Getting attributes for every field and process them.
     * @param x
     * @param node
     * @return
     * @throws UnsupportedEncodingException
     */

    private String processSnippet(int x, Node node) throws UnsupportedEncodingException {

        StringBuilder blockBuilder = new StringBuilder();
        for (int i = 0; i < snippetsHodler.get(x).size(); i++) {

            Map<String, String> actuals = this.snippetsHodler.get(x).get(i).get(this.snippetsHodler.get(x).get(i).size() - 1);

            String currentToken = actuals.get(ConfigHolder.getClonedConfiguration().getField());
            String actualId = actuals.get("nerid");
            try{
                if (actualId.length() > 3 && !actuals.get("nertag").equals("0")) {
                    blockBuilder.append(buildEntityFieldAttrs(this.snippetsHodler.get(x).get(i)));
                } else {
                    if (currentToken != null && currentToken.length() > 0){
                        blockBuilder.append(buildNonEntityField(actuals, currentToken));
                    }

                }
            }catch (Exception e){
                continue;
            }


            blockBuilder.append(' ');
        }

        String out = node == null ? blockBuilder.toString() : "";

        return out;
    }


    /**
     * Based on style tag processed snippet.
     * @param attrs
     * @param content
     * @param me
     * @return
     */

    private String tagMe(String attrs, String content, String me){
        if (ConfigHolder.getClonedConfiguration().getStyle().toLowerCase().equals("ascii") || ConfigHolder.getClonedConfiguration().getStyle().toLowerCase().equals("raw")){
            Pattern dataPattern = Pattern.compile("data-");
            Matcher dataMatcher = dataPattern.matcher(attrs);
            String attributes = dataMatcher.replaceAll("");
            if (ConfigHolder.getClonedConfiguration().getStyle().toLowerCase().equals("ascii") && !me.equals("field")){
                Pattern spacesPattern = Pattern.compile("\\s+");
                Matcher spacesMatcher = spacesPattern.matcher(configAndPreformate.getDye().get(me));
                String withoutSpaces = spacesMatcher.replaceAll("");
                for (Color color:
                        Color.values()) {
                    if (color.toString().toLowerCase().equals(withoutSpaces.toLowerCase()))
                        return  color.getColor() + "<" + me  + attributes + ">" + content + "</" + me +">" + "\u001B[0m";
                }
            }
            return  "<" + me + attributes + ">" + content + "</" + me + "> ";
        }else {
            if (me.equals("span")){
                return "<span" + attrs + ">" + content + "</span> ";
            }else {
                return "<a" + attrs + ">" + content + "</a>";
            }
        }


    }

    /**
     * Process nont entity field.
     * @param properties
     * @param content
     * @return
     */
    private String buildNonEntityField(Map<String, String> properties, String content){
        StringBuilder builder = new StringBuilder();
        this.configAndPreformate.readNonEntiry(properties, this.format).forEach((key, value) -> {
                builder.append(" ").append(key).append("=\"").append(value).append('"');
        });
        if (builder.length() > 0){
            return tagMe(builder.toString(), content, ConfigHolder.getClonedConfiguration().getStyle().toLowerCase().equals("html") ? "span" : "field");
        }
        return content;
    }

    /**
     * Process entity field.
     * @param fields
     * @return
     */
    private String buildEntityFieldAttrs(List<Map<String, String>> fields) {

        StringBuilder processed = new StringBuilder();
        Map<String, List<String>> mergedAttrs = new HashMap<>();

        for (int index = 0; index < fields.size(); ++index){
            String content = fields.get(index).get("token");

            Map<String, String> properties = fields.get(index);

            if (format.get("attributes") != null
                    && ((ArrayList)format.get("attributes")).size() > 0){
                processed.append(buildNonEntityField(properties, content));

                for (Entry entry:
                        configAndPreformate.getMapping().get(fields.get(fields.size() - 1).get("nertag")).entrySet()) {
                    Pattern ptrn = Pattern.compile(entry.getValue().toString() + "=");
                    Matcher mtch = ptrn.matcher(processed.toString());
                    processed.setLength(0);
                    processed.append(mtch.replaceAll(entry.getKey().toString() + "="));
                }
            }

            if (format.get("nertags") != null && ((HashMap) format.get("nertags")).size() > 0){
                configAndPreformate.readEntity(content, properties, format).forEach((name, value) -> {
                    if (mergedAttrs.get(name) != null){
                        mergedAttrs.get(name).add(value);
                    }else {
                        List<String> values = new ArrayList<String>();
                        values.add(value);
                        mergedAttrs.put(name, values);
                    }
                });
            }
            if((format.get("nertag") == null) &&
                    (format.get("attributes") == null ||  ((ArrayList)format.get("attributes")).size() <= 0)){
                if (index < fields.size())
                    processed.append(content + " ");
            }

        }
        StringBuilder mergedEntitiesFields = new StringBuilder();
        if (mergedAttrs.size() > 0){
            if (((HashMap)format.get("nertags")).containsKey("all") || ((HashMap)format.get("nertags")).containsKey(fields.get(fields.size() - 1).get("nertag"))){

                int last = 0;
                for (Entry entry:
                        mergedAttrs.entrySet()) {
                    StringBuilder attrsOfEntity = new StringBuilder();

                    for (int i = 0; i < ((ArrayList)entry.getValue()).size(); ++i){
                        if (i < ((ArrayList)entry.getValue()).size() - 1){
                            attrsOfEntity.append(((ArrayList)entry.getValue()).get(i) + " ");
                        }else{
                            attrsOfEntity.append(((ArrayList)entry.getValue()).get(i));
                        }
                    }
                    if (! ConfigHolder.getClonedConfiguration().getNonDuplicateFields().contains(String.valueOf(entry.getKey()).replaceAll("data-", ""))){

                        mergedEntitiesFields.append(entry.getKey() + "=\"" + attrsOfEntity.toString().replaceAll("\\n", "") + "\" ");

                    }
                }

                for (Entry entry:
                        mergedAttrs.entrySet()){
                    if (ConfigHolder.getClonedConfiguration().getNonDuplicateFields().contains(String.valueOf(entry.getKey()).replaceAll("data-", ""))){
                        mergedEntitiesFields.append(entry.getKey() + "=\"" + String.valueOf((((ArrayList)entry.getValue()).get(((ArrayList)entry.getValue()).size() - 1))).replaceAll("\\n", "") + "\" ");
                    }
                }



                StringBuilder aux = new StringBuilder();
                aux.append(tagMe(" " + mergedEntitiesFields.toString(), processed.toString(), fields.get(fields.size() - 1).get("nertag")));
                processed.setLength(0);
                processed.append(aux.toString());
            }

        }else{
            if (format.get("nertags") != null){
                if (((HashMap)format.get("nertags")).size() <= 0){
                    StringBuilder aux = new StringBuilder();
                    aux.append(tagMe("", processed.toString(), fields.get(fields.size() - 1).get("nertag")));
                    processed.setLength(0);
                    processed.append(aux.toString());
                }

            }
        }


         return processed.toString();

    }
}
