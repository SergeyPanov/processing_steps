/**
 * Class modify input query(constraints) and makes it understandable to MG4J engine.
 */
package cz.vutbr.fit.knot.corpproc.queryserver;



import cz.vutbr.fit.knot.corpproc.config.ConfigHolder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class QueryTranslater {

    private static QueryTranslater instance;

    private final List<String> reservedSymbols = Arrays.asList("<", ">", "!", "^", "-");

    public static QueryTranslater getInstance()
    {
        if (instance == null)
        {
            instance = new QueryTranslater();
        }
        return instance;
    }

    /**
     * Simple method for processing tokens. Is able to recognize tokens without tag 'token:'
     * @param query
     * @return processed query
     */
    private String processTokens(String query)
    {
        StringBuilder token = new StringBuilder();
        StringBuilder newQuery = new StringBuilder();
        int index = 0;
        State current = State.BEGIN;
        while (index <= query.length() - 1)
        {
            if (Character.isWhitespace(query.charAt(index)))
                current = State.BEGIN;
            if (query.charAt(index) == '(')
                current = State.LEFT_BRACE;
            if (query.charAt(index) == '"')
                current = State.LEFT_QUOTE;
            if (reservedSymbols.contains(Character.toString(query.charAt(index))))
                current = State.RESERVED;
            if (!Character.isWhitespace(query.charAt(index)) && current != State.LEFT_BRACE && current != State.RESERVED && current != State.LEFT_QUOTE)
                current = State.TOKEN;


            switch (current){
                case BEGIN:
                    if (token.toString().length() > 0)
                    {
                        if (!token.toString().equals("_SENT_") && !token.toString().equals("_PAR_"))
                        {
                            newQuery.append("token:\"" + token.toString() + "\"");
                        }
                        else
                        {
                            newQuery.append(token.toString());
                        }

                        token.setLength(0);
                    }
                    break;

                case RESERVED:
                    newQuery.append(query.charAt(index));
                    current = State.BEGIN;
                    break;

                case LEFT_BRACE:
                    int aux = getFinishIndex('(', ')', query, index) + 1;
                    newQuery.append(query.substring(index, aux));
                    index = aux - 1; current = State.BEGIN;
                    break;

                case LEFT_QUOTE:
                    int aux2 = getFinishIndex('"', '"', query, index) + 1;
                    newQuery.append("token:" + query.substring(index, aux2));
                    index = aux2 - 1;
                    current = State.BEGIN;
                    break;

                case TOKEN:
                    token.append(query.charAt(index));
                    break;

                default: current = State.BEGIN; break;
            }

            ++index;
        }


        if (token.toString().length() > 0)
        {
            if (!token.toString().equals("_SENT_") && !token.toString().equals("_PAR_"))
            {
                newQuery.append("token:\"" + token.toString() + "\"");
            }else
            {
                newQuery.append(token.toString());
            }


        }

        return newQuery.toString();
    }

    /**
     * Makes query understandable for mg4j query engine.
     * @param query input query
     * @return
     */
    public String modifyQuery(String query, boolean isConstraint)
    {
        Map<String, String> mapping =  ConfigHolder.getClonedConfiguration().getModifMapping();
        for (Map.Entry<String, String> entry:
                mapping.entrySet())
        {
            Pattern pt = Pattern.compile(entry.getKey());
            Matcher mt = pt.matcher(query);
            query = mt.replaceAll(entry.getValue());
        }

        Pattern remap = Pattern.compile("((\\d+:)?(" + ConfigHolder.getClonedConfiguration().getRemapTags() + "):\\s*((\\w+)|(\\(.*?\\))))");
        Matcher m = remap.matcher(query);
        StringBuffer s = new StringBuffer();
        int index = 0;
        boolean found = false;

        while (m.find()) {
            s.append(query.substring(index, m.start()));
            s.append("(" + m.group(1) + "{{" + m.group(3) + "->" + ConfigHolder.getClonedConfiguration().getRemapTagsTo() + "}})");
            index = m.end();
            found = true;
        }

        if (found) {
            s.append(query.substring(index));
            query = s.toString();
        }
        if (isConstraint)
            return query;
        return processTokens(query);
    }


    private int getFinishIndex(char begin, char end, String text, int start)
    {
        int index = start;
        int braces = 0;

        boolean same = begin == end;
        while (true)
        {
            if (!same){
                if (text.charAt(index) == begin){
                    ++braces;
                }

                if (text.charAt(index) == end) {
                    --braces;
                }
            }else {
                if (text.charAt(index) == begin){
                    if (braces > 0){
                        --braces;
                    }else {
                        ++braces;
                    }
                }
            }


            if (braces == 0)
                return index;
        ++index;
        }
    }
}
