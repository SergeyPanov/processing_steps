package cz.vutbr.fit.knot.corpproc.queryserver;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ConstraintHolder {

    private Integer number = 0;
    private String constraint = "";
    private List<String> values = new ArrayList<>();
    private final List<HashMap<String, String>> possibleMatches = new ArrayList<>();
    private boolean sequenceMark = false;
    private ConstraintHolder before = null;
    private ConstraintHolder near = null;
    private int proximity = 0;

    public Integer getNumber() {
        return number;
    }

    public String getConstraint() {
        return constraint;
    }

    public List<String> getValues() {
        return values;
    }

    public boolean isSequenceMark() {
        return sequenceMark;
    }

    public ConstraintHolder getBefore() {
        return before;
    }

    public ConstraintHolder getNear() {
        return near;
    }

    public int getProximity() {
        return proximity;
    }

    public void addMatch(HashMap<String, String> match) {
        possibleMatches.add(match);
    }

    public void clearPossibilities() {
        possibleMatches.clear();
    }

    public List<HashMap<String, String>> getMatches() {
        return possibleMatches;
    }

    /**
     * The pattern for global constraints
     */
    private static final Pattern GLOB_CONSTRAINTS = Pattern.compile("(\")?\\(?(([1-9][0-9]*):)?(([^\\(: ]+))(($)|(\\:((\\w|_)+\\*?))|(:(\\(.*?\\)))|([^:]))");
    /**
     * The pattern for extracting values
     */
    private static final Pattern CONSTRAINT_VALUES = Pattern.compile("\\((.*)\\)");
    /**
     * The pattern for proximity queries
     */
    private static final Pattern PROXIMITY = Pattern.compile("\\(\\(?(([1-9][0-9]*):)?((\\w+):)?(([^\\(: ]+)|(\\(.*?\\)))(\\{\\{.*?\\}\\})?\\)?\\s+\\(?(([1-9][0-9]*):)?((\\w+):)?(([^\\(: ]+)|(\\(.*?\\)))(\\{\\{.*?\\}\\})?\\)?\\s*\\)~([1-9][0-9]*)");

    private static List<String> parseValues(String val) {
        Matcher m = CONSTRAINT_VALUES.matcher(val);
        if (!m.find()) {
            return Arrays.asList(new String[]{val.replaceAll("[()]", "").trim()});
        }

        return Arrays.stream(m.group(1).split("OR"))
                .map(v -> v.replaceAll("[()]", "").trim())
                .collect(Collectors.toList());
    }

    public static List<ConstraintHolder> parseConstraints(List<String> indexNames, String origQ) {
        ArrayList<ConstraintHolder> constraints = new ArrayList<>();

        Matcher p = PROXIMITY.matcher(origQ);
        while (p.find()) {
            String firstNumber = p.group(2);
            String firstConstraint = p.group(4);
            String firstValue = p.group(5);
            String secondNumber = p.group(10);
            String secondConstraint = p.group(12);
            String secondValue = p.group(13);
            int prox = Integer.parseInt(p.group(17));

            ConstraintHolder sc = new ConstraintHolder();
            ConstraintHolder fc = new ConstraintHolder();

            fc.number = firstNumber != null ? Integer.parseInt(firstNumber) : 0;
            sc.number = secondNumber != null ? Integer.parseInt(secondNumber) : 0;

            fc.values = parseValues(firstValue.replaceAll("\\{\\{.*?\\}\\}", ""));
            sc.values = parseValues(secondValue.replaceAll("\\{\\{.*?\\}\\}", ""));

            fc.constraint = firstConstraint != null ? firstConstraint : "token";
            sc.constraint = secondConstraint != null ? secondConstraint : "token";

            fc.proximity = sc.proximity = prox;

            fc.near = sc;
            sc.near = fc;

            constraints.add(fc);
            constraints.add(sc);
        }

        Matcher m = GLOB_CONSTRAINTS.matcher(p.replaceAll(""));

        boolean mark = false;
        int start = 0;
        int last = 0;

        while (m.find()) {
            ConstraintHolder constraintHolder = new ConstraintHolder();
            String in = m.group(3);
            String val = m.group(4).trim();

            if (val.startsWith("\"") || m.group(1) != null) {
                mark = true;
            }

            constraintHolder.sequenceMark = mark;

            if (val.endsWith("\"")) {
                mark = false;
            }

            val = val.replace("\"", "");

            if (val.length() == 0 || val.contains("{{")) {
                continue;
            }

            if (in != null) {
                constraintHolder.number = Integer.parseInt(in);
            }
            if (indexNames.contains(val)) {
                constraintHolder.constraint = val;
                val = m.group(6).substring(1).trim();
            } else {
                constraintHolder.constraint = "token";
            }

            constraintHolder.values = parseValues(val);

            if (start != 0) {
                String sub = origQ.substring(start, last);
                if (sub.indexOf('<') >= 0) {
                    constraintHolder.before = constraints.get(constraints.size() - 1);
                }
            }

            start = m.end();
            last = start;
            constraints.add(constraintHolder);
        }
        return constraints;
    }
}
