package suskun.extractor;

import com.google.common.base.Splitter;
import zemberek.core.io.Strings;
import zemberek.core.text.Regexps;
import zemberek.core.text.TextConsumer;
import zemberek.core.text.TextUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ContentPatterns {
    String source;
    Set<Pattern> linePatterns = new LinkedHashSet<>();
    Set<Pattern> pagePatterns = new LinkedHashSet<>();
    Set<Pattern> urlRemovePatterns = new LinkedHashSet<>();
    Set<Pattern> urlAcceptPatterns = new LinkedHashSet<>();
    Pattern categoryPattern;
    Pattern titlePattern;
    Pattern labelPattern;
    Set<Pattern> wordPattern = new LinkedHashSet<>();
    String extractor;

    Map<Pattern, String> replacePatterns = new HashMap<>();
    Map<String, String> replaceWords = new HashMap<>();

    static ContentPatterns fromList(String source, List<String> rules) {
        ContentPatterns patterns = new ContentPatterns();
        patterns.source = source;
        for (String rule : rules) {
            if (rule.trim().length() == 0)
                continue;
            String category = Strings.subStringUntilFirst(rule, ":");
            String patternStr = Strings.subStringAfterFirst(rule, ":");
            switch (category) {
                case "E":
                    patterns.extractor = patternStr;
                    break;
                case "I-":
                    patterns.urlRemovePatterns.add(Pattern.compile(patternStr));
                    break;
                case "P":
                    patterns.pagePatterns.add(Pattern.compile(patternStr, Pattern.DOTALL));
                    break;
                case "L":
                    patterns.linePatterns.add(Pattern.compile(patternStr));
                    break;
                case "W":
                    patterns.wordPattern.add(Pattern.compile(patternStr));
                    break;
                case "I+":
                    patterns.urlAcceptPatterns.add(Pattern.compile(patternStr));
                case "LABEL":
                    patterns.labelPattern = Pattern.compile(patternStr, Pattern.DOTALL);
                    break;
                case "TITLE":
                    patterns.titlePattern = Pattern.compile(patternStr);
                    break;
                case "CATEGORY":
                    patterns.categoryPattern = Pattern.compile(patternStr);
                    break;
                case "RP":
                    String pattern = Strings.subStringUntilFirst(patternStr, "->").trim();
                    String value = Strings.subStringAfterFirst(patternStr, "->").trim();
                    patterns.replacePatterns.put(Pattern.compile(pattern), value);
                    break;
                case "RW-E":
                    String key = Strings.subStringUntilFirst(patternStr, "->").trim();
                    key = key.replaceAll("^\\[|]$", "");
                    String val = Strings.subStringAfterFirst(patternStr, "->").trim();
                    val = val.replaceAll("^\\[|]$", "");
                    patterns.replaceWords.put(key, decodeAsciiLetters(val));
                    break;
                case "RW":
                    String k = Strings.subStringUntilFirst(patternStr, "->").trim();
                    k = k.replaceAll("^\\[|]$", "");
                    String v = Strings.subStringAfterFirst(patternStr, "->").trim();
                    v = v.replaceAll("^\\[|]$", "");
                    patterns.replaceWords.put(k, v);
                    break;
            }
        }
        return patterns;
    }

    static String decodeAsciiLetters(String input) {
        StringBuilder sb = new StringBuilder();
        for (char c : input.toCharArray()) {
            if (c < 'A' || c > 'z') {
                sb.append(c);
            } else {
                sb.append((char) (c - 1));
            }
        }
        return sb.toString();
    }

    public void merge(ContentPatterns patterns) {
        this.getUrlRemovePatterns().addAll(patterns.getUrlRemovePatterns());
        this.getUrlAcceptPatterns().addAll(patterns.getUrlAcceptPatterns());
        this.getLinePatterns().addAll(patterns.getLinePatterns());
        this.getPagePatterns().addAll(patterns.getPagePatterns());
        this.getWordPattern().addAll(patterns.getWordPattern());
        this.getReplacePatterns().putAll(patterns.getReplacePatterns());
        this.getReplaceWords().putAll(patterns.getReplaceWords());
        if (patterns.categoryPattern != null) {
            this.categoryPattern = patterns.categoryPattern;
        }
        if (patterns.titlePattern != null) {
            this.titlePattern = patterns.titlePattern;
        }
        if (patterns.labelPattern != null) {
            this.labelPattern = patterns.labelPattern;
        }

    }


    public static Map<String, ContentPatterns> fromFile(Path path) throws IOException {

        Map<String, ContentPatterns> result = new HashMap<>();

        List<String> allLines = Files.readAllLines(path, StandardCharsets.UTF_8);
        allLines = allLines.stream().filter(s -> !s.startsWith("#")).collect(Collectors.toList());

        TextConsumer textConsumer = new TextConsumer(allLines);
        textConsumer.moveUntil(s -> s.trim().length() > 0 && !s.contains(":"));
        while (!textConsumer.finished()) {
            String meta = textConsumer.current().trim();
            textConsumer.advance();
            List<String> ruleData = textConsumer.moveUntil(s -> s.trim().length() > 0 && !s.contains(":"));
            ContentPatterns patterns = ContentPatterns.fromList(meta, ruleData);
            result.put(meta, patterns);
            result.put(meta.replaceAll("www\\.|http://|https://", ""), patterns);
        }

        ContentPatterns global = result.get("global");
        if (global == null) {
            return result;
        }

        result.keySet().forEach(s -> result.get(s).merge(global));

        return result;
    }

    public void applyReplacePatterns(WebDocument page) {
        if (page.lines.size() == 0) {
            return;
        }
        if (replaceWords.size() == 0 && replacePatterns.size() == 0) {
            return;
        }
        List<String> lines = new ArrayList<>(page.getLines().size());
        for (String line : page.lines) {
            String content = line.replaceAll("\\s|\u00A0"," ");
            for (String key : replaceWords.keySet()) {
                content = content.replaceAll(key, replaceWords.get(key)).trim();
            }
            for (Pattern pattern : replacePatterns.keySet()) {
                String s = replacePatterns.get(pattern);
                content = pattern.matcher(content).reset().replaceAll(s);
                // TODO: a hack.
                content = content.replaceAll("%n", "\n");
            }
            if (content.contains("\n")) {
                lines.addAll(Splitter.on("\n").omitEmptyStrings().trimResults().splitToList(content));
            } else {
                lines.add(content);
            }
        }
        page.setContent(lines);
    }

    public WebDocument reduce(
            WebDocument page,
            boolean removeDuplicates) {

        if (urlAcceptPatterns.size() > 0) {
            boolean accepted = false;
            for (Pattern urlAcceptPattern : urlAcceptPatterns) {
                if (Regexps.matchesAny(urlAcceptPattern, page.id)) {
                    accepted = true;
                }
            }
            if (!accepted)
                return page.emptyContent();
        }

        for (Pattern urlPattern : urlRemovePatterns) {
            if (Regexps.matchesAny(urlPattern, page.id)) {
                return page.emptyContent();
            }
        }


        for (Pattern pagePattern : pagePatterns) {
            if (Regexps.matchesAny(pagePattern, page.getContentAsString())) {
                return page.emptyContent();
            }
        }

        Collection<String> reduced = removeDuplicates ? reduceLines(page).result : reduceLinesNoUnique(page);
        return page.copy(new ArrayList<>(reduced));
    }


    private Collection<String> reduceLinesNoUnique(WebDocument page) {
        List<String> reduced = new ArrayList<>(page.lines);
        List<String> next = new ArrayList<>(reduced);
        for (Pattern linePattern : linePatterns) {
            for (String s : reduced) {
                if (!Regexps.matchesAny(linePattern, s)) {
                    next.add(s);
                }
            }
            reduced = new ArrayList<>(next);
        }

        // clean, normalize and separate some connected words. replace word patter matches with space.
        next = new ArrayList<>();
        for (String s : reduced) {
            String cleanAndNormalized = cleanAndNormalize(s);
            for (Pattern pattern : wordPattern) {
                cleanAndNormalized = pattern.matcher(cleanAndNormalized).replaceAll(" ");
            }
            next.add(TextUtil.separatePunctuationConnectedWords(cleanAndNormalized, 3));
        }
        reduced = new ArrayList<>(next);
        List<String> result = new ArrayList<>();

        for (String s : reduced) {
            if (digitRatio(s) <= 0.2 /*&& capitalRatio(s) <= 0.3 && !(s.length() > 20 && badlyTypedTurkish(s))*/) {
                result.add(s);
            }
        }
        return result;
    }

    public static String cleanAndNormalize(String input) {
        return TextUtil.cleanAllHtmlRelated(
                TextUtil.normalizeQuotesHyphens(
                        TextUtil.convertAmpresandStrings(
                                TextUtil.cleanCdataIllegalChars(input, " "))));
    }


    static class ReducedResult {
        LinkedHashSet<String> result;
        int digitRemovedCount = 0;
        int capitalRemoveCount = 0;
        int repetitionCount = 0;
        int badlyTypedTurkish = 0;
    }

    private ReducedResult reduceLines(WebDocument page) {

        ReducedResult result = new ReducedResult();

        LinkedHashSet<String> reduced = new LinkedHashSet<>(page.lines);
        LinkedHashSet<String> next = new LinkedHashSet<>(reduced);
        for (Pattern linePattern : linePatterns) {
            for (String s : reduced) {
                if (Regexps.matchesAny(linePattern, s)) {
                    next.remove(s);
                }
            }
            reduced = new LinkedHashSet<>(next);
        }

        // clean, normalize and separate some connected words. replace word patter matches with space.
        next = new LinkedHashSet<>();
        for (String s : reduced) {
            String cleanAndNormalized = cleanAndNormalize(s);
            for (String key : replaceWords.keySet()) {
                cleanAndNormalized = cleanAndNormalized.replaceAll(key, replaceWords.get(key));
            }
            for (Pattern pattern : wordPattern) {
                cleanAndNormalized = pattern.matcher(cleanAndNormalized).replaceAll(" ");
            }
            //TODO: not sure about this.
            next.add(TextUtil.separatePunctuationConnectedWords(cleanAndNormalized, 3));
        }
        reduced = new LinkedHashSet<>(next);


        for (String s : reduced) {
            if (digitRatio(s) > 0.2) {
                next.remove(s);
                result.digitRemovedCount++;
            }
        }

        reduced = new LinkedHashSet<>(next);


        for (String s : reduced) {
            if (capitalRatio(s) > 0.3) {
                next.remove(s);
                result.capitalRemoveCount++;
            }
        }
        reduced = new LinkedHashSet<>(next);

        for (String s : reduced) {
            if (s.length() > 20 && badlyTypedTurkish(s)) {
                next.remove(s);
                result.badlyTypedTurkish++;
            }
        }
        reduced = new LinkedHashSet<>(next);

        for (String s : reduced) {
            if (s.length() > 50 && repetitionRation(s) < 0.7) {
                next.remove(s);
                result.repetitionCount++;
            }
        }

        result.result = next;
        return result;
    }

    private float repetitionRation(String s) {
        List<String> l = Splitter.on(" ").splitToList(s);
        LinkedHashSet<String> set = new LinkedHashSet<>(l);
        return (set.size() * 1f) / l.size();
    }


    public static double digitRatio(String s) {
        if (s.trim().length() == 0)
            return 0;
        int d = 0;
        for (char c : s.toCharArray()) {
            if (Character.isDigit(c)) {
                d++;
            }
        }
        return (d * 1d) / s.length();
    }

    public static double capitalRatio(String s) {
        if (s.trim().length() == 0)
            return 0;
        int d = 0;
        for (char c : s.toCharArray()) {
            if (Character.isUpperCase(c)) {
                d++;
            }
        }
        return (d * 1d) / s.length();
    }

    private static String turkishChars = "[çğıöşüÇŞĞÜÖİ]";
    private static char[] turkishCharArray = turkishChars.toCharArray();

    private static Pattern turkishCharsPattern = Pattern.compile(turkishChars);


    // simple heuristic for catching badly typed Turkish sentences.
    // This checks if sentence is written without Turkish characters,
    // or 'ı' is used instead of 'i'
    // this kind of mistakes are pretty common in forum like places.
    static public boolean badlyTypedTurkish(String s) {
        int dotlessICount = TextUtil.countChars(s, 'ı');
        int iCount = TextUtil.countChars(s, 'i');

        if (dotlessICount > 0) {
            if (iCount == 0 || ((double) iCount) / dotlessICount < 0.2d)
                return true;
        }
        if (!Regexps.matchesAny(turkishCharsPattern, s)) {
            return true;
        }
        int turkishCharCount = TextUtil.countChars(s, turkishCharArray);
        return ((double) turkishCharCount) / s.length() < 0.05d;
    }

    private static Pattern URL_PERCENT_PATTERN = Pattern.compile("[%][A-Fa-f0-9]{2}");

    /**
     * Converts a string like
     * http%3A%2F%2Fwowturkey.com%2Fforum%2Fviewtopic.php%3Fstart%3D10%26t%3D124646
     * to
     * http://wowturkey.com/forum/viewtopic.php?start=10&t=124646
     */
    static String normalizePercentStrings(String in) {
        Matcher matcher = URL_PERCENT_PATTERN.matcher(in);
        StringBuffer sb = new StringBuffer(in.length());
        while (matcher.find()) {
            String text = matcher.group();
            char c = (char) Integer.parseInt(text.replace("%", ""), 16);
            matcher.appendReplacement(sb, Matcher.quoteReplacement(String.valueOf(c)));
        }
        if (sb.length() > 0)
            matcher.appendTail(sb);
        if (sb.length() == 0) {
            return in;
        }
        return sb.toString();
    }


    public String getSource() {
        return source;
    }

    public Set<Pattern> getLinePatterns() {
        return linePatterns;
    }

    public Set<Pattern> getPagePatterns() {
        return pagePatterns;
    }

    public Set<Pattern> getUrlRemovePatterns() {
        return urlRemovePatterns;
    }

    public Set<Pattern> getUrlAcceptPatterns() {
        return urlAcceptPatterns;
    }

    public Set<Pattern> getWordPattern() {
        return wordPattern;
    }

    public String getExtractor() {
        return extractor;
    }

    public Map<Pattern, String> getReplacePatterns() {
        return replacePatterns;
    }

    public Map<String, String> getReplaceWords() {
        return replaceWords;
    }
}