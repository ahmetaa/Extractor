package suskun.extractor;

import zemberek.core.io.Strings;
import zemberek.core.text.Regexps;
import zemberek.core.text.TextConsumer;
import zemberek.core.text.TextUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static suskun.extractor.Crawl4jExtractionCleaner.*;

public class ContentPatterns {
    String source;
    Set<Pattern> linePatterns = new LinkedHashSet<>();
    Set<Pattern> pagePatterns = new LinkedHashSet<>();
    Set<Pattern> urlRemovePatterns = new LinkedHashSet<>();
    Set<Pattern> urlAcceptPatterns = new LinkedHashSet<>();
    Set<Pattern> wordPattern = new LinkedHashSet<>();
    String extractor ;

    Map<Pattern, String> replacePatterns = new HashMap<>();

    static ContentPatterns fromList(String  source, List<String> rules) {
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
                case "R":
                    String pattern = Strings.subStringUntilFirst(patternStr, "->").trim();
                    String value = Strings.subStringAfterFirst(patternStr, "->").trim();
                    patterns.replacePatterns.put(Pattern.compile(pattern), value);
                    break;
            }
        }
        return patterns;
    }

    public void merge(ContentPatterns patterns)  {
        this.getUrlRemovePatterns().addAll(patterns.getUrlRemovePatterns());
        this.getUrlAcceptPatterns().addAll(patterns.getUrlAcceptPatterns());
        this.getLinePatterns().addAll(patterns.getLinePatterns());
        this.getPagePatterns().addAll(patterns.getPagePatterns());
        this.getWordPattern().addAll(patterns.getWordPattern());
        this.getReplacePatterns().putAll(patterns.getReplacePatterns());
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
            result.put(meta.replaceAll("www\\.",""), patterns);
        }

        ContentPatterns global = result.get("global");
        if(global==null) {
            return result;
        }

        result.keySet().forEach(s->result.get(s).merge(global));

        return result;
    }

    public Crawl4jExtractionCleaner.Page reduce(Crawl4jExtractionCleaner.Page page, boolean removeDuplicates) {

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
            if (Regexps.matchesAny(pagePattern, page.content())) {
                return page.emptyContent();
            }
        }

        Collection<String> reduced = removeDuplicates ? reduceLines(page) : reduceLinesNoUnique(page);
        return page.copy(reduced);
    }


    private Collection<String> reduceLinesNoUnique(Crawl4jExtractionCleaner.Page page) {
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
    private LinkedHashSet<String> reduceLines(Crawl4jExtractionCleaner.Page page) {
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
            for (Pattern pattern : wordPattern) {
                cleanAndNormalized = pattern.matcher(cleanAndNormalized).replaceAll(" ");
            }
            next.add(TextUtil.separatePunctuationConnectedWords(cleanAndNormalized, 3));
        }
        reduced = new LinkedHashSet<>(next);

        for (String s : reduced) {
            if (digitRatio(s) > 0.2) {
                next.remove(s);
            }
        }
        reduced = new LinkedHashSet<>(next);

        for (String s : reduced) {
            if (capitalRatio(s) > 0.3) {
                next.remove(s);
            }
        }
        reduced = new LinkedHashSet<>(next);

        for (String s : reduced) {
            if (s.length() > 20 && badlyTypedTurkish(s)) {
                next.remove(s);
            }
        }
        reduced = new LinkedHashSet<>(next);
        return reduced;
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
}