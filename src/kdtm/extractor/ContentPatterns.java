package kdtm.extractor;

import orhun.core.io.Strings;
import orhun.core.text.Regexps;
import orhun.core.text.TextConsumer;
import orhun.core.text.TextUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static kdtm.extractor.Crawl4jExtractionCleaner.*;

/**
 * Created by sila on 28.06.2016.
 */
public class ContentPatterns {
    String source;
    List<Pattern> linePatterns = new ArrayList<>();
    List<Pattern> pagePatterns = new ArrayList<>();
    List<Pattern> urlRemovePatterns = new ArrayList<>();
    List<Pattern> urlAcceptPatterns = new ArrayList<>();
    List<Pattern> wordPattern = new ArrayList<>();
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
                case "I":
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
            result.put(meta, ContentPatterns.fromList(meta, ruleData));
        }

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
                //  System.out.println(s);
                next.remove(s);
            }
        }
        reduced = new LinkedHashSet<>(next);
//            if(extractorPattern.equals("ARTICLE")){
//                extractor = "ARTICLE";
//            } else if(extractorPattern.equals("EVERYTHING")){
//                extractor = "EVERYTHING";
//            }

        return reduced;
    }

}