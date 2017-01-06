package suskun.extractor;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import zemberek.core.logging.Log;
import zemberek.core.text.Regexps;
import zemberek.core.text.TextConsumer;
import zemberek.core.text.TextUtil;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Crawl4jExtractionCleaner {

    public static void main(String[] args) throws IOException {


        reduce(Paths.get("/media/disk2/corpora/raw"), Paths.get("/media/disk2/corpora/clean"), false);

    }

    public static class Corpus {
        String source;
        String id;

        List<Page> pages = new ArrayList<>();

        public Corpus(String source, String id, List<Page> pages) {
            this.source = source;
            this.id = id;
            this.pages = pages;
        }

        static Corpus loadFromCrawl4jExport(String source, Path corpusFile) throws IOException {
            List<String> allLines = Files.readAllLines(corpusFile, StandardCharsets.UTF_8);

            List<Page> pages = new ArrayList<>(allLines.size() / 10);

            TextConsumer textConsumer = new TextConsumer(allLines);
            textConsumer.moveUntil(s -> s.startsWith("<doc id="));
            while (!textConsumer.finished()) {
                String meta = textConsumer.current();
                textConsumer.advance();
                List<String> pageData = textConsumer.moveUntil(s -> s.startsWith("<doc id="));
                Page e = Page.fromText(meta, pageData);
                if (e != null) {
                    pages.add(e);
                }
            }
            return new Corpus(source, corpusFile.toFile().getName(), pages);
        }

        @Override
        public String toString() {
            return source + "-" + id;
        }

        public int sizeUnique() {
            return pages.stream().map(Page::contentHash).collect(Collectors.toSet()).size();
        }

        public int totalPageLineCount() {
            int total = 0;
            for (Page page : pages) {
                for (String line : page.lines) {
                    if (line.length() == 0)
                        continue;
                    total++;
                }
            }
            return total;
        }

        public int uniquePageLineCount() {

            Set<Long> hashes = new HashSet<>(100000);
            for (Page page : pages) {
                for (String line : page.lines) {
                    hashes.add(Hashing.murmur3_128().hashUnencodedChars(line).asLong());
                }
            }
            return hashes.size();
        }

        public void saveReduced(ContentPatterns patterns, Path outRoot, boolean onlyContent, boolean removeDuplicatedLines) throws IOException {

            List<Page> reducedPages = getReducedPages(patterns, removeDuplicatedLines);

            Path resolve = outRoot.resolve(source);
            Files.createDirectories(resolve);

            try (PrintWriter p = new PrintWriter(resolve.resolve(id).toFile(), "utf-8")) {
                for (Page reducedPage : reducedPages) {
                    if (!onlyContent) {
                        p.println(reducedPage.url);
                    }
                    p.println(reducedPage.content());
                    p.println("</doc>");
                }
            }
        }

        public List<Page> getReducedPages(ContentPatterns patterns, boolean removeDuplicatedLines) {
            // remove duplicated lines.
            List<Page> uniqueLinePages = new ArrayList<>();

            Set<Long> lineHashes = new HashSet<>(100000);
            int all = 0;
            int unique = 0;

            for (Page page : this.pages) {
                List<String> uniqueLines = new ArrayList<>();
                all += page.lines.size();
                for (String l : page.lines) {
                    l = l.trim().replaceAll("\\s+", " ");
                    if (l.length() == 0)
                        continue;
                    // eliminate single words.
                    if (!l.contains(" "))
                        continue;
                    long h = Hashing.murmur3_128().hashUnencodedChars(l).asLong();
                    if (!lineHashes.contains(h) || !removeDuplicatedLines) {
                        lineHashes.add(h);
                        uniqueLines.add(l);
                    }
                }

                unique += uniqueLines.size();
                uniqueLinePages.add(page.copy(uniqueLines));
            }
            Log.info("Before unique line = %d  After = %d", all, unique);

            List<Page> reducedPages;

            if (patterns != null) {
                reducedPages = new ArrayList<>();
                for (Page p : uniqueLinePages) {
                    Page r = patterns.reduce(p, removeDuplicatedLines);
                    if (r.lines.isEmpty())
                        continue;
                    reducedPages.add(r);
                }
            } else {
                reducedPages = new ArrayList<>(uniqueLinePages);
            }
            if (reducedPages.size() == 0) {
                Log.warn("No page with content left!");
            } else {
                Log.info("Total pages to save %d", reducedPages.size());
            }
            return reducedPages;
        }
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

    static String turkishChars = "[çğıöşüÇŞĞÜÖİ]";
    static char[] turkishCharArray = turkishChars.toCharArray();

    static Pattern turkishCharsPattern = Pattern.compile(turkishChars);


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

    public static class Page {
        String source;
        String id;
        String url;

        List<String> lines = new ArrayList<>();

        public Page(String source, String id, List<String> lines, String url) {
            this.source = source;
            this.id = id;
            this.lines = lines;
            this.url = url;
        }

        public Page emptyContent() {
            return new Page(this.source, this.id, Collections.emptyList(), this.url);
        }

        public static Page fromText(String meta, List<String> pageData) {
            String id = normalizePercentStrings(meta).replaceAll("^#####|http://|https://", "").replace("www.", "");
            if (!id.contains("/")) {
                Log.warn("Cannot identify id form meta: %s", meta);
                return null;
            } else {
                String source = id.substring(0, id.indexOf("/"));
                return new Page(source, id, pageData, meta);
            }
        }

        public long contentHash() {
            return com.google.common.hash.Hashing.murmur3_128().hashUnencodedChars(content()).asLong();
        }

        public String content() {
            return Joiner.on("\n").join(lines);
        }

        public Page copy(Collection<String> reduced) {
            return new Page(this.source, this.id, new ArrayList<>(reduced), this.url);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Page page = (Page) o;

            return page.contentHash() == this.contentHash();

        }

        @Override
        public int hashCode() {
            long h = contentHash();
            return (int) ((h & 0xffffffffL) ^ (h >> 32));
        }


    }

    static Pattern URL_PERCENT_PATTERN = Pattern.compile("[%][A-Fa-f0-9]{2}");


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

    public static void reduce(Path sourceRoot, Path outRoot, boolean saveOnlyContent) throws IOException {

        List<Path> dirs = Lists.newArrayList(Files.walk(sourceRoot)
                .filter(path -> path.toFile().isDirectory()).iterator());
        Files.createDirectories(outRoot);
        Map<String, ContentPatterns> removePatternsMap =
                ContentPatterns.fromFile(Paths.get("content-rules.txt"));


        for (Path dir : dirs) {

            List<Path> files = Lists.newArrayList(Files.walk(dir, 1)
                    .filter(path -> path.toFile().isFile()).iterator());
            if (files.size() == 0) continue;

            for (Path file : files) {
                Corpus corpus = Corpus.loadFromCrawl4jExport(dir.toFile().getName(), file);

                Log.info(corpus);
                Log.info("Total = %d ", corpus.pages.size());

                ContentPatterns patterns = removePatternsMap.get(corpus.source);
                if (patterns == null) {
                    Log.warn("No remove pattern found for " + corpus.source);
                    patterns = new ContentPatterns();
                }

                corpus.saveReduced(patterns, outRoot, saveOnlyContent, true);
            }
        }
    }

    public static String cleanAndNormalize(String input) {
        return TextUtil.cleanAllHtmlRelated(
                TextUtil.normalizeQuotesHyphens(
                        TextUtil.convertAmpresandStrings(
                                TextUtil.cleanCdataIllegalChars(input, " "))));
    }

    private static void stats(List<Path> dirs) throws IOException {
        int total = 0, unique = 0;
        int totalLine = 0, totalUniqueLine = 0;


        for (Path dir : dirs) {

            int dirTotal = 0, dirUnique = 0;
            int dirLineTotal = 0, dirLineUnique = 0;


            List<Path> files = Lists.newArrayList(Files.walk(dir, 1)
                    .filter(path -> path.toFile().isFile()).iterator());
            if (files.isEmpty()) continue;


            for (Path file : files) {
                Corpus corpus = Corpus.loadFromCrawl4jExport(dir.toFile().getName(), file);
                Log.info(corpus);
                Log.info("Total = %d ", corpus.pages.size());
                int u = corpus.sizeUnique();
                Log.info("Unique = %d ", u);
                total += corpus.pages.size();
                unique += u;
                dirTotal += corpus.pages.size();
                dirUnique += u;

                Log.info("Total lines = %d", corpus.totalPageLineCount());
                int i = corpus.uniquePageLineCount();
                Log.info("Total unique lines = %d", i);
                dirLineTotal += corpus.totalPageLineCount();
                dirLineUnique += i;

                totalLine += corpus.totalPageLineCount();
                totalUniqueLine += i;

            }

            Log.info("Dir Total = %d", dirTotal);
            Log.info("Dir Unique = %d", dirUnique);

            Log.info("Dir Total line = %d", dirLineTotal);
            Log.info("Dir Unique line = %d", dirLineUnique);

        }

        Log.info("All Total = %d", total);
        Log.info("All Unique = %d", unique);

        Log.info("All Total Line = %d", totalLine);
        Log.info("All Unique Line = %d", totalUniqueLine);
    }

}

