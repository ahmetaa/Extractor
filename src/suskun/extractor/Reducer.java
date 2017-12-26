package suskun.extractor;

import com.google.common.collect.Lists;
import zemberek.core.logging.Log;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Reducer {

    public static void main(String[] args) throws IOException {

        Path sourceRoot = Paths.get("/media/aaa/Data/corpora/forum-test");
        Path outRoot = Paths.get("/media/aaa/Data/corpora/forum-test-reduced");
        reduceAll(sourceRoot, outRoot, false);

    }

    private static void reduceAll(Path sourceRoot, Path outRoot, boolean saveOnlyContent) throws IOException {
        List<Path> sourceDirs = Lists.newArrayList(Files.walk(sourceRoot, 1)
                .filter(path -> path.toFile().isDirectory() && !path.equals(sourceRoot)).iterator());
        for (Path sourceDir : sourceDirs) {
            Log.info("Processing %s", sourceDir);
            reduceSingle(sourceDir, outRoot, saveOnlyContent);
        }
    }

    public static void reduce(
            List<Path> files,
            Path outRoot,
            boolean saveOnlyContent,
            boolean removeDuplicateLines) throws IOException {

        Files.createDirectories(outRoot);
        Map<String, ContentPatterns> removePatternsMap =
                ContentPatterns.fromFile(Paths.get("content-rules.txt"));


        for (Path file : files) {
            Log.info("Loading %s", file);
            WebCorpus corpus = new WebCorpus(file.toFile().getName(), file.toFile().getName());
            corpus.addDocuments(WebCorpus.loadDocuments(file));
            Log.info(corpus);
            Log.info("Total = %d ", corpus.getPages().size());

            ContentPatterns patterns = removePatternsMap.get(corpus.source.replaceAll("\\.corpus$", ""));
            if (patterns == null) {
                Log.warn("No remove pattern found for " + corpus.source);
                patterns = new ContentPatterns();
            }

            WebCorpus reducedCorpus = new WebCorpus(corpus.source, corpus.id);
            reducedCorpus.addDocuments(corpus.getReducedPages(patterns, removeDuplicateLines));

            Log.info(reducedCorpus);
            Log.info("Total Reduced Docs = %d ", corpus.getPages().size());

            reducedCorpus.save(outRoot.resolve(file.toFile().getName()), saveOnlyContent);
        }
    }


    public static void reduceSingle(Path sourceRoot, Path outRoot, boolean saveOnlyContent) throws IOException {

        Map<String, ContentPatterns> removePatternsMap =
                ContentPatterns.fromFile(Paths.get("content-rules.txt"));

        String name = sourceRoot.toFile().getName();

        List<Path> files = Lists.newArrayList(Files.walk(sourceRoot, 1)
                .filter(path -> path.toFile().isFile()).iterator());


        for (Path file : files) {
            WebCorpus corpus = new WebCorpus(name, file.toFile().getName());
            Path dir = outRoot.resolve(corpus.source);
            Files.createDirectories(dir);
            Path out = dir.resolve(corpus.id);
            if (out.toFile().exists()) {
                Log.info("%s Exists. skipping.", out);
                continue;
            }
            corpus.addDocuments(WebCorpus.loadDocuments(file));

            Log.info(corpus);
            Log.info("Total = %d ", corpus.getPages().size());

            ContentPatterns patterns = removePatternsMap.get(name);
            if (patterns == null) {
                Log.warn("No remove pattern found for " + name);
                patterns = new ContentPatterns();
            }

            List<WebDocument> reducedPages = corpus.getReducedPages(patterns, true);
            corpus.removeAll();
            corpus.addDocuments(reducedPages);
            corpus.save(out, saveOnlyContent);
        }

    }


    private static void stats(Path... dirs) throws IOException {
        stats(Arrays.asList(dirs));
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
                WebCorpus corpus = new WebCorpus(file.toFile().getName(), "foo");
                corpus.addDocuments(WebCorpus.loadDocuments(file));
                Log.info(corpus);
                Log.info("Total = %d ", corpus.getPages().size());
                int u = corpus.sizeUnique();
                Log.info("Unique = %d ", u);
                total += corpus.getPages().size();
                unique += u;
                dirTotal += corpus.getPages().size();
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

