package suskun.extractor;

import com.google.common.collect.Lists;
import zemberek.core.logging.Log;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class Reducer {

    public static void main(String[] args) throws IOException {

        Path sourceRoot = Paths.get("/media/data/corpora/single-file");
        List<Path> paths = Lists.newArrayList(Files.walk(sourceRoot, 1)
                .filter(path -> path.toFile().isFile()).iterator());

        Path outRoot = Paths.get("/media/data/corpora/single-file-reduced");
        reduce(paths, outRoot, false, true);
    }

    public static void reduce(List<Path> files, Path outRoot, boolean saveOnlyContent, boolean removeDuplicateLines) throws IOException {

        Files.createDirectories(outRoot);
        Map<String, ContentPatterns> removePatternsMap =
                ContentPatterns.fromFile(Paths.get("content-rules.txt"));


        for (Path file : files) {
            Log.info("Loading %s", file);
            WebCorpus corpus = new WebCorpus(file.toFile().getName(), file.toFile().getName());
            corpus.addDocuments(WebCorpus.loadDocuments(file));
            Log.info(corpus);
            Log.info("Total = %d ", corpus.getPages().size());

            ContentPatterns patterns = removePatternsMap.get(corpus.source.replaceAll("\\.corpus$",""));
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
                WebCorpus corpus = new WebCorpus(file.toFile().getName(), "foo");
                corpus.addDocuments(WebCorpus.loadDocuments(file));

                Log.info(corpus);
                Log.info("Total = %d ", corpus.getPages().size());

                ContentPatterns patterns = removePatternsMap.get(corpus.source);
                if (patterns == null) {
                    Log.warn("No remove pattern found for " + corpus.source);
                    patterns = new ContentPatterns();
                }

                corpus.saveReduced(patterns, outRoot, saveOnlyContent, true);
            }
        }
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

