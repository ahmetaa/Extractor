package suskun.extractor;

import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import zemberek.core.collections.LongUIntMap;
import zemberek.core.logging.Log;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class DuplicateLineRemover {

    LongUIntMap map1 = new LongUIntMap(10000000);
    LongUIntMap map2 = new LongUIntMap(10000000);

    Locale locale = new Locale("Turkish");
    Map<String, ContentPatterns> contentPatternsMap;

    public DuplicateLineRemover(Path patternsPath) throws IOException {
        contentPatternsMap = ContentPatterns.fromFile(patternsPath);
    }

    public void add(WebCorpus corpus) {
        ContentPatterns patterns = contentPatternsMap.get(corpus.source);

        for (WebDocument document : corpus.getPages()) {

            if (patterns != null) {
                patterns.applyReplacePatterns(document);
            } else {
                Log.info("No content patterns for [%s]", corpus.source);
            }

            for (String line : document.lines) {
                if (line.trim().length() == 0) {
                    continue;
                }
                String processed = process(line);
                if (processed.length() == 0) {
                    continue;
                }
                long hash1 = hash1(processed);
                long hash2 = hash2(processed);
                map1.increment(hash1);
                map2.increment(hash2);
            }
        }
    }

    private long hash2(String line) {
        return Hashing.murmur3_128(0Xdeadbeef).hashBytes(line.getBytes()).asLong();
    }

    private long hash1(String line) {
        return Hashing.murmur3_128().hashBytes(line.getBytes()).asLong();
    }

    public WebCorpus reduce(WebCorpus corpus) {
        ContentPatterns patterns = contentPatternsMap.get(corpus.source);

        List<WebDocument> reducedDocs = new ArrayList<>(corpus.documentCount());
        for (WebDocument document : corpus.getPages()) {

            if (patterns != null) {
                patterns.applyReplacePatterns(document);
            } else {
                Log.info("No content patterns for [%s]", corpus.source);
            }

            List<String> reducedLines = new ArrayList<>();
            for (String line : document.lines) {
                if (line.trim().length() == 0) {
                    continue;
                }
                String processed = process(line);
                if (processed.length() == 0) {
                    continue;
                }
                long hash1 = hash1(processed);
                long hash2 = hash2(processed);

                int count1 = map1.get(hash1);
                int count2 = map2.get(hash2);

                int minCount = determineAmount(processed);

                // duplicated.
                if (count1 > minCount && count2 > minCount) {
                    map1.decrement(hash1);
                    map1.decrement(hash2);
                } else {
                    reducedLines.add(line);
                }
            }
            if (reducedLines.size() > 0) {
                WebDocument doc = document.copy(reducedLines);
                reducedDocs.add(doc);
            }
        }
        return new WebCorpus(corpus.source, corpus.id, reducedDocs);
    }

    private int determineAmount(String line) {
        if (line.length() > 200) {
            return 1;
        }
        if (line.length() > 100) {
            return 2;
        } else if (line.length() > 50) {
            return 3;
        } else return 4;
    }

    private String process(String line) {
        return line.toLowerCase(locale).replaceAll("[^a-zçğıöşü]", "");
    }

    public static void main(String[] args) throws IOException {

        Path corporaRoot = Paths.get("/media/aaa/Data/corpora/news");
        Path outRoot = Paths.get("/media/aaa/Data/corpora/noDup");
        Files.createDirectories(outRoot);

        DuplicateLineRemover remover = new DuplicateLineRemover(Paths.get("content-rules.txt"));

        List<Path> corpusRoots = Lists.newArrayList(Files.walk(corporaRoot, 1)
                .filter(path -> path.toFile().isDirectory() && !path.equals(corporaRoot)).iterator());

        for (Path corpusRoot : corpusRoots) {

            List<Path> files = Lists.newArrayList(Files.walk(corpusRoot, 1)
                    .filter(path -> path.toFile().isFile()).iterator());
            for (Path file : files) {
                Log.info("Loading %s", file);
                WebCorpus corpus = new WebCorpus(corpusRoot.toFile().getName(), file.toFile().getName());
                corpus.addDocuments(WebCorpus.loadDocuments(file));
                Log.info("Processing %s", corpus);
                remover.add(corpus);
            }
        }

        Log.info("Total key count map 1 = %d", remover.map1.size());
        int totalMultiple = 0;
        int[] vals = remover.map1.copyOfValues();
        for (int val : vals) {
            if (val > 1) {
                totalMultiple++;
            }
        }
        Log.info("Total keys with value >1  = %d", totalMultiple);

        for (Path corpusRoot : corpusRoots) {
            List<Path> files = Lists.newArrayList(Files.walk(corpusRoot, 1)
                    .filter(path -> path.toFile().isFile()).iterator());

            Path outDir = outRoot.resolve(corpusRoot.toFile().getName());
            Files.createDirectories(outDir);

            for (Path file : files) {
                Log.info("Loading %s", file);
                WebCorpus corpus = new WebCorpus(corpusRoot.toFile().getName(), file.toFile().getName());
                corpus.addDocuments(WebCorpus.loadDocuments(file));
                Log.info("Reducing %s", corpus);
                WebCorpus reduced = remover.reduce(corpus);
                Log.info("Before lines = %d, After lines = %d", corpus.totalPageLineCount(), reduced.totalPageLineCount());
                Log.info("Saving %s", corpus);
                reduced.save(outDir.resolve(file.toFile().getName()), false);
            }
        }

    }


}
