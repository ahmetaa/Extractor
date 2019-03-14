package suskun.extractor;

import com.google.common.collect.Lists;
import zemberek.core.logging.Log;
import zemberek.core.text.Regexps;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class PageWithUrlRemover {

    public static WebCorpus removePagesWithUnwantedUrls(WebCorpus corpus,  ContentPatterns patterns) {
        if (patterns == null) {
            Log.info("No content patterns for [%s]. Returning input without changing.", corpus.source);
            return corpus;
        }
        List<WebDocument> reducedDocs = new ArrayList<>(corpus.documentCount());

        for (WebDocument document : corpus.getPages()) {
            boolean ignore = false;
            for (Pattern p : patterns.getUrlRemovePatterns()) {
                if (Regexps.matchesAny(p, document.url)) {
                    ignore = true;
                    break;
                }
            }
            if(!ignore) {
                reducedDocs.add(document);
            }
        }
        return new WebCorpus(corpus.source, corpus.id, reducedDocs);
    }

    public static void main(String[] args) throws IOException {
        Path corporaRoot = Paths.get("/media/aaa/Data/corpora/forum-test");
        Path outRoot = Paths.get("/media/aaa/Data/corpora/forum");
        Files.createDirectories(outRoot);

        List<Path> corpusRoots = Lists.newArrayList(Files.walk(corporaRoot, 1)
                .filter(path -> path.toFile().isDirectory() && !path.equals(corporaRoot)).iterator());

        Path patternsPath = Paths.get("content-rules.txt");
        Map<String, ContentPatterns> contentPatternsMap = ContentPatterns.fromFile(patternsPath);

        for (Path corpusRoot : corpusRoots) {
            List<Path> files = Lists.newArrayList(Files.walk(corpusRoot, 1)
                    .filter(path -> path.toFile().isFile()).iterator());

            Path outDir = outRoot.resolve(corpusRoot.toFile().getName());
            Files.createDirectories(outDir);

            for (Path file : files) {
                Log.info("Loading %s", file);
                WebCorpus corpus = new WebCorpus(corpusRoot.toFile().getName(), file.toFile().getName());
                corpus.addDocuments(WebCorpus.loadDocuments(file));
                Log.info("Processing %s", corpus);
                WebCorpus reduced = removePagesWithUnwantedUrls(corpus, contentPatternsMap.get(corpus.source));
                Log.info("Saving %s", corpus);
                reduced.save(outDir.resolve(file.toFile().getName()), false);
            }
        }
    }
}
