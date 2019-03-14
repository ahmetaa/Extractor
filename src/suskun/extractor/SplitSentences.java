package suskun.extractor;

import com.google.common.collect.Lists;
import zemberek.core.logging.Log;
import zemberek.tokenization.TurkishSentenceExtractor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

public class SplitSentences {

    public static void main(String[] args) throws IOException {

        Path sourceRoot = Paths.get("/media/aaa/Data/corpora/mt-corpora");
        Path outRoot = Paths.get("/media/aaa/Data/corpora/mt-sentences");
        splitAll(sourceRoot, outRoot);

    }

    private static void splitAll(Path sourceRoot, Path outRoot) throws IOException {
        List<Path> sourceDirs = Lists.newArrayList(Files.walk(sourceRoot, 1)
                .filter(path -> path.toFile().isDirectory() && !path.equals(sourceRoot)).iterator());
        for (Path sourceDir : sourceDirs) {
            Log.info("Processing %s", sourceDir);
            splitSingle(sourceDir, outRoot);
        }
    }


    public static void splitSingle(Path sourceRoot, Path outRoot) throws IOException {

        String name = sourceRoot.toFile().getName();

        List<Path> files = Lists.newArrayList(Files.walk(sourceRoot, 1)
                .filter(path -> path.toFile().isFile()).iterator());

        TurkishSentenceExtractor extractor = TurkishSentenceExtractor.DEFAULT;

        for (Path file : files) {
            Log.info("Processing %s", file);
            WebCorpus corpus = new WebCorpus(name, file.toFile().getName());
            Path dir = outRoot.resolve(corpus.source);
            Files.createDirectories(dir);
            Path out = dir.resolve(corpus.id);
            if (out.toFile().exists()) {
                Log.info("%s Exists. skipping.", out);
                continue;
            }
            List<WebDocument> documents = WebCorpus.loadDocuments(file);
            corpus.addDocuments(documents);

            LinkedHashSet<String> all = new LinkedHashSet<>();
            for (WebDocument document : documents) {
                for (String paragraph : document.lines) {
                    List<String> sentences = extractor.fromParagraph(paragraph);
                    sentences = sentences.stream().map(s->clean(s)).collect(Collectors.toList());
                    all.addAll(sentences.stream().filter(s -> !s.contains("\"")).collect(Collectors.toList()));
                }
            }

            Files.write(out, all, StandardCharsets.UTF_8);

        }
    }

    public static String clean(String sentence) {
        sentence = sentence.replaceAll("\\s+|\\u00a0+", " ");
        sentence = sentence.replaceAll("[\\u00ad]", "");
        return sentence.trim();
    }


}
