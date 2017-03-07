package suskun.extractor;


import com.google.common.base.Splitter;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import de.l3s.boilerpipe.extractors.KeepEverythingExtractor;
import zemberek.core.text.Regexps;
import zemberek.core.text.TextUtil;

import java.io.*;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Extractor {

    private Map<String, ContentPatterns> patterns;

    public Extractor() throws IOException {
        patterns = ContentPatterns.fromFile(Paths.get("content-rules.txt"));
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        //TODO: fill below
        Path inRoot = Paths.get("/media/data/crawl/news");
        Path outRoot = Paths.get("/media/data/corpora/raw3");
        Extractor e = new Extractor();
        e.extract(inRoot, outRoot, Pattern.compile("t24"));
        // e.extract(inRoot, outRoot);
    }

    private void extract(final Path inRoot, final Path outRoot, Pattern pattern) throws IOException, InterruptedException {
        // TODO: system should process files multithreaded, not directories.
        ThreadPoolExecutor es = new ThreadPoolExecutor(
                10,
                10,
                0L,
                TimeUnit.MILLISECONDS,
                new LimitedQueue<>(10));

        List<Path> sourcePaths = Files.walk(inRoot, 1).filter(
                s -> s.toFile().isDirectory() && (pattern == null || Regexps.matchesAny(pattern, s.toFile().getName()))
        ).collect(Collectors.toList());

        final Pattern DATE = Pattern.compile("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]");

        for (Path sourceDir : sourcePaths) {
            String source = sourceDir.toFile().getName();

            Path data = sourceDir.resolve("data");
            if (!data.toFile().exists())
                continue;
            List<Path> days = Files
                    .walk(data, 1)
                    .filter(s -> s.toFile().isDirectory())
                    .collect(Collectors.toList());
            for (Path day : days) {
                if (!DATE.matcher(day.toFile().getName()).matches())
                    continue;
                String extractString = null;
                if (patterns.containsKey(source)) {
                    extractString = patterns.get(source).extractor;
                }
                Path outDir = outRoot.resolve(sourceDir.toFile().getName());
                Files.createDirectories(outDir);
                Path outFile = outDir.resolve(day.toFile().getName());
                System.out.println("Processing " + day + " to " + outFile);
                if (Files.notExists(outFile)) {
                    es.submit(new ExtractorTask(day, outFile, extractString, patterns.get(source)));
                }
            }
        }

        es.shutdown();
        es.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

    }


    private void extract(final Path inRoot, final Path outRoot) throws IOException, InterruptedException {
        extract(inRoot, outRoot, null);
    }

    private static final class ExtractorTask implements Runnable {

        private static final Pattern pattern = Pattern.compile("<head>.*</head>", Pattern.DOTALL | Pattern.CASE_INSENSITIVE);

        final Path inDir;
        final Path outFile;
        String extractType;
        ContentPatterns patterns;

        ExtractorTask(Path inDir, Path outFile, String extractType, ContentPatterns patterns) {
            this.inDir = inDir;
            this.outFile = outFile;
            this.extractType = extractType;
            this.patterns = patterns;
        }

        @Override
        public void run() {

            Path tmp = Paths.get(outFile.toString() + ".tmp");
            if (Files.exists(tmp)) {
                try {
                    Files.delete(tmp);
                } catch (IOException ex) {
                    System.err.println(ex.toString());
                    return;
                }
            }

            if (extractType == null) {
                extractType = "ARTICLE";
            }

            try (OutputStream os = Files.newOutputStream(tmp);
                 DirectoryStream<Path> ds = Files.newDirectoryStream(inDir)) {
                int count = 0;
                for (Path inFile : ds) {
                    try {
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        BufferedWriter mbw = new BufferedWriter(new OutputStreamWriter(baos, Charset.forName("UTF-8")));
                        String text = new String(Files.readAllBytes(inFile), Charset.forName("UTF-8"));

                        List<String> labels = patterns.labelPattern != null ?
                                extractLabels(text, patterns.labelPattern) : Collections.emptyList();
                        String category = patterns.categoryPattern != null ?
                                extractCategory(text, patterns.categoryPattern) : "";
                        String title = patterns.titlePattern != null ?
                                extractTitle(text, patterns.titlePattern) : "";

                        text = pattern.matcher(text).replaceAll("<head><meta charset=\"UTF-8\"></head>");
                        if (extractType == null || extractType.equals("ARTICLE")) {
                            text = ArticleExtractor.INSTANCE.getText(text);
                        } else if (extractType.equals("EVERYTHING")) {
                            text = KeepEverythingExtractor.INSTANCE.getText(text);
                        }
                        String id = URLDecoder.decode(inFile.toFile().getName(), "utf-8");
                        String source = inDir.getParent().getParent().toFile().getName();
                        String crawlDate = inDir.toFile().getName();
                        mbw.write("<doc id=\"" + id
                                + "\" source=\"" + source
                                + "\" title=\"" + title
                                + "\" labels=\"" + String.join(",", labels)
                                + "\" category=\"" + category
                                + "\" crawl-date=\"" + crawlDate + "\">");
                        mbw.newLine();
                        mbw.write(text);
                        mbw.write("</doc>");
                        mbw.newLine();
                        mbw.newLine();
                        mbw.close();
                        os.write(baos.toByteArray());
                        count++;
                    } catch (Exception aex) {
                        aex.printStackTrace();
                        System.err.println("Exception in file " + inFile);
                    }
                }
                Files.move(tmp, outFile);
                System.out.println("completed : " + inDir + " " + count + " files" + "   extract : " + extractType);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        static Pattern labelSplitPattern = Pattern.compile("<.+?>");

        private List<String> extractLabels(String text, Pattern pattern) {
            String labelChunk = Regexps.firstMatch(pattern, text, 2);
            if (labelChunk == null || labelChunk.trim().length() == 0) {
                return Collections.emptyList();
            }
            labelChunk = TextUtil.convertAmpresandStrings(labelChunk);
            List<String> labels;
            if (labelChunk.contains("<")) {
                labels = Splitter.on(labelSplitPattern).omitEmptyStrings().trimResults().splitToList(labelChunk);
            } else {
                labels = Splitter.on(",").trimResults().splitToList(labelChunk);
            }
            return labels;
        }

        private String extractCategory(String text, Pattern pattern) {
            String category = Regexps.firstMatch(pattern, text, 2);
            if (category == null) {
                return "";
            }
            return TextUtil.convertAmpresandStrings(category);
        }

        private String extractTitle(String text, Pattern pattern) {
            String title = Regexps.firstMatch(pattern, text, 2);
            if (title == null) {
                return "";
            }
            return TextUtil.convertAmpresandStrings(title);
        }
    }

    private static final class LimitedQueue<E> extends LinkedBlockingQueue<E> {

        public LimitedQueue(int maxSize) {
            super(maxSize);
        }

        @Override
        public boolean offer(E e) {
            // turn offer() and add() into a blocking calls (unless interrupted)
            try {
                put(e);
                return true;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            return false;
        }

    }
}
