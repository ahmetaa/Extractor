package suskun.extractor;


import com.google.common.base.Splitter;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import de.l3s.boilerpipe.extractors.KeepEverythingExtractor;
import zemberek.core.text.Regexps;

import java.io.*;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
        Path outRoot = Paths.get("/media/data/corpora/raw2");
        Extractor e = new Extractor();
        e.extract(inRoot, outRoot, Pattern.compile("cnn"));
        // e.extract(inRoot, outRoot);
    }

    private void extract(final Path inRoot, final Path outRoot, Pattern pattern) throws IOException, InterruptedException {
        // TODO: system should process files multithreaded, not directories.
        ThreadPoolExecutor es = new ThreadPoolExecutor(
                10,
                10,
                0L,
                TimeUnit.MILLISECONDS,
                new LimitedQueue<>(5));

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
                    es.submit(new ExtractorTask(day, outFile, extractString));
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

        ExtractorTask(Path inDir, Path outFile, String extractType) {
            this.inDir = inDir;
            this.outFile = outFile;
            this.extractType = extractType;
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

                        String fileName = inFile.toFile().getName();
                        // TODO: clean this hack.
                        List<String> labels = fileName.contains("cnnturk") ? extractLabels(text) : Collections.emptyList();
                        String category = fileName.contains("cnnturk") ? "" : extractCategory(text);

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


        static Pattern cnnLabelPattern = Pattern.compile("(<meta name=\"keywords\" content=\")(.+?)(\"[ ]?/>)");
        static Pattern cnnCategoryPattern = Pattern.compile(
                "(<!--Haber Üst Kısmı-->.+?title=\")(.+?)(\")");

        private List<String> extractLabels(String text) {
            String labels = Regexps.firstMatch(cnnLabelPattern, text, 2);
            if (labels == null || labels.contains("/") || labels.contains("<")) {
                return Collections.emptyList();
            }
            return Splitter.on(",").trimResults().splitToList(labels);
        }

        private String extractCategory(String text) {
            String category = Regexps.firstMatch(cnnCategoryPattern, text, 2);
            if (category == null) {
                return "";
            }
            return category;
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
