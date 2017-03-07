package suskun.extractor;


import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import de.l3s.boilerpipe.extractors.KeepEverythingExtractor;
import zemberek.core.concurrency.BlockingExecutor;
import zemberek.core.logging.Log;
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
import java.util.concurrent.*;
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
        //e.extract(inRoot, outRoot, Pattern.compile("t24"));
        e.extract(inRoot, outRoot);
    }

    private void extract(final Path inRoot, final Path outRoot, Pattern pattern) throws IOException, InterruptedException {

        ExecutorService es = new BlockingExecutor(20, 20);
        CompletionService<Path> service = new ExecutorCompletionService<>(es);

        List<Path> sourcePaths = Files.walk(inRoot, 1).filter(
                s -> s.toFile().isDirectory() && (pattern == null || Regexps.matchesAny(pattern, s.toFile().getName()))
        ).collect(Collectors.toList());

        final Pattern DATE = Pattern.compile("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]");
        int taskCounter = 0;

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
                Log.info("Processing " + day + " to " + outFile);

                if (Files.notExists(outFile)) {
                    service.submit(new ExtractorTask(day, outFile, extractString, patterns.get(source)));
                    taskCounter++;
                }
            }
        }
        es.shutdown();
        try {
            List<Path> results = new ArrayList<>();
            while (results.size() < taskCounter) {
                Path e = service.take().get();
                if (e != null) {
                    results.add(e);
                } else {
                    taskCounter--;
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException("An error occurred during recognition", e);
        }

    }

    private void extract(final Path inRoot, final Path outRoot) throws IOException, InterruptedException {
        extract(inRoot, outRoot, null);
    }

    static class FileContent {
        Path path;
        String content;

        public FileContent(Path path, String content) {
            this.path = path;
            this.content = content;
        }
    }

    private static final class ExtractorTask implements Callable<Path> {

        private static final Pattern pattern =
                Pattern.compile("<head>.*</head>", Pattern.DOTALL | Pattern.CASE_INSENSITIVE);

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
        public Path call() {

            if (extractType == null) {
                extractType = "ARTICLE";
            }

            // load all paths in the directory.
            List<Path> paths = new ArrayList<>(5000);
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(inDir)) {
                for (Path inFile : ds) {
                    if (inFile.toFile().isDirectory()) {
                        Log.warn("%s is a directory. Ignoring.");
                        continue;
                    }
                    paths.add(inFile);
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println(e.toString());
                return null;
            }
            Log.info("There are %d files to process in %s ", paths.size(), inDir);


            // generate a temporary file.
            Path tmp = Paths.get(outFile.toString() + ".tmp");
            if (Files.exists(tmp)) {
                try {
                    Files.delete(tmp);
                } catch (IOException ex) {
                    ex.printStackTrace();
                    System.err.println(ex.toString());
                    return null;
                }
            }

            try (PrintWriter pw = new PrintWriter(
                    new BufferedOutputStream(new FileOutputStream(tmp.toFile()), 10_000_000))) {

                int count = 0;
                int start = 0, end = 1000;
                if (paths.size() < end) {
                    end = paths.size();
                }
                Stopwatch sw = Stopwatch.createStarted();
                while (start < paths.size()) {

                    List<Path> block = paths.subList(start, end);
                    start = end;
                    end += 1000;
                    if (paths.size() < end) {
                        end = paths.size();
                    }
                    List<FileContent> contents = new ArrayList<>();
                    for (Path path : block) {
                        String content = new String(Files.readAllBytes(path), Charset.forName("UTF-8"));
                        contents.add(new FileContent(path, content));
                    }
                    Log.info("Processing %d file in %s", block.size(), inDir);

                    for (FileContent fileContent : contents) {
                        String text = fileContent.content;
                        Path inFile = fileContent.path;
                        try {
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
                            pw.println("<doc id=\"" + TextUtil.escapeQuotesApostrpohes(id)
                                    + "\" source=\"" + TextUtil.escapeQuotesApostrpohes(source)
                                    + "\" title=\"" + TextUtil.escapeQuotesApostrpohes(title)
                                    + "\" labels=\"" + TextUtil.escapeQuotesApostrpohes(String.join(",", labels))
                                    + "\" category=\"" + TextUtil.escapeQuotesApostrpohes(category)
                                    + "\" crawl-date=\"" + crawlDate + "\">");
                            pw.println(text.trim());
                            pw.println("</doc>");
                            count++;
                        } catch (Exception aex) {
                            aex.printStackTrace();
                            System.err.println("Exception in file " + inFile);
                        }
                    }
                }
                Files.move(tmp, outFile);
                Log.info("%s completed in %.1f seconds. There are %d files. %s used.",
                        inDir,
                        sw.elapsed(TimeUnit.MILLISECONDS) / 1000f,
                        count,
                        extractType);

            } catch (IOException ex) {
                ex.printStackTrace();
            }
            return outFile;
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
