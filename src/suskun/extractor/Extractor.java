package suskun.extractor;


import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.kohlschutter.boilerpipe.extractors.ArticleExtractor;
import com.kohlschutter.boilerpipe.extractors.KeepEverythingExtractor;
import zemberek.core.concurrency.BlockingExecutor;
import zemberek.core.logging.Log;
import zemberek.core.text.Regexps;
import zemberek.core.text.TextUtil;

import java.io.*;
import java.net.URI;
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
    private Set<Path> sourcePaths = new HashSet<>();

    public Extractor() throws IOException {
        patterns = ContentPatterns.fromFile(Paths.get("content-rules.txt"));
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        Path inRoot = Paths.get("/media/aaa/Data/crawl/news");
        Path outRoot = Paths.get("/media/aaa/Data/corpora/news");
        Path sourcesList = Paths.get("news");

        extractAll(inRoot, outRoot, sourcesList);
/*
        Extractor extractor = new Extractor();
        extractor.extractFromSourceCrawls(
                inRoot.resolve("www.uludagsozluk.com"), outRoot, 4 );
                */
    }

    private static void extractAll(Path inRoot, Path outRoot, Path sourcesList) throws IOException, InterruptedException {
        Extractor e = new Extractor();
        List<Path> paths = TextUtil.loadLinesWithText(sourcesList)
                .stream()
                .filter(s -> !s.trim().startsWith("#") && s.trim().length() > 0)
                .map((s) -> {
                    try {
                        if (!s.startsWith("http")) {
                            s = ("http://" + s);
                        }
                        URI uri = new URI(s);
                        Log.info(uri.getHost());
                        return inRoot.resolve(uri.getHost());
                    } catch (Exception exception) {
                        exception.printStackTrace();
                        System.exit(-1);
                        return null;
                    }
                }).collect(Collectors.toList());
        e.sourcePaths = new LinkedHashSet<>(paths);
        e.extract(outRoot, 4);
    }

    static final Pattern DATE = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");

    private void extract(final Path outRoot, int threadCount)
            throws IOException, InterruptedException {

        for (Path sourceDir : sourcePaths) {
            Path data = sourceDir.resolve("data");
            if (!data.toFile().exists()) {
                continue;
            }
            extractFromSourceCrawls(sourceDir, outRoot, threadCount);
        }
    }


    private void extractFromSourceCrawls(final Path sourceCrawlRoot, final Path outRoot, int threadCount)
            throws IOException, InterruptedException {

        String sourceName = sourceCrawlRoot.toFile().getName();

        Path data = sourceCrawlRoot.resolve("data");

        List<Path> crawlDayFolders = Files
                .walk(data, 1)
                .filter(s -> s.toFile().isDirectory())
                .collect(Collectors.toList());

        Log.info("There are %d crawl day folders for %s", crawlDayFolders.size(), sourceName);

        ExecutorService es = new BlockingExecutor(threadCount, threadCount);
        CompletionService<Path> service = new ExecutorCompletionService<>(es);

        int taskCounter = 0;

        for (Path day : crawlDayFolders) {

            // make sure we are processing correct folders.
            if (!DATE.matcher(day.toFile().getName()).matches()) {
                Log.warn("Incorrect folder name: %s. A Date pattern is expected.", day);
                continue;
            }

            String extractString = null;

            if (patterns.containsKey(sourceName)) {
                extractString = patterns.get(sourceName).extractor;
            }

            Path outDir = outRoot.resolve(sourceName);

            Files.createDirectories(outDir);

            Path outFile = outDir.resolve(day.toFile().getName());

            Log.info("Processing " + day + " to " + outFile);

            if (Files.notExists(outFile)) {
                service.submit(new ExtractorTask(day, outFile, extractString, patterns.get(sourceName), 500));
                taskCounter++;
            } else {
                Log.warn("File %s exist, skipping.", outFile);
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


    static class FileContent {
        Path path;
        String content;

        public FileContent(Path path, String content) {
            this.path = path;
            this.content = content;
        }
    }

    static class ExtractData {
        String id;
        String source;
        String title;
        String category;
        String crawlDate;
        String text;
        List<String> labels = new ArrayList<>();
    }

    private static final class ExtractorTask implements Callable<Path> {

        private static final Pattern pattern =
                Pattern.compile("<head>.*</head>", Pattern.DOTALL | Pattern.CASE_INSENSITIVE);

        final Path inDir;
        final Path outFile;
        String extractType;
        ContentPatterns patterns;
        int blockSize;

        ExtractorTask(Path inDir, Path outFile, String extractType, ContentPatterns patterns, int blockSize) {
            this.inDir = inDir;
            this.outFile = outFile;
            this.extractType = extractType;
            this.patterns = patterns;
            this.blockSize = blockSize;
        }

        @Override
        public Path call() {

            if (extractType == null) {
                extractType = "ARTICLE";
            }

            // load all paths in the directory.
            List<Path> paths = getPaths();
            if (paths == null) return null;

            int count = 0;
            int start = 0, end = blockSize;
            if (paths.size() < end) {
                end = paths.size();
            }
            Stopwatch sw = Stopwatch.createStarted();

            List<ExtractData> extractDataList = new ArrayList<>(10000);
            while (start < paths.size()) {

                List<Path> block = paths.subList(start, end);
                start = end;
                end += blockSize;
                if (paths.size() < end) {
                    end = paths.size();
                }
                long st = sw.elapsed(TimeUnit.MILLISECONDS);
                List<FileContent> contents = new ArrayList<>();

                for (Path path : block) {
                    try {
                        String content = new String(Files.readAllBytes(path), Charset.forName("UTF-8"));
                        contents.add(new FileContent(path, content));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                Log.info("Loaded %d from %s in %.1f seconds. Processing. (ThreadID=%d)",
                        block.size(),
                        inDir,
                        (sw.elapsed(TimeUnit.MILLISECONDS) - st) / 1000f,
                        Thread.currentThread().getId());
                st = sw.elapsed(TimeUnit.MILLISECONDS);
                for (FileContent fileContent : contents) {
                    String text = fileContent.content;
                    Path inFile = fileContent.path;
                    try {
                        List<String> labels = (patterns != null && patterns.labelPattern != null) ?
                                extractLabels(text, patterns.labelPattern) : Collections.emptyList();
                        String category = (patterns != null && patterns.categoryPattern != null) ?
                                extractCategory(text, patterns.categoryPattern) : "";
                        String title = (patterns != null && patterns.titlePattern != null) ?
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
                        ExtractData data = new ExtractData();
                        data.id = id;
                        data.source = source;
                        data.category = category;
                        data.title = title;
                        data.labels = labels;
                        data.crawlDate = crawlDate;
                        data.text = text;
                        extractDataList.add(data);
                        count++;
                    } catch (Exception aex) {
                        aex.printStackTrace();
                        System.err.println("Exception in file " + inFile);
                    }
                }
                Log.info("Processed %d from %s in %.1f seconds. (ThreadID=%d)",
                        block.size(),
                        inDir,
                        (sw.elapsed(TimeUnit.MILLISECONDS) - st) / 1000f,
                        Thread.currentThread().getId());
            }

            Log.info("Saving to file %s", outFile);
            try (PrintWriter pw = new PrintWriter(
                    new BufferedOutputStream(new FileOutputStream(outFile.toFile()), 10_000_000))) {
                for (ExtractData data : extractDataList) {
                    pw.println("<doc id=\"" + TextUtil.escapeQuotesApostrpohes(data.id)
                            + "\" source=\"" + TextUtil.escapeQuotesApostrpohes(data.source)
                            + "\" title=\"" + TextUtil.escapeQuotesApostrpohes(data.title)
                            + "\" labels=\"" + TextUtil.escapeQuotesApostrpohes(String.join(",", data.labels))
                            + "\" category=\"" + TextUtil.escapeQuotesApostrpohes(data.category)
                            + "\" crawl-date=\"" + data.crawlDate + "\">");
                    pw.println(data.text.trim());
                    pw.println("</doc>");
                }
                Log.info("%s completed in %.1f seconds. There are %d files. %s used. (ThreadID=%d)",
                        inDir,
                        sw.elapsed(TimeUnit.MILLISECONDS) / 1000f,
                        count,
                        extractType,
                        Thread.currentThread().getId());

            } catch (IOException ex) {
                ex.printStackTrace();
            }
            return outFile;
        }

        private List<Path> getPaths() {
            List<Path> paths = new ArrayList<>(5000);
            int ignoredCount = 0;
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(inDir)) {
                for (Path inFile : ds) {
                    if (inFile.toFile().isDirectory()) {
                        Log.warn("%s is a directory. Ignoring.", inFile);
                        continue;
                    }
                    String url = inFile.toFile().getName();
                    try {
                        url = URLDecoder.decode(url, "UTF-8");
                    } catch (Exception e) {
                        Log.warn("Cannot decode URL %s ", url);
                        continue;
                    }
                    boolean ignore = false;
                    if (patterns != null) {
                        for (Pattern p : patterns.getUrlRemovePatterns()) {
                            if (Regexps.matchesAny(p, url)) {
                                ignore = true;
                                break;
                            }
                        }
                    }
                    if (!ignore) {
                        paths.add(inFile);
                    } else {
                        ignoredCount++;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println(e.toString());
                return null;
            }
            Log.info("There are %d files to process in %s. %d ignored. ", paths.size(), inDir, ignoredCount);
            return paths;
        }

        static Pattern labelSplitPattern = Pattern.compile("<.+?>|[,]");
        static Pattern labelSplitPatternHref = Pattern.compile("(?:<a href.+?>\\s?+)(.+?)(?:</a>)");

        private List<String> extractLabels(String text, Pattern pattern) {
            String labelChunk = Regexps.firstMatch(pattern, text, 2).replaceAll("[\n\r]+", " ");
            if (labelChunk == null || labelChunk.trim().length() == 0) {
                return Collections.emptyList();
            }
            labelChunk = TextUtil.convertAmpresandStrings(labelChunk);
            List<String> labels;
            if (labelChunk.contains("<a href")) {
                labels = Splitter.on(labelSplitPatternHref).omitEmptyStrings().trimResults().splitToList(labelChunk);
                labels = labels.stream().filter(s -> !s.trim().equals(",")).collect(Collectors.toList());
            } else if (labelChunk.contains("<")) {
                labels = Splitter.on(labelSplitPattern).omitEmptyStrings().trimResults().splitToList(labelChunk);
                labels = labels.stream().filter(s -> !s.trim().equals(",")).collect(Collectors.toList());
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
}
