package kdtm.extractor;


import de.l3s.boilerpipe.extractors.ArticleExtractor;
import de.l3s.boilerpipe.extractors.KeepEverythingExtractor;
import sun.rmi.runtime.Log;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Extractor {

    Map<String, ContentPatterns> patterns;

    public Extractor() throws IOException {
        patterns = ContentPatterns.fromFile(Paths.get("content-rules.txt"));
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        //TODO: fill below
        Path inRoot = Paths.get("/home/sila/projects/Extractor/root");
        Path outRoot = Paths.get("/home/sila/projects/Extractor/outExtractor");
        Extractor e = new Extractor();

        e.extract(inRoot, outRoot);
    }

    private void extract(final Path inRoot, final Path outRoot) throws IOException, InterruptedException {
        ThreadPoolExecutor es = new ThreadPoolExecutor(3, 3, 0L, TimeUnit.MILLISECONDS, new LimitedQueue<>(3));

        List<Path> sourcePaths = Files.walk(inRoot, 1).filter(s -> s.toFile().isDirectory()).collect(Collectors.toList());

        final Pattern DATE = Pattern.compile("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]");

        for (Path sourceDir : sourcePaths) {
            String source = sourceDir.toFile().getName();

            Path data = sourceDir.resolve("data");
            if (!data.toFile().exists())
                continue;
            List<Path> days = Files.walk(data, 1).filter(s -> s.toFile().isDirectory()).collect(Collectors.toList());
            for (Path day : days) {
                if (!DATE.matcher(day.toFile().getName()).matches())
                    continue;
                Path relative = inRoot.relativize(day);

                String extractString = null;
                if (patterns.containsKey(source)) {
                    extractString = patterns.get(source).extractor;
                }
                String rel1 = relative.toString().substring(0, relative.toString().indexOf("data"));
                String rel2 = relative.toString().substring(relative.toString().indexOf("/data") + 6);
                Path outFile = outRoot.resolve(rel1 + rel2);
                if (Files.notExists(outFile)) {
                    es.submit(new ExtractorTask(day, outFile, extractString));
                }
            }
        }

        es.shutdown();
        es.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }

    private static final class ExtractorTask implements Runnable {

        private static final Pattern pattern = Pattern.compile("<head>.*</head>", Pattern.DOTALL | Pattern.CASE_INSENSITIVE);

        final Path inDir;
        final Path outFile;
        final String extractType;

        ExtractorTask(Path inDir, Path outFile, String extractType) {
            this.inDir = inDir;
            this.outFile = outFile;
            this.extractType = extractType;

        }

        @Override
        public void run() {
            try {
                Files.createDirectories(outFile.getParent());
            } catch (IOException ex) {
                System.err.println(ex.toString());
                return;
            }
            Path tmp = Paths.get(outFile.toString() + ".tmp");
            if (Files.exists(tmp)) {
                try {
                    Files.delete(tmp);
                } catch (IOException ex) {
                    System.err.println(ex.toString());
                    return;
                }
            }
            try (OutputStream os = Files.newOutputStream(tmp);
                 DirectoryStream<Path> ds = Files.newDirectoryStream(inDir)) {
                int count = 0;
                for (Path inFile : ds) {
                    try {
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        BufferedWriter mbw = new BufferedWriter(new OutputStreamWriter(baos, Charset.forName("UTF-8")));
                        String text = new String(Files.readAllBytes(inFile), Charset.forName("UTF-8"));
                        text = pattern.matcher(text).replaceAll("<head><meta charset=\"UTF-8\"></head>");
                        if (extractType == null || extractType.equals("ARTICLE")) {
                            text = ArticleExtractor.INSTANCE.getText(text);
                        } else if (extractType.equals("EVERYTHING")) {
                            text = KeepEverythingExtractor.INSTANCE.getText(text);
                        }
//                        text = DefaultExtractor.INSTANCE.getText(text);

                        mbw.write("<doc id=\"" + inFile.getFileName().toString().replace("%3A", ":").replace("%2F", "/") + "\" source=\"" + inDir.toString().substring(inDir.toString().indexOf("root/") + 5, inDir.toString().indexOf("/data")) + "\" crawl-date=\"" + outFile.toString().substring(outFile.toString().length() - 10) + "\">");
                        mbw.newLine();
                        mbw.write(text);
                        mbw.write("</doc>");
                        mbw.newLine();
                        mbw.newLine();
                        mbw.close();
                        os.write(baos.toByteArray());
                        count++;
                    } catch (Exception aex) {
                        System.err.println("Exception in file " + inFile);
                    }
                }
                Files.move(tmp, outFile);
                System.out.println("completed : " + inDir + " " + count + " files" +"   extract : "+extractType );
            } catch (IOException ex) {
                ex.printStackTrace();
            }
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
