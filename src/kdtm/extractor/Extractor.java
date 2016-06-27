package kdtm.extractor;


import de.l3s.boilerpipe.extractors.ArticleExtractor;
import de.l3s.boilerpipe.extractors.KeepEverythingExtractor;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class Extractor {

    public static void main(String[] args) throws IOException, InterruptedException {
        //TODO: fill below
        Path inRoot = Paths.get("/home/sila/projects/Extractor/root");
        Path outRoot = Paths.get("/home/sila/projects/Extractor/outExtractor");


        ThreadPoolExecutor es = new ThreadPoolExecutor(3, 3, 0L, TimeUnit.MILLISECONDS, new LimitedQueue<>(3));

        Files.walkFileTree(inRoot, new FileVisitor<Path>() {

            private final Pattern DATE = Pattern.compile("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]");

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                if (DATE.matcher(dir.getFileName().toString()).matches()) {
                    Path relative = inRoot.relativize(dir);
                    String rel1 = relative.toString().substring(0, relative.toString().indexOf("data"));
                    String rel2 = relative.toString().substring(relative.toString().indexOf("/data")+6);
                    Path outFile = outRoot.resolve(rel1+rel2);
                    if (Files.notExists(outFile)) {
                        es.submit(new ExtractorTask(dir, outFile, "All"));
                    }
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                return FileVisitResult.CONTINUE;
            }
        });
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
                        if(extractType.equals("Art")) {
                            text = ArticleExtractor.INSTANCE.getText(text);
                        } else if(extractType.equals("All")) {
                            text = KeepEverythingExtractor.INSTANCE.getText(text);
                        }
//                        text = DefaultExtractor.INSTANCE.getText(text);

                        mbw.write("<doc id=\"" + inFile.getFileName().toString().replace("%3A",":").replace("%2F","/") + "\" source=\""+  inDir.toString().substring(inDir.toString().indexOf("root/")+5,inDir.toString().indexOf("/data")) + "\" crawl-date=\"" +outFile.toString().substring(outFile.toString().length()-10)+"\">");
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
                System.out.println("completed : " + inDir + " " + count + " files");
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
