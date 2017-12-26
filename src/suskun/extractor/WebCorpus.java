package suskun.extractor;

import com.google.common.hash.Hashing;
import zemberek.core.logging.Log;
import zemberek.core.text.TextConsumer;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class WebCorpus {

    String source;
    String id;

    private List<WebDocument> pages = new ArrayList<>();
    private Map<String, WebDocument> lookup = new HashMap<>();

    public WebCorpus(String source, String id, List<WebDocument> pages) {
        this.source = source;
        this.id = id;
        this.pages = pages;
        for (WebDocument page : pages) {
            lookup.put(page.getId(), page);
        }
    }

    //TODO: this may be lossy.
    public WebCorpus copyNoDuplicates() {
        Set<Long> hashes = new HashSet<>();
        WebCorpus noDup = new WebCorpus(this.source, this.id);
        for (WebDocument doc : pages) {
            if (hashes.contains(doc.getHash())) {
                continue;
            }
            if (doc.contentLength() < 50) {
                continue;
            }
            hashes.add(doc.getHash());
            noDup.addDocument(doc);
        }
        return noDup;
    }

    public WebDocument getDocument(String id) {
        return lookup.get(id);
    }

    public WebCorpus(String source, String id) {
        this.source = source;
        this.id = id;
    }

    public String getSource() {
        return source;
    }

    public String getId() {
        return id;
    }

    public List<WebDocument> getPages() {
        return pages;
    }

    public void addDocuments(Collection<WebDocument> documents) {
        for (WebDocument document : documents) {
            addDocument(document);
        }
    }

    public void addDocument(WebDocument document) {
        pages.add(document);
        lookup.put(document.getId(), document);
    }

    public void removeAll() {
        pages.clear();
        lookup.clear();
    }


    public static List<WebDocument> loadDocuments(Path corpusFile) throws IOException {
        List<String> allLines = Files.readAllLines(corpusFile, StandardCharsets.UTF_8);

        List<WebDocument> pages = new ArrayList<>(allLines.size() / 10);

        TextConsumer textConsumer = new TextConsumer(allLines);
        textConsumer.moveUntil(s -> s.startsWith("<doc id="));
        while (!textConsumer.finished()) {
            String meta = textConsumer.current();
            textConsumer.advance();
            List<String> pageData = textConsumer.moveUntil(s -> s.startsWith("</doc>"));
            textConsumer.moveUntil(s -> s.startsWith("<doc"));
            WebDocument e = WebDocument.fromText(meta, pageData);
            if (e != null) {
                pages.add(e);
            }
        }
        return pages;
    }

    public int documentCount() {
        return pages.size();
    }

    @Override
    public String toString() {
        return source + "-" + id;
    }

    public int sizeUnique() {
        return pages.stream()
                .map(WebDocument::getHash)
                .collect(Collectors.toSet())
                .size();
    }

    public int totalPageLineCount() {
        int total = 0;
        for (WebDocument page : pages) {
            for (String line : page.lines) {
                if (line.length() == 0)
                    continue;
                total++;
            }
        }
        return total;
    }

    public int uniquePageLineCount() {

        Set<Long> hashes = new HashSet<>(100000);
        for (WebDocument page : pages) {
            for (String line : page.lines) {
                hashes.add(Hashing.murmur3_128().hashUnencodedChars(line).asLong());
            }
        }
        return hashes.size();
    }

    public void saveToDir(Path outRoot, boolean onlyContent) throws IOException {

        Path subDir = outRoot.resolve(source);
        Files.createDirectories(subDir);
        save(subDir.resolve(id), onlyContent);
    }

    public void save(Path outFile, boolean onlyContent) throws IOException {

        try (PrintWriter p = new PrintWriter(outFile.toFile(), "utf-8")) {
            for (WebDocument page : pages) {
                if (!onlyContent) {
                    p.println(page.getDocumentHeader());
                }
                p.println(page.getContentAsString());
                if (!onlyContent) {
                    p.println("</doc>");
                }
            }
        }
    }


    public List<WebDocument> getReducedPages(ContentPatterns patterns, boolean removeDuplicatedLines) {
        // remove duplicated lines.
        List<WebDocument> uniqueLinePages = new ArrayList<>();

        Set<Long> lineHashes = new HashSet<>(100000);
        int all = 0;
        int unique = 0;

        for (WebDocument page : this.pages) {
            List<String> uniqueLines = new ArrayList<>();
            all += page.lines.size();
            for (String l : page.lines) {
                l = l.trim().replaceAll("\\s+", " ");
                if (l.length() == 0)
                    continue;
                // eliminate single words.
                if (!l.contains(" "))
                    continue;
                long h = Hashing.murmur3_128().hashUnencodedChars(l).asLong();
                if (!lineHashes.contains(h) || !removeDuplicatedLines) {
                    lineHashes.add(h);
                    uniqueLines.add(l);
                }
            }

            unique += uniqueLines.size();
            uniqueLinePages.add(page.copy(uniqueLines));
        }
        Log.info("Before unique line = %d  After = %d", all, unique);

        List<WebDocument> reducedPages;

        if (patterns != null) {
            reducedPages = new ArrayList<>();
            for (WebDocument p : uniqueLinePages) {
                WebDocument r = patterns.reduce(p, removeDuplicatedLines);
                if (r.lines.isEmpty())
                    continue;
                reducedPages.add(r);
            }
        } else {
            reducedPages = new ArrayList<>(uniqueLinePages);
        }
        if (reducedPages.size() == 0) {
            Log.warn("No page with content left!");
        } else {
            Log.info("Total pages to save %d", reducedPages.size());
        }
        return reducedPages;
    }
}


