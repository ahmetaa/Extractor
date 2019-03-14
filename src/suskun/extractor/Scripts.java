package suskun.extractor;

import com.google.common.collect.Lists;
import zemberek.core.logging.Log;
import zemberek.core.text.TextIO;
import zemberek.core.text.TextUtil;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Scripts {

    public static void main(String[] args) throws IOException {

        Path inRoot = Paths.get("/media/aaa/Data/corpora/reduced");
        Path outRoot = Paths.get("/media/aaa/Data/corpora/final");
        Path sourcesList = Paths.get("test");

        Files.createDirectories(outRoot);
        Set<Path> sourceSet = loadSourcePaths(inRoot, sourcesList);

        for (Path sourceDir : sourceSet) {

            Path outDir = outRoot.resolve(sourceDir.toFile().getName());
            Files.createDirectories(outDir);

            Log.info("Processing %s", sourceDir);
            List<Path> files = Lists.newArrayList(Files.walk(sourceDir, 1)
                    .filter(path -> path.toFile().isFile()).iterator());
            for (Path file : files) {
                String name = file.toFile().getName();
                if (name.endsWith("corpus.corpus")) {
                    name = name.replaceAll("[.]corpus$", "");
                } else if (!name.endsWith(".corpus")) {
                    name = name + ".corpus";
                }
                Path target = outDir.resolve(name);
                if (target.toFile().exists()) {
                    Log.info("%s already exists. Skipping.", target);
                    continue;
                }
                Log.info("Copying %s to %s", file, target);
                Files.copy(file, target);
            }
        }

    }

    public static Set<Path> loadSourcePaths(Path root, Path sourcesFilePath) throws IOException {
        return TextIO.loadLines(sourcesFilePath)
                .stream()
                .filter(s -> !s.trim().startsWith("#") && s.trim().length() > 0)
                .map((s) -> {
                    try {
                        if (!s.startsWith("http")) {
                            s = ("http://" + s);
                        }
                        URI uri = new URI(s);
                        Log.info(uri.getHost());
                        return root.resolve(uri.getHost());
                    } catch (Exception exception) {
                        exception.printStackTrace();
                        System.exit(-1);
                        return null;
                    }
                }).collect(Collectors.toSet());
    }

}
