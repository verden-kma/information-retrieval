package ukma.ir;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class ProLibrarian {
    private static ProLibrarian instance;

    private ProLibrarian() {
    }

    private String readPath, writePath;

    private final Object lock = new Object();
    private TreeMap<String, TreeSet<String>> dictionary;
    private ExecutorService workers;

    // there is no need to have multiple instances of this class as it is not supposed to be stored in collections
    // creating multiple instances and running them simultaneously may use more threads than expected
    // it can be refactored to take an array of strings if the files are spread across multiple files
    public static ProLibrarian getInstance(String readPath, String writePath) {
        if (instance == null) {
            instance = new ProLibrarian();
            instance.readPath = readPath;
            instance.writePath = writePath;
        }
        return instance;
    }

    public void makeDictionary() {
        long startTime = System.nanoTime();
        dictionary = new TreeMap<>();
        workers = Executors.newFixedThreadPool(4);

        try (Stream<Path> files = Files.walk(Paths.get(readPath))) {
            files.filter(Files::isRegularFile)
                    .map(path -> workers.submit(new FileProcessor(path)))
                    .forEach(instance::merge);
        } catch (IOException e) {
            e.printStackTrace();
        }
        workers.shutdown();
        long endTime = System.nanoTime();
        System.out.println("multi time: " + (endTime - startTime) / 1e9);
    }

    private void merge(Future<TreeMap<String, TreeSet<String>>> futureMicroMap) {
        try {
            TreeMap<String, TreeSet<String>> microMap = futureMicroMap.get();
            synchronized (lock) {
                for (Map.Entry<String, TreeSet<String>> entry : microMap.entrySet()) {
                    dictionary.merge(entry.getKey(), entry.getValue(),
                            (oldVal, newVal) -> {
                                oldVal.addAll(newVal);
                                return oldVal;
                            });
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println("merged");
    }

    private static class FileProcessor implements Callable<TreeMap<String, TreeSet<String>>> {
        private TreeMap<String, TreeSet<String>> map = new TreeMap<>();

        private FileProcessor(Path path) {
            final String fileName = path.toFile().getName();
            try (BufferedReader br = new BufferedReader(new FileReader(path.toFile()))) {
                br.lines()
                        .map(line -> line.split("\\s"))
                        .flatMap(Arrays::stream)
                        .map(token -> token.replaceAll("(<\\w+>)|[.,!?:]+", ""))
                        .filter(token -> !token.isEmpty())
                        .map(String::toLowerCase)
                        .forEach(word -> {
                            if (map.get(word) == null) {
                                TreeSet<String> posting = new TreeSet<>();
                                posting.add(fileName);
                                map.put(word, posting);
                            } else {
                                map.get(word).add(fileName);
                            }
                        });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public TreeMap<String, TreeSet<String>> call() {
            return map;
        }
    }

    /**
     * tries to serialize dictionary on disk,
     *
     * @return true if succeed, false otherwise
     */
    public boolean writeDictionary() {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(writePath))) {
            oos.writeObject(dictionary);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean loadDictionary(String dictionaryPath) {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(dictionaryPath))) {
            dictionary = (TreeMap<String, TreeSet<String>>) ois.readObject();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("IO related issues");
            return false;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("Class related issues");
            return false;
        }
        return true;
    }

    public TreeMap<String, TreeSet> getDictionary() {
        if (dictionary == null) throw new IllegalArgumentException ("no dictionary built yet");
        return new TreeMap<>(dictionary);
    }

    public String getReadPath() {
        return readPath;
    }

    public void setReadPath(String readPath) {
        this.readPath = readPath;
    }

    public String getWritePath() {
        return writePath;
    }

    public void setWritePath(String writePath) {
        this.writePath = writePath;
    }
}