package ukma.ir;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

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

public class ProLibrarian implements Serializable {
    private static ProLibrarian instance;
    private volatile BiMap<String, Integer> docId = HashBiMap.create();

    private transient String readPath, writePath;

    private transient final Object lock = new Object();
    private TreeMap<String, TreeSet<Integer>> dictionary;
    private transient ExecutorService workers; // refactor so that finished Futures are passed first

    private ProLibrarian() {
    }

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
        workers = Executors.newFixedThreadPool(1);

        try (Stream<Path> files = Files.walk(Paths.get(readPath))) {
            files.filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .map(doc -> {
                        docId.put(doc.getName(), docId.size());
                        return doc;
                    })
                    .map(doc -> workers.submit(new FileProcessor(doc)))
                    .forEach(instance::merge);
        } catch (IOException e) {
            e.printStackTrace();
        }
        workers.shutdown(); // fix concurrency
        long endTime = System.nanoTime();
        System.out.println("multi time: " + (endTime - startTime) / 1e9);
    }

    private void merge(Future<TreeMap<String, TreeSet<Integer>>> futureMicroMap) {
        try {
            TreeMap<String, TreeSet<Integer>> microMap = futureMicroMap.get();
            synchronized (lock) {
                for (Map.Entry<String, TreeSet<Integer>> entry : microMap.entrySet()) {
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

    private class FileProcessor implements Callable<TreeMap<String, TreeSet<Integer>>> {
        private TreeMap<String, TreeSet<Integer>> map = new TreeMap<>();

        private FileProcessor(File file) {
            final String fileName = file.getName();
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                br.lines()
                        .map(line -> line.split("\\s"))
                        .flatMap(Arrays::stream)
                        .map(token -> token.replaceAll("(\\W+)|(<\\w+(\\\\)?>)", ""))
                        .filter(token -> !token.isEmpty())
                        .map(String::toLowerCase)
                        .forEach(word -> {
                            // no need for synchronization as all threads only read from docId
                            if (map.get(word) == null) {
                                TreeSet<Integer> posting = new TreeSet<>();
                                posting.add(docId.get(fileName));
                                map.put(word, posting);
                            } else {
                                map.get(word).add(docId.get(fileName));
                            }
                        });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public TreeMap<String, TreeSet<Integer>> call() {
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
            dictionary = (TreeMap<String, TreeSet<Integer>>) ois.readObject();
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

    public TreeMap<String, TreeSet<String>> getDictionary() {
        if (dictionary == null) throw new IllegalArgumentException("no dictionary built yet");
        TreeMap<String, TreeSet<String>> result = new TreeMap<>();
        for (Map.Entry<String, TreeSet<Integer>> entry : dictionary.entrySet()) {
            TreeSet<String> restoredPosting = new TreeSet<>();
            for (Integer id : entry.getValue())
                restoredPosting.add(docId.inverse().get(id));
            result.put(entry.getKey(), restoredPosting);
        }
        return result;
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
