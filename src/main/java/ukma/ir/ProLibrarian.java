package ukma.ir;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

public class ProLibrarian {
    private static final Path LIBRARY = Paths.get("infoSearch/data/library");
    private static final Path INDEX = Paths.get("infoSearch/data/index");

    private static ProLibrarian instance;
    private int freeID;
    private volatile BiMap<String, Integer> docId = HashBiMap.create();

    // private transient final Object lock = new Object();
    private TreeMap<String, TreeSet<Integer>> index;
    private ExecutorCompletionService<TreeMap<String, TreeSet<Integer>>> workers;

    private ProLibrarian() {
    }

    // there is no need to have multiple instances of this class as it is not supposed to be stored in collections
    // creating multiple instances and running them simultaneously may use more threads than expected
    // it can be refactored to take an array of strings if the files are spread across multiple files
    public static ProLibrarian getInstance() {
        if (instance == null)
            instance = new ProLibrarian();
        return instance;
    }

    public void buildInvertedIndex() {
        long startTime = System.nanoTime();
        index = new TreeMap<>();
        ExecutorService es = Executors.newFixedThreadPool(4);
        workers = new ExecutorCompletionService<>(es);

        try (Stream<Path> files = Files.walk(LIBRARY)) {
            long n = files.filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .peek(doc -> docId.put(doc.getName(), freeID++))
                    .map(FileProcessor::new)
                    .map(workers::submit)
                    .count();

            for (long i = 0; i < n; i++) {
                merge(workers.take().get());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        es.shutdown();
        long endTime = System.nanoTime();
        System.out.println("multi time: " + (endTime - startTime) / 1e9);
    }

    /**
     * merges small indices into a big index in ram
     * @param microMap - particle of index built from a single file
     */
    private void merge(TreeMap<String, TreeSet<Integer>> microMap) {
        for (Map.Entry<String, TreeSet<Integer>> entry : microMap.entrySet()) {
            index.merge(entry.getKey(), entry.getValue(),
                    (oldVal, newVal) -> {
                        oldVal.addAll(newVal);
                        return oldVal;
                    });
        }
    }

    private class FileProcessor implements Callable<TreeMap<String, TreeSet<Integer>>> {
        private final File file;
        private TreeMap<String, TreeSet<Integer>> map = new TreeMap<>();

        FileProcessor(File file) {
            this.file = file;
        }

        @Override
        public TreeMap<String, TreeSet<Integer>> call() {

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
            return map;
        }
    }

    /**
     * @param q - query to be processed
     * @return list of documents that match a given query
     */
    public List<String> processQuery(String q) {
        Set<Integer> reminders = new TreeSet<>();
        Arrays.stream(q.split("OR"))
                .map(union -> union.split("AND"))
        .forEach(unionParticle -> {
            List<String> includeTerms = new ArrayList<>();
            List<String> excludeTerms = new ArrayList<>();
            for (String token : unionParticle)
                if (token.startsWith("NOT"))
                    excludeTerms.add(token.toLowerCase().substring(3));
                else includeTerms.add(token.toLowerCase());

            // if there are terms to include and one of these terms is in index (in this case - 0th),
            // i.e. input is not empty
            // then set posting for this term as a base for further intersections
            // else do no intersection as the result is empty if any entry is empty
            Set<Integer> inTerms = new TreeSet<>();
            if (!includeTerms.isEmpty() && index.containsKey(includeTerms.get(0))) {
                inTerms.addAll(index.get(includeTerms.get(0))); // set base
                includeTerms.forEach(term -> {
                    Set<Integer> posting = index.get(term);
                    if (posting != null) inTerms.retainAll(posting); // intersect
                });
            }

            Set<Integer> exTerms = new TreeSet<>();
            for (String term : excludeTerms) {
                Set<Integer> posting = index.get(term);
                if (posting != null) exTerms.addAll(posting);
            }

            inTerms.removeAll(exTerms);
            reminders.addAll(inTerms);
        });

        List<String> result = new ArrayList<>(reminders.size());
        reminders.forEach(id -> result.add(docId.inverse().get(id)));
        return result;
    }

    /**
     * tries to serialize index on disk,
     * @return true if succeed, false otherwise
     */
    public boolean saveIndex() {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(INDEX.toString() + "/p1.bin"))) {
            oos.writeObject(index);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * tries to deserialize index on disk,
     * @return true if succeed, false otherwise
     */
    public boolean loadIndex() {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(INDEX.toString() + "/p1.bin"))) {
            index = (TreeMap<String, TreeSet<Integer>>) ois.readObject();
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

    public TreeMap<String, TreeSet<String>> getIndex() {
        if (index == null) throw new IllegalArgumentException("no index built yet");
        TreeMap<String, TreeSet<String>> result = new TreeMap<>();
        for (Map.Entry<String, TreeSet<Integer>> entry : index.entrySet()) {
            TreeSet<String> restoredPosting = new TreeSet<>();
            for (Integer id : entry.getValue())
                restoredPosting.add(docId.inverse().get(id));
            result.put(entry.getKey(), restoredPosting);
        }
        return result;
    }
}
