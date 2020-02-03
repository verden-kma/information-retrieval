package ukma.ir;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import edu.stanford.nlp.simple.Sentence;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class ProLibrarian {
    // too much time waiting for:
    // 1. too many threads -> free memory to return Future,
    // 2. too few -> next Future to merge it
    private static final int THREADS = 2;
    private static final Path LIBRARY = Paths.get("infoSearch/data/library/custom");
    private static final String TEMP_PARTICLES = "infoSearch/data/dictionary/dp%d.txt";
    private static final String INDEX_FLECKS = "infoSearch/data/dictionary/indexFleck_%d.txt";
    // restricts the amount of memory Future`s take
    private static final double MEMORY_LIMIT = 0.6 * Runtime.getRuntime().maxMemory();

    private static ProLibrarian instance;
    private int freeID;
    private int numStoredDictionaries;
    private int numFlecks;
    private volatile BiMap<String, Integer> docId = HashBiMap.create();

    private final Object lock = new Object();
    private volatile boolean lackMemory;
    private volatile AtomicLong currentlyRead = new AtomicLong();


    private volatile AtomicInteger numDoneFutures = new AtomicInteger();

    private TreeMap<String, ArrayList<Integer>> dictionary;

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
        dictionary = new TreeMap<>();
        ExecutorService es = Executors.newFixedThreadPool(THREADS); // , new ThreadFactoryBuilder().setNameFormat("executor-thread-%d").build()
        ExecutorCompletionService<OutEntry<Integer, String[]>> workers = new ExecutorCompletionService<>(es);

        try (Stream<Path> files = Files.walk(LIBRARY)) {
            long n = files.filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .peek(doc -> docId.put(doc.getName(), freeID++))
                    .map(FileProcessor::new)
                    .map(workers::submit)
                    .count();

            //add overlaps every sqrt(n)
            for (long i = 0; i < n; i++)
                merge(workers.take().get());
//            System.out.println("START MERGING PARTICLES");
//            mergeParticles();
//            System.out.println("Merged Particles");
            System.out.println("FINISH");
            System.out.println(showMemory());
        } catch (Exception e) {
            e.printStackTrace();
        }
        es.shutdown();
        long endTime = System.nanoTime();
        System.out.println("multi time: " + (endTime - startTime) / 1e9);
    }

    static int debugCounter = 1;

    private class FileProcessor implements Callable<OutEntry<Integer, String[]>> {
        private final File file;

        FileProcessor(File file) {
            this.file = file;
        }

        @Override
        public OutEntry<Integer, String[]> call() {
            Runtime rt = Runtime.getRuntime();
            synchronized (lock) {
                if (!lackMemory) lackMemory = rt.maxMemory() - rt.freeMemory() + file.length()*2 + currentlyRead.get() > MEMORY_LIMIT;
                if (lackMemory) System.out.println("MEMORY LIMIT");
                while (lackMemory) {
                    try {
                        System.out.println("start WAIT");
                        lock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            currentlyRead.set(file.length());
            StringBuilder fs = new StringBuilder();
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                br.lines().forEach(x -> {
                    fs.append(x);
                    fs.append('\n');
                });
                //expensive lemmatization for production
//                Document doc = new Document(fs.toString());
//                String[] lemmas = doc.sentences().stream()
//                        .map(Sentence::lemmas)
//                        .flatMap(List::stream)
//                        .distinct()
//                        .toArray(String[]::new);
                String[] lemmas = Arrays.stream(fs.toString().split("\\s+"))
                        .filter(x -> !x.matches("\\s+|\\W+|(</?\\w>)+"))
                        .map(s -> s.replaceAll("\\W", ""))
                        .map(String::toLowerCase)
                        .distinct()
                        .toArray(String[]::new);

                System.out.println("file processed: " + debugCounter++);
                showMemory();

                currentlyRead.set(currentlyRead.get() - file.length());
                numDoneFutures.incrementAndGet();
                return new OutEntry<>(docId.get(file.getName()), lemmas);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }


    static int mergeDebug = 1;
    /**
     * merges Future values into a dictionary piece
     *
     * @param microMap - particle of dictionary built from a single file
     */
    private void merge(OutEntry<Integer, String[]> microMap) {
        // TODO: handle situation when microMap is stored 2 times: as it is and as lemmas in ArrayLists in dictionary
        for (String lemma : microMap.getValue()) {
            ArrayList<Integer> posting = dictionary.get(lemma);
            if (posting != null) posting.add(microMap.getKey());
            else {
                posting = new ArrayList<>();
                posting.add(microMap.getKey());
                dictionary.put(lemma, posting);
            }
        }
        /* debug code */
        if (lackMemory)
            System.out.println("Lack merge " + mergeDebug++);
        else
            System.out.println("Merge " + mergeDebug++);
        /*end debug code*/

        Runtime rt = Runtime.getRuntime();
        // I/O tasks (and lemmatization) performed by workers take more time than merge operation,\
        // so starting them when there is only 1 left is too late to avoid waiting of current 'merger' thread
        // the idea is to make workers process as many files as they can before memory runs out
        // and to make them wait for as long as there is job for merger in order to minimize time for synchronization
        // at this point program is supposed to work at high but stable memory usage, when all nearly Futures processed
        // into a dictionary, save the dictionary, free a large part of the memory and allow workers to proceed
        if (numDoneFutures.decrementAndGet() <= 2 && lackMemory) {
            System.out.println("need to save");
            saveParticle();
            synchronized (lock) {
                lackMemory = false;
                lock.notifyAll();
            }
        }
        System.out.println("Reminder after merge: " + numDoneFutures.get());
    }

    private String showMemory() {
        Runtime rt = Runtime.getRuntime();
        return String.format("FREE memory: %.2f%%", (double) rt.freeMemory() / rt.maxMemory() * 100);
    }

    private void saveParticle() {
        System.out.println("start to save");
        String pathName = String.format(TEMP_PARTICLES, numStoredDictionaries++);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(pathName)))) {
            for (Map.Entry<String, ArrayList<Integer>> entry : dictionary.entrySet()) {
                bw.write(entry.getKey() + " : " + entry.getValue().toString());
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        dictionary = new TreeMap<>();
        System.out.println("Particle saved! -> " + showMemory());
    }

    // TODO: test
    private void mergeParticles() throws IOException {
        saveParticle();
// open reader for each particle and maintain priority queue for each particle on disk
        PriorityQueue<InEntry> entries = new PriorityQueue<>();
        BufferedReader[] readers = new BufferedReader[numStoredDictionaries];
        String checkLine;
        for (int i = 0; i < numStoredDictionaries; i++) {
            String pathName = String.format(TEMP_PARTICLES, i);
            readers[i] = new BufferedReader(new FileReader(new File(pathName)));
            checkLine = readers[i].readLine();
            if (checkLine != null) entries.add(new InEntry(i, checkLine));
        }

        // initialize writer for first dictionary fleck
        String pathName = String.format(INDEX_FLECKS, numFlecks++);
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(pathName)));

        while (true) {
            InEntry nextEntry = entries.poll();
            if (nextEntry == null) {
                bw.flush();
                bw.close();
                break;
            }

            StringBuilder posting = new StringBuilder();
            appendPosting(entries, readers, nextEntry, posting);
// check '!entries.isEmpty()' eliminates NullPointerException
            // find and process current term in all files
            while (!entries.isEmpty()
                    && nextEntry.getKey().equals(entries.peek().getKey())) {
                InEntry mergeEntry = entries.poll();
                appendPosting(entries, readers, mergeEntry, posting);
            }
            bw.write(nextEntry.getKey() + " : " + posting.toString().replaceAll("[\\[\\]]", ""));
            bw.newLine();
            Runtime rt = Runtime.getRuntime();
            if (rt.freeMemory() < MEMORY_LIMIT) {
                bw.flush();
                bw.close();
                pathName = String.format(INDEX_FLECKS, numFlecks++);
                bw = new BufferedWriter(new FileWriter(new File(pathName)));
            }
        }
    }

    private void appendPosting(PriorityQueue<InEntry> entries, BufferedReader[] readers,
                               InEntry nextEntry, StringBuilder posting) throws IOException {
        posting.append(nextEntry.getValue());
        String newLine = readers[nextEntry.getIndex()].readLine();
        if (newLine == null) readers[nextEntry.getIndex()].close();
        else entries.add(new InEntry(nextEntry.getIndex(), newLine));
    }

    /**
     * NEEDS EDITING!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
     *
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
                            excludeTerms.add(new Sentence(token.substring(3)).lemma(0));
                        else includeTerms.add(new Sentence(token).lemma(0));

                    // if there are terms to include and one of these terms is in dictionary (in this case - 0th),
                    // i.e. input is not empty
                    // then set posting for this term as a base for further intersections
                    // else do no intersection as the result is empty if any entry is empty
                    Set<Integer> inTerms = new TreeSet<>();
                    if (!includeTerms.isEmpty() && dictionary.containsKey(includeTerms.get(0))) {
                        inTerms.addAll(dictionary.get(includeTerms.get(0))); // set base
                        includeTerms.forEach(term -> {
                            ArrayList<Integer> posting = dictionary.get(term);
                            if (posting != null) inTerms.retainAll(posting); // intersect
                        });
                    }

                    Set<Integer> exTerms = new TreeSet<>();
                    for (String term : excludeTerms) {
                        ArrayList<Integer> posting = dictionary.get(term);
                        if (posting != null) exTerms.addAll(posting);
                    }

                    inTerms.removeAll(exTerms);
                    reminders.addAll(inTerms);
                });

        List<String> result = new ArrayList<>(reminders.size());
        reminders.forEach(id -> result.add(docId.inverse().get(id)));
        return result;
    }

    // generics used to remind about types and add flexibility to refactor
    private class OutEntry<K, V> {
        private final K key;
        private final V value;

        OutEntry(K key, V value) {
            if (key == null) throw new IllegalArgumentException("null key is not permitted");
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }

    class InEntry implements Comparable<InEntry> {
        private int index;
        private String key;
        private String value;

        public InEntry(int index, String value) {
            this.index = index;
            int separator = value.indexOf(" : ");
            this.key = value.substring(0, separator);
            this.value = value.substring(separator + 3);
        }

        @Override
        public int compareTo(InEntry entry) {
            return key.compareTo(entry.key);
        }

        public int getIndex() {
            return index;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }
    }

//    /**
//     * tries to serialize dictionary on disk,
//     * @return true if succeed, false otherwise
//     */
//    public boolean saveIndex() {
//        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(INDEX.toString() + "/p1.bin"))) {
//            oos.writeObject(dictionary);
//        } catch (IOException e) {
//            e.printStackTrace();
//            return false;
//        }
//        return true;
//    }

//    /**
//     * tries to deserialize dictionary on disk,
//     * @return true if succeed, false otherwise
//     */
//    public boolean loadIndex() {
//        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(INDEX.toString() + "/p1.bin"))) {
//            dictionary = (TreeMap<String, TreeSet<Integer>>) ois.readObject();
//        } catch (IOException e) {
//            e.printStackTrace();
//            System.out.println("IO related issues");
//            return false;
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//            System.out.println("Class related issues");
//            return false;
//        }
//        return true;
//    }

//    public TreeMap<String, ArrayList<String>> getIndex() {
//        if (dictionary == null) throw new IllegalArgumentException("no dictionary built yet");
//        TreeMap<String, ArrayList<String>> result = new TreeMap<>();
//        for (Map.InEntry<String, ArrayList<Integer>> entry : dictionary.entrySet()) {
//            ArrayList<String> restoredPosting = new ArrayList<>();
//            for (Integer id : entry.getValue())
//                restoredPosting.add(docId.inverse().get(id));
//            result.put(entry.getKey(), restoredPosting);
//        }
//        return result;
//    }
}
