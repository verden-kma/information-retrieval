package ukma.ir;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.sun.istack.internal.NotNull;
import edu.stanford.nlp.process.Morphology;
import edu.stanford.nlp.simple.Sentence;

import javax.annotation.Nonnull;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import static java.lang.Thread.sleep;

public class ProLibrarian {
    // too much time waiting for:
    // 1. too many threads -> free memory to return Future,
    // 2. too few -> next Future to merge it
    private static final int WORKERS = 1;
    private static final Path LIBRARY = Paths.get("data/library/custom");
    private static final String TEMP_PARTICLES = "data/dictionary/dp%d.txt";
    private static final String INDEX_FLECKS = "data/dictionary/indexFleck_%d.txt";
    private static final long WORKERS_MEMORY_LIMIT = Math.round(Runtime.getRuntime().maxMemory() * 0.5);
    private static final long MAX_MEMORY_LOAD = Math.round(Runtime.getRuntime().maxMemory() * 0.7);

    private final CountDownLatch completion = new CountDownLatch(WORKERS);

    private static final ProLibrarian instance = new ProLibrarian();

    private int numStoredDictionaries;
    private int numFlecks;

    private int freeID;
    private volatile BiMap<String, Integer> docId = HashBiMap.create();

    // TODO: use ternary search tree
    private TreeMap<String, Set<Integer>> dictionary;

    private ProLibrarian() {
    }

    // there is no need to have multiple instances of this class as it is not supposed to be stored in collections
    // creating multiple instances and running them simultaneously may use more threads than expected
    // it can be refactored to take an array of strings if the files are spread across multiple files
    public static ProLibrarian getInstance() {
        return instance;
    }

    public void buildInvertedIndex() {
        long startTime = System.nanoTime();
        dictionary = new TreeMap<>();

        try (Stream<Path> files = Files.walk(LIBRARY)) {
            File[] documents = files.filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .peek(doc -> docId.put(doc.getName(), freeID++))
                    .toArray(File[]::new);

            for (int i = 0; i < WORKERS; i++) {
                int from = (int) Math.round((double)documents.length / WORKERS * i);
                int to = (int) Math.round((double)documents.length / WORKERS * (i + 1));
                showMemory();
                new Thread(new FileProcessor(Arrays.copyOfRange(documents, from, to))).start();
            }
            completion.await();
//            System.out.println("START MERGING PARTICLES");
            mergeParticles();
//            System.out.println("Merged Particles");
            System.out.println("FINISH");
            System.out.println(showMemory());
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.nanoTime();
        System.out.println("multi time: " + (endTime - startTime) / 1e9);
    }

    static int debugCounter = 1;

    private class FileProcessor implements Runnable {
        // random is used to prevent situation when all threads want to merge simultaneously
        private final long WORKER_MEMORY = Math.round(WORKERS_MEMORY_LIMIT * (0.95 + Math.random()/10));
        private final File[] files;

        FileProcessor(File[] docs) {
            this.files = docs;
            System.out.println("Worker memory: " + WORKER_MEMORY/1024/1024 + " MB");
        }

        @Override
        public void run() {
            System.out.println("start run");
            Morphology morph = new Morphology();
            TreeSet<String> vocabulary;
            for (File docFile : files) {
                vocabulary = new TreeSet<>();
                int docID = docId.get(docFile.getName());
                try (BufferedReader br = new BufferedReader(new FileReader(docFile))) {
                    String nextLine = br.readLine();
                    while (nextLine != null) {
                        for (String token : nextLine.split("\\s")) {
                            token = morph.stem(token.toLowerCase().replaceAll("\\W", ""));
                            if (token != null) vocabulary.add(token);
                        }
                        Runtime rt = Runtime.getRuntime();
                        if (rt.maxMemory() - rt.freeMemory() > WORKER_MEMORY) {
                            merge(new OutEntry<>(docID, vocabulary.toArray(new String[0])));
                            vocabulary = new TreeSet<>();
                        }
                        nextLine = br.readLine();
                    }
                    // send to merge after processing anyway
                    merge(new OutEntry<>(docId.get(docFile.getName()), vocabulary.toArray(new String[0])));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("Out of processing thread!");
            completion.countDown();
        }
    }


    static int mergeDebug = 1;

    /**
     * merges small local dictionaries into a big global
     *
     * @param microMap - particle of dictionary built during a processing period
     */
    private synchronized void merge(OutEntry<Integer, String[]> microMap) {
        System.out.println("MERGE");
        for (String term : microMap.getValue()) {
            Set<Integer> posting = dictionary.get(term);
            if (posting != null) posting.add(microMap.getKey());
            else {
                posting = new TreeSet<>();
                posting.add(microMap.getKey());
                dictionary.put(term, posting);
            }
        }

        Runtime rt = Runtime.getRuntime();
        if (rt.maxMemory() - rt.freeMemory() > MAX_MEMORY_LOAD) {
            System.out.println("Max memory: " + rt.maxMemory()/1024/1024 + " MB");
            System.out.println("Free: " + rt.freeMemory()/1024/1024 + " MB");
            System.out.println("Diff: " + (rt.maxMemory() - rt.freeMemory())/1024/1024 + " MB");
            System.out.println("Limit: " + WORKERS_MEMORY_LIMIT/1024/1024 + " MB");
            System.out.println("need to save");
            saveParticle();
        }
    }

    private String showMemory() {
        Runtime rt = Runtime.getRuntime();
        return String.format("FREE memory: %.2f%%", (double) rt.freeMemory() / rt.maxMemory() * 100);
    }

    private void saveParticle() {
        System.out.println("start to save");
        String pathName = String.format(TEMP_PARTICLES, numStoredDictionaries++);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(pathName)))) {
            for (Map.Entry<String, Set<Integer>> entry : dictionary.entrySet()) {
                bw.write(entry.getKey() + " : " + entry.getValue().toString());
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        dictionary = new TreeMap<>();
        System.gc();
        System.out.println("Particle saved! -> " + showMemory());
    }

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
            if (entries.isEmpty()) {
                bw.flush();
                bw.close();
                break;
            }

            InEntry nextEntry = entries.poll();
            StringBuilder posting = new StringBuilder();
            appendPosting(entries, readers, nextEntry, posting);
            // check '!entries.isEmpty()' eliminates NullPointerException
            // find and process current term in all files
            while (!entries.isEmpty() && nextEntry.getTerm().equals(entries.peek().getTerm()))
                appendPosting(entries, readers, entries.poll(), posting);

            bw.write(nextEntry.getTerm() + " : " + posting.toString().replaceAll("[\\[\\]]", " ").trim());
            bw.newLine();
            System.out.println("write index");
            Runtime rt = Runtime.getRuntime();
            if (rt.maxMemory() - rt.freeMemory() > MAX_MEMORY_LOAD) {
                bw.flush();
                bw.close();
                pathName = String.format(INDEX_FLECKS, numFlecks++);
                bw = new BufferedWriter(new FileWriter(new File(pathName)));
                System.out.println("new index file");
            }
        }
    }

    private void appendPosting(PriorityQueue<InEntry> entries, BufferedReader[] readers,
                               InEntry nextEntry, StringBuilder posting) throws IOException {
        posting.append(nextEntry.getPostings());
        String newLine = readers[nextEntry.getIndex()].readLine();
        if (newLine == null) {
            readers[nextEntry.getIndex()].close(); //delete file
            Files.delete(Paths.get(String.format(TEMP_PARTICLES, nextEntry.getIndex())));
        }
        else entries.add(new InEntry(nextEntry.getIndex(), newLine));
    }

//    /**
//     * NEEDS EDITING!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
//     *
//     * @param q - query to be processed
//     * @return list of documents that match a given query
//     */
//    public List<String> processQuery(String q) {
//        //TODO: add overlaps every sqrt(n)
//        Set<Integer> reminders = new TreeSet<>();
//        Arrays.stream(q.split("OR"))
//                .map(union -> union.split("AND"))
//                .forEach(unionParticle -> {
//                    List<String> includeTerms = new ArrayList<>();
//                    List<String> excludeTerms = new ArrayList<>();
//                    for (String token : unionParticle)
//                        if (token.startsWith("NOT"))
//                            excludeTerms.add(new Sentence(token.substring(3)).lemma(0));
//                        else includeTerms.add(new Sentence(token).lemma(0));
//
//                    // if there are terms to include and one of these terms is in dictionary (in this case - 0th),
//                    // i.e. input is not empty
//                    // then set posting for this term as a base for further intersections
//                    // else do no intersection as the result is empty if any entry is empty
//                    Set<Integer> inTerms = new TreeSet<>();
//                    if (!includeTerms.isEmpty() && dictionary.containsKey(includeTerms.get(0))) {
//                        inTerms.addAll(dictionary.get(includeTerms.get(0))); // set base
//                        includeTerms.forEach(term -> {
//                            ArrayList<Integer> posting = dictionary.get(term);
//                            if (posting != null) inTerms.retainAll(posting); // intersect
//                        });
//                    }
//
//                    Set<Integer> exTerms = new TreeSet<>();
//                    for (String term : excludeTerms) {
//                        ArrayList<Integer> posting = dictionary.get(term);
//                        if (posting != null) exTerms.addAll(posting);
//                    }
//
//                    inTerms.removeAll(exTerms);
//                    reminders.addAll(inTerms);
//                });
//
//        List<String> result = new ArrayList<>(reminders.size());
//        reminders.forEach(id -> result.add(docId.inverse().get(id)));
//        return result;
//    }

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
        private int index; // index of particle file
        private String term; // term
        private String postings; // postings

        public InEntry(int index, String value) {
            this.index = index;
            int separator = value.indexOf(" : ");
            term = value.substring(0, separator);
            postings = value.substring(separator + 3);
        }

        @Override
        public int compareTo(InEntry entry) {
            return term.compareTo(entry.term);
        }

        public int getIndex() {
            return index;
        }

        public String getTerm() {
            return term;
        }

        public String getPostings() {
            return postings;
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
