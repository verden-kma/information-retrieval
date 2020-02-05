package ukma.ir;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import edu.stanford.nlp.process.Morphology;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;


public class ProLibrarian {
    private static final int WORKERS = 3;
    private static final Path LIBRARY = Paths.get("data/library/custom");
    private static final String TEMP_PARTICLES = "data/dictionary/dp%d.txt";
    private static final String INDEX_FLECKS = "data/dictionary/indexFleck_%d.txt";
    private static final long WORKERS_MEMORY_LIMIT = Math.round(Runtime.getRuntime().maxMemory() * 0.5);
    private static final long MAX_MEMORY_LOAD = Math.round(Runtime.getRuntime().maxMemory() * 0.7);
    private static final String DP_SEPARATOR = " : "; // DOC_POSTING_SEPARATOR

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
                new Thread(new FileProcessor(Arrays.copyOfRange(documents, from, to))).start();
            }
            completion.await();
            mergeParticles();

        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.nanoTime();
        System.out.println("multi time: " + (endTime - startTime) / 1e9);
    }

    private class FileProcessor implements Runnable {
        // random is used to prevent situation when all threads want to merge simultaneously
        private final long WORKER_MEMORY = Math.round(WORKERS_MEMORY_LIMIT * (0.95 + Math.random()/10));
        private final File[] files;

        FileProcessor(File[] docs) {
            this.files = docs;
        }

        @Override
        public void run() {
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
                            merge(new OutEntry(docID, vocabulary.toArray(new String[0])));
                            vocabulary = new TreeSet<>();
                        }
                        nextLine = br.readLine();
                    }
                    // send to merge after processing anyway
                    merge(new OutEntry(docId.get(docFile.getName()), vocabulary.toArray(new String[0])));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            completion.countDown();
        }
    }

    /**
     * merges small local dictionaries into a big global
     *
     * @param microMap - particle of dictionary built during a processing period
     */
    private synchronized void merge(OutEntry microMap) {
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
            saveParticle();
        }
    }

/*    private String showMemory() {
        Runtime rt = Runtime.getRuntime();
        return String.format("FREE memory: %.2f%%", (double) rt.freeMemory() / rt.maxMemory() * 100);
    }*/

    private void saveParticle() {
        String pathName = String.format(TEMP_PARTICLES, numStoredDictionaries++);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(pathName)))) {
            for (Map.Entry<String, Set<Integer>> entry : dictionary.entrySet()) {
                bw.write(entry.getKey() + DP_SEPARATOR + entry.getValue().toString());
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        dictionary = new TreeMap<>();
        System.gc();
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

            bw.write(nextEntry.getTerm() + DP_SEPARATOR + posting.toString()
                    .replaceAll("[\\[\\]]", " ").replaceAll(",", "").trim());
            bw.newLine();
            Runtime rt = Runtime.getRuntime();
            if (rt.maxMemory() - rt.freeMemory() > MAX_MEMORY_LOAD) {
                bw.flush();
                bw.close();
                pathName = String.format(INDEX_FLECKS, numFlecks++);
                bw = new BufferedWriter(new FileWriter(new File(pathName)));
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

    private class OutEntry {
        private final Integer docID;
        private final String[] vocabulary;

        OutEntry(Integer key, String[] value) {
            if (key == null) throw new IllegalArgumentException("null key is not permitted");
            docID = key;
            vocabulary = value;
        }

        public int getKey() {
            return docID;
        }

        public String[] getValue() {
            return vocabulary;
        }
    }

    class InEntry implements Comparable<InEntry> {
        private int index; // index of a particle file
        private String term;
        private String postings;

        InEntry(int index, String value) {
            this.index = index;
            int separator = value.indexOf(DP_SEPARATOR);
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
}
