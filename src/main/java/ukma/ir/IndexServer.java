package ukma.ir;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.sun.istack.internal.NotNull;
import edu.stanford.nlp.process.Morphology;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;


public class IndexServer {
    private static final int WORKERS = 4;
    private static final String TEMP_PARTICLES = "data/dictionary/dp%d.txt";
    private static final String TEMP_PHRASE_PARTICLES = "data/dictionary/phdp%d.txt";
    private static final String TERM_INDEX_FLECKS = "data/dictionary/indexFleck_%d.txt";
    private static final String PHRASE_INDEX_FLECKS = "data/dictionary/phraseIndexFleck_%d.txt";
    private static final long WORKERS_MEMORY_LIMIT = Math.round(Runtime.getRuntime().maxMemory() * 0.5);
    private static final long MAX_MEMORY_LOAD = Math.round(Runtime.getRuntime().maxMemory() * 0.7);
    private static final String DP_SEPARATOR = " : "; // DOC_POSTING_SEPARATOR

    private Path LIBRARY = Paths.get("G:\\project\\library\\custom");
    private static final IndexServer INSTANCE = new IndexServer();
    private final CountDownLatch completion = new CountDownLatch(WORKERS);

    private int numStoredDictionaries;
    private int numStoredPhraseDicts;
    //    might be useful for debug
    private int numFlecks;
    private int numPhraseFlecks;

    // path - docId
    private final BiMap<Path, Integer> docId = HashBiMap.create();
    private final Map<String, Integer> termPostingPath = new HashMap<>(); // term - number of index file
    private final Map<String, Integer> phrasePostingPath = new HashMap<>();
    // building-time dictionaries
    // TODO: use ternary search tree
    private TreeMap<String, Set<Integer>> dictionary;
    private TreeMap<String, Set<Integer>> phraseDict;
    private static final String FUNC_WORDS_PATH = "models/functional_words.txt";
    private static final Set<String> funcWords;

    static {
        funcWords = parseFuncSet();
    }

    private static final Morphology MORPH = new Morphology();

    private IndexServer() {
    }

    // there is no need to have multiple instances of this class as it is not supposed to be stored in collections
    // creating multiple instances and running them simultaneously may use more threads than expected
    // it can be refactored to take an array of strings if the files are spread across multiple files
    public static IndexServer getInstance() {
        return INSTANCE;
    }

    public enum IndexType {
        TERM, PHRASE, COORDINATE
    }

    public boolean containsElement(String term, IndexType type) {
        switch (type) {
            case TERM:
                return termPostingPath.containsKey(term);
            case PHRASE:
                return phrasePostingPath.containsKey(term);
            case COORDINATE:
                throw new UnsupportedOperationException("need to implement");
            default:
                throw new IllegalArgumentException("incorrect type");
        }
    }

    /**
     * @param term element to search for
     * @param type type of index to search in
     * @return posting or null if no such element found
     */
    public ArrayList<Integer> getPostings(String term, IndexType type) {
        String path;
        switch (type) {
            case TERM:
                path = String.format(TERM_INDEX_FLECKS, termPostingPath.get(term));
                break;
            case PHRASE:
                path = String.format(PHRASE_INDEX_FLECKS, phrasePostingPath.get(term));
                break;
            case COORDINATE:
                throw new UnsupportedOperationException("need to implement");
            default:
                throw new IllegalArgumentException("incorrect type");
        }

        ArrayList<Integer> pList = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(new File(path)))) {
            String search = br.readLine();
            while (search != null) {
                if (search.startsWith(term)) {
                    String postings = search.substring(search.indexOf(DP_SEPARATOR) + DP_SEPARATOR.length());
                    for (String docID : postings.split("\\s+"))
                        pList.add(Integer.parseInt(docID));
                    return pList;
                }
                search = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return pList;
    }

    public String getDocName(int docID) {
        return docId.inverse().get(docID).getFileName().toString();
    }

    public Path getDocPath(int docID) {
        return docId.inverse().get(docID);
    }

    public void buildInvertedIndex() {
        long startTime = System.nanoTime();
        dictionary = new TreeMap<>();
        phraseDict = new TreeMap<>();

        try (Stream<Path> files = Files.walk(LIBRARY)) {
            File[] documents = files.filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .peek(doc -> docId.put(doc.toPath(), docId.size()))
                    .toArray(File[]::new);
            // inefficient: MaxentTagger tagger = new MaxentTagger("models/english-left3words-distsim.tagger");

            for (int i = 0; i < WORKERS; i++) {
                int from = (int) Math.round((double) documents.length / WORKERS * i);
                int to = (int) Math.round((double) documents.length / WORKERS * (i + 1));
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
        private final long WORKER_MEMORY = Math.round(WORKERS_MEMORY_LIMIT * (0.95 + Math.random() * 10));
        private final File[] files;

        FileProcessor(File[] docs) {
            files = docs;
        }

        @Override
        public void run() {
            Morphology morph = new Morphology();
            Set<String> vocabulary;
            Set<String> phrases;
            for (File docFile : files) {
                vocabulary = new TreeSet<>();
                phrases = new TreeSet<>();
                int docID = docId.get(docFile.toPath());
                TermProvider terms = new TermProvider(docFile, morph);
                // check func words from set, construct set
                String firstWord = null;
                boolean hasFuncWords = false;

                while (terms.hasNextTerm()) {
                    String term = terms.nextTerm();
                    vocabulary.add(term);
                    if (firstWord == null && !isFuncWord(term)) firstWord = term;
                    else if (isFuncWord(term)) hasFuncWords = true;
                    else { // secondWord == null && !funcWords.contains(term)
                        if (hasFuncWords) phrases.add("* " + firstWord + " " + term);
                        else phrases.add(firstWord + " " + term);
                        firstWord = null;
                        hasFuncWords = false;
                    }

                    Runtime rt = Runtime.getRuntime();
                    if (rt.maxMemory() - rt.freeMemory() > WORKER_MEMORY) {
                        merge(new OutEntry(docID, vocabulary.toArray(new String[0]), phrases.toArray(new String[0])));
                        vocabulary = new TreeSet<>();
                        phrases = new TreeSet<>();
                    }
                }
                // send to merge after processing anyway
                merge(new OutEntry(docID, vocabulary.toArray(new String[0]), phrases.toArray(new String[0])));
            }
            completion.countDown();
        }
    }

    /**
     * takes normalized word as a parameter
     */
    public static boolean isFuncWord(String word) {
        return funcWords.contains(word);
    }

    private class TermProvider {
        private BufferedReader br;
        private String[] lineTokens;
        private int nextTokenIndex;
        private Morphology morph;

        TermProvider(File f, Morphology m) {
            morph = m;
            try {
                br = new BufferedReader(new FileReader(f));
            } catch (FileNotFoundException e) {
                throw new IllegalArgumentException("incorrect path to file", e);
            }
        }

        String nextTerm() {
            if (!hasNextTerm()) throw new NoSuchElementException("no more tokes left in this file");
            return lineTokens[nextTokenIndex++];
        }

        boolean hasNextTerm() {
            if (lineTokens == null || nextTokenIndex == lineTokens.length) {
                try {
                    String nextLine = br.readLine();
                    if (nextLine == null) return false;
                    lineTokens = nextLine.split("\\s+");
                    for (int i = 0; i < lineTokens.length; i++)
                        lineTokens[i] = normalize(lineTokens[i], morph);
                    nextTokenIndex = 0;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            while (nextTokenIndex < lineTokens.length) {
                if (lineTokens[nextTokenIndex] != null) return true;
                nextTokenIndex++;
            }
            return hasNextTerm();
        }

        @Override
        public void finalize() {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * @param token char sequence split by whitespace
     * @return normalized term or null if the passed token is not a word
     */
    public @Nullable
    static String normalize(String token) {
        return normalize(token, MORPH);
    }

    static String normalize(String token, Morphology m) {
        return m.stem(token.toLowerCase().replaceAll("\\W", ""));
    }

    /**
     * merges small local dictionaries into a big global
     *
     * @param microMap - particle of dictionary built during a processing period
     */
    private synchronized void merge(OutEntry microMap) {
        replenishDictionary(dictionary, microMap.getVocabulary(), microMap.getDocId());
        replenishDictionary(phraseDict, microMap.getPhrases(), microMap.getDocId());

        Runtime rt = Runtime.getRuntime();
        if (rt.maxMemory() - rt.freeMemory() > MAX_MEMORY_LOAD) {
            saveParticles();
        }
    }

    private void replenishDictionary(Map<String, Set<Integer>> dictionary, String[] target, int docID) {
        for (String element : target) {
            Set<Integer> posting = dictionary.get(element);
            if (posting != null) posting.add(docID);
            else {
                posting = new TreeSet<>();
                posting.add(docID);
                dictionary.put(element, posting);
            }
        }
    }

/*    private String showMemory() {
        Runtime rt = Runtime.getRuntime();
        return String.format("FREE memory: %.2f%%", (double) rt.freeMemory() / rt.maxMemory() * 100);
    }*/

    private void saveParticles() {
        String pathNameDict = String.format(TEMP_PARTICLES, numStoredDictionaries++);
        String pathNamePhraseDict = String.format(TEMP_PHRASE_PARTICLES, numStoredPhraseDicts++);
        write(pathNameDict, dictionary);
        write(pathNamePhraseDict, phraseDict);
        dictionary = new TreeMap<>();
        phraseDict = new TreeMap<>();
        System.gc();
    }

    private void write(String path, TreeMap<String, Set<Integer>> dictionary) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(path)))) {
            for (Map.Entry<String, Set<Integer>> entry : dictionary.entrySet()) {
                bw.write(entry.getKey() + DP_SEPARATOR + entry.getValue().toString());
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void mergeParticles() throws IOException {
        saveParticles();
        // open reader for each particle and maintain priority queue for each particle on disk
        PriorityQueue<InEntry> entries = new PriorityQueue<>();
        BufferedReader[] readers = new BufferedReader[numStoredDictionaries];
        initReaders(entries, readers, numStoredDictionaries, TEMP_PARTICLES);
        numFlecks = abstractMerge(entries, readers, TERM_INDEX_FLECKS, termPostingPath, TEMP_PARTICLES);

        // repeat for phrase index
        entries = new PriorityQueue<>();
        readers = new BufferedReader[numStoredPhraseDicts];
        initReaders(entries, readers, numStoredPhraseDicts, TEMP_PHRASE_PARTICLES);
        numPhraseFlecks = abstractMerge(entries, readers, PHRASE_INDEX_FLECKS, phrasePostingPath, TEMP_PHRASE_PARTICLES);
    }

    private void initReaders(PriorityQueue<InEntry> entries, BufferedReader[] readers,
                             int numStored, String particlesPath) throws IOException {
        String checkLine;
        for (int i = 0; i < numStored; i++) {
            String pathName = String.format(particlesPath, i);
            readers[i] = new BufferedReader(new FileReader(new File(pathName)));
            checkLine = readers[i].readLine();
            if (checkLine != null) entries.add(new InEntry(i, checkLine));
        }
    }

    private int abstractMerge(PriorityQueue<InEntry> entries, BufferedReader[] readers, String outPath,
                              Map<String, Integer> postingPath, String primaryPath) throws IOException {
        int numWritten = 0;
        // initialize writer for first dictionary fleck
        String pathName = String.format(outPath, numWritten);
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(pathName)));

        while (true) {
            if (entries.isEmpty()) {
                bw.flush();
                bw.close();
                break;
            }

            InEntry nextEntry = entries.poll();
            StringBuilder posting = new StringBuilder();
            appendPosting(entries, readers, nextEntry, posting, primaryPath);
            postingPath.put(nextEntry.getTerm(), numWritten);
            // check '!entries.isEmpty()' eliminates NullPointerException
            // find and process current term in all files
            while (!entries.isEmpty() && nextEntry.getTerm().equals(entries.peek().getTerm()))
                appendPosting(entries, readers, entries.poll(), posting, primaryPath);

            bw.write(nextEntry.getTerm() + DP_SEPARATOR + posting.toString()
                    .replaceAll("[\\[\\]]", " ").replaceAll(",", "").trim());
            bw.newLine();
            Runtime rt = Runtime.getRuntime();
            if (rt.maxMemory() - rt.freeMemory() > MAX_MEMORY_LOAD) {
                bw.flush();
                bw.close();
                pathName = String.format(outPath, ++numWritten);
                bw = new BufferedWriter(new FileWriter(new File(pathName)));
                System.gc();
            }
        }
        return numWritten;
    }

    private void appendPosting(PriorityQueue<InEntry> entries, BufferedReader[] readers,
                               InEntry nextEntry, StringBuilder posting, String primaryPath) throws IOException {
        posting.append(nextEntry.getPostings());
        String newLine = readers[nextEntry.getIndex()].readLine();
        if (newLine == null) {
            readers[nextEntry.getIndex()].close(); //delete file
           //Files.delete(Paths.get(String.format(primaryPath, nextEntry.getIndex())));
        } else entries.add(new InEntry(nextEntry.getIndex(), newLine));
    }

    private class OutEntry {
        private final Integer docID;
        private final String[] vocabulary;
        private final String[] phrases;

        OutEntry(Integer id, String[] v, String[] ph) {
            if (id == null) throw new IllegalArgumentException("null id is not permitted");
            docID = id;
            vocabulary = v;
            phrases = ph;
        }

        int getDocId() {
            return docID;
        }

        String[] getVocabulary() {
            return vocabulary;
        }

        String[] getPhrases() {
            return phrases;
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
        public int compareTo(@NotNull InEntry entry) {
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

    private static Set<String> parseFuncSet() {
        Set<String> func = new HashSet<>();
        try (BufferedReader br = new BufferedReader(new FileReader(
                new File(Thread.currentThread().getContextClassLoader().getResource(FUNC_WORDS_PATH).getPath())))) {
            String word = br.readLine();
            while (word != null) {
                func.add(word);
                word = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return func;
    }
}
