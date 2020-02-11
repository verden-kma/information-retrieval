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
    private static final int WORKERS = 3;
    private Path LIBRARY = Paths.get("G:\\project\\library\\custom");
    private static final String FUNC_WORDS_PATH = "models/functional_words.txt";
    private static final String TEMP_PARTICLES = "data/dictionary/dp%d.txt";
    private static final String TEMP_PHRASE_PARTICLES = "data/dictionary/phdp%d.txt";
    private static final String TERM_INDEX_FLECKS = "data/dictionary/indexFleck_%d.txt";
    private static final String PHRASE_INDEX_FLECKS = "data/dictionary/phraseIndexFleck_%d.txt";
    private static final long WORKERS_MEMORY_LIMIT = Math.round(Runtime.getRuntime().maxMemory() * 0.5);
    private static final long MAX_MEMORY_LOAD = Math.round(Runtime.getRuntime().maxMemory() * 0.7);
    private static final String DP_SEPARATOR = " : "; // DOC_POSTING_SEPARATOR
    private static final String TC_SEPARATOR = " > "; // TERM_COORDINATE_SEPARATOR
    private static final String TERM_PATHS = "data/cache/term.bin";
    private static final String PHRASE_PATHS = "data/cache/phrase.bin";
    private static final String DOC_ID_PATH = "data/cache/docId.bin";

    private final BiMap<String, Integer> docId; // path - docId
    // term - id of the corresponding index file
    private final TST<Integer> termPostingPath;
    private final TST<Integer> phrasePostingPath;
    private static final Set<String> funcWords;
    private static final IndexServer INSTANCE = new IndexServer();
    private static final Morphology MORPH = new Morphology();

    // building-time variables
    private int numStoredDictionaries;
    private int numStoredPhraseDicts;
    private final CountDownLatch completion = new CountDownLatch(WORKERS);
    // hash function is effective for Integer
    private TreeMap<String, HashMap<Integer, ArrayList<Integer>>> dictionary;
    private TreeMap<String, Set<Integer>> phraseDict;

    static {
        funcWords = parseFuncSet();
    }

    @SuppressWarnings("unchecked")
    private IndexServer() {
        TST<Integer> termCache, phraseCache;
        BiMap<String, Integer> docIdCache;
        File term = new File(TERM_PATHS);
        File phrase = new File(PHRASE_PATHS);
        File docID = new File(DOC_ID_PATH);
        long p1 = System.currentTimeMillis();
        try {
            if (!term.exists() || !phrase.exists() || !docID.exists()) throw new NoSuchFieldException();
            System.out.println("load");
            ObjectInputStream oisTerm = new ObjectInputStream(new FileInputStream(term));
            termCache = (TST<Integer>) oisTerm.readObject();

            ObjectInputStream oisPhrase = new ObjectInputStream(new FileInputStream(phrase));
            phraseCache = (TST<Integer>) oisPhrase.readObject();

            ObjectInputStream oisDocId = new ObjectInputStream(new FileInputStream(docID));
            docIdCache = (BiMap<String, Integer>) oisDocId.readObject();
        } catch (Exception e) {
            termPostingPath = new TST<>();
            phrasePostingPath = new TST<>();
            docId = HashBiMap.create();
            return;
        }
        termPostingPath = termCache;
        phrasePostingPath = phraseCache;
        docId = docIdCache;
        System.out.println("time to load: " + (System.currentTimeMillis() - p1));
    }

    // there is no need to have multiple instances of this class as it is not supposed to be stored in collections
    // creating multiple instances and running them simultaneously may use more threads than expected
    // it can be refactored to take an array of strings if the files are spread across multiple files
    public static IndexServer getInstance() {
        if (INSTANCE.docId.size() == 0) INSTANCE.buildInvertedIndex();
        return INSTANCE;
    }

    public enum IndexType {
        TERM, PHRASE, COORDINATE
    }

    public boolean containsElement(String term, IndexType type) {
        switch (type) {
            case TERM:
                return termPostingPath.contains(term);
            case PHRASE:
                return phrasePostingPath.contains(term);
            case COORDINATE:
                return termPostingPath.contains(term);
            default:
                throw new IllegalArgumentException("incorrect type");
        }
    }

    public Map<Integer, ArrayList<Integer>> getTermDocCoord(String term) throws IOException {
        if (!containsElement(term, IndexType.TERM)) throw new NoSuchElementException("no term \"" + term + "\" found");
        try (Stream<String> lines = Files.lines(Paths.get(String.format(TERM_INDEX_FLECKS, termPostingPath.get(term))))) {
            String target = lines
                    .filter(line -> line.startsWith(term))
                    .toArray(String[]::new)[0];
            return parseCoords(target);
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
            default:
                throw new IllegalArgumentException("incorrect type");
        }

        ArrayList<Integer> pList = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(new File(path)))) {
            String search = br.readLine();
            while (search != null) {
                if (search.startsWith(term)) {
                    String postings = search.substring(search.indexOf(DP_SEPARATOR) + DP_SEPARATOR.length());
                    for (String docID : postings.split(DP_SEPARATOR))
                        pList.add(Integer.parseInt(docID.substring(0, docID.indexOf(TC_SEPARATOR))));
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
        return Paths.get(docId.inverse().get(docID)).getFileName().toString();
    }

    public Path getDocPath(int docID) {
        return Paths.get(docId.inverse().get(docID));
    }

    public void buildInvertedIndex() {
        System.out.println("start building");
        long startTime = System.nanoTime();
        dictionary = new TreeMap<>();
        phraseDict = new TreeMap<>();

        try (Stream<Path> files = Files.walk(LIBRARY)) {
            File[] documents = files.filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .peek(doc -> docId.put(doc.toString(), docId.size()))
                    .toArray(File[]::new);

            for (int i = 0; i < WORKERS; i++) {
                int from = (int) Math.round((double) documents.length / WORKERS * i);
                int to = (int) Math.round((double) documents.length / WORKERS * (i + 1));
                new Thread(new FileProcessor(Arrays.copyOfRange(documents, from, to))).start();
            }
            completion.await();
            //System.out.println("start transferMerge" + (System.currentTimeMillis() - startTime) / 1000);
            transferMerge();

            dictionary = null;
            phraseDict = null;

        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.nanoTime();
        System.out.println("multi time: " + (endTime - startTime) / 1e9);
        //writeCache();
    }

    private class FileProcessor implements Runnable {
        // random is used to prevent situation when all threads want to mergeOut simultaneously
        private final long WORKER_MEMORY = Math.round(WORKERS_MEMORY_LIMIT * (0.95 + Math.random() / 10));
        private final File[] files;

        FileProcessor(File[] docs) {
            files = docs;
        }

        @Override
        public void run() {
            Morphology morph = new Morphology();
            Map<String, ArrayList<Integer>> vocabulary;
            Set<String> phrases;
            for (File docFile : files) {
                vocabulary = new TreeMap<>();
                phrases = new TreeSet<>();
                int docID = docId.get(docFile.toString());
                TermProvider terms = new TermProvider(docFile, morph);
                // check func words from set, construct set
                String firstWord = null;
                boolean hasFuncWords = false;
                //System.out.println("passed init block " + (System.currentTimeMillis() - startTime) / 1000);

                for (int n = 0; terms.hasNextTerm(); n++) {
                    String term = terms.nextTerm();
                    vocabulary.putIfAbsent(term, new ArrayList<>());
                    vocabulary.get(term).add(n);

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
                        //System.out.println("time to save: " + showMemory());
                        //System.out.println("interrupted to save " + (System.currentTimeMillis() - startTime) / 1000);
                        mergeOut(new OutEntry(docID, vocabulary.entrySet(), phrases.toArray(new String[0])));
                        vocabulary = new TreeMap<>();
                        phrases = new TreeSet<>();
                        //System.out.println("back in task, proceed" + (System.currentTimeMillis() - startTime) / 1000);
                    }
                }
                // send to mergeOut after processing anyway
                mergeOut(new OutEntry(docID, vocabulary.entrySet(), phrases.toArray(new String[0])));
                //System.out.println("back in task, next " + (System.currentTimeMillis() - startTime) / 1000);
            }
            System.out.println("task completed, total: " + files.length + " time: " + getTime());
            completion.countDown();
        }
    }

    /**
     * takes normalized word as a parameter
     */
    public static boolean isFuncWord(String word) {
        return funcWords.contains(word);
    }

    /**
     * @param token char sequence split by whitespace
     * @return normalized term or null if the passed token is not a word
     */
    public @Nullable
    static String normalize(String token) {
        return normalize(token, MORPH);
    }

    private static String normalize(String token, Morphology m) {
        return m.stem(token.toLowerCase().replaceAll("\\W", ""));
    }

    private long startTime = System.currentTimeMillis();

    /**
     * merges small local dictionaries into a big global
     *
     * @param microMap - particle of dictionary built during a processing period
     */
    private synchronized void mergeOut(OutEntry microMap) {
        replenishDictionary(dictionary, microMap.getVocabulary(), microMap.getDocId());
        replenishDictionary(phraseDict, microMap.getPhrases(), microMap.getDocId());

        Runtime rt = Runtime.getRuntime();
        if (rt.maxMemory() - rt.freeMemory() > MAX_MEMORY_LOAD) {
            //System.out.println("mergeOut save: " + showMemory());
            saveParticles();
            //System.out.println("saved: " + showMemory());
        }
    }

    private void replenishDictionary(TreeMap<String, HashMap<Integer, ArrayList<Integer>>> dictionary,
                                     Set<Map.Entry<String, ArrayList<Integer>>> vocabulary, int docId) {
        for (Map.Entry<String, ArrayList<Integer>> entry : vocabulary) {
            dictionary.putIfAbsent(entry.getKey(), new HashMap<>());
            HashMap<Integer, ArrayList<Integer>> docCoords = dictionary.get(entry.getKey());
            if (docCoords.get(docId) == null) docCoords.put(docId, entry.getValue());
            else docCoords.get(docId).addAll(entry.getValue());
        }
    }

    private void replenishDictionary(Map<String, Set<Integer>> dictionary, String[] target, int docID) {
        for (String element : target) {
            dictionary.putIfAbsent(element, new TreeSet<>());
            dictionary.get(element).add(docID);
        }
    }

    private String showMemory() {
        Runtime rt = Runtime.getRuntime();
        return String.format("FREE memory: %.2f%%", (double) rt.freeMemory() / rt.maxMemory() * 100);
    }

    private void saveParticles() {
        String pathNameDict = String.format(TEMP_PARTICLES, numStoredDictionaries++);
        String pathNamePhraseDict = String.format(TEMP_PHRASE_PARTICLES, numStoredPhraseDicts++);
        writeTerms(pathNameDict, dictionary);
        writePhrase(pathNamePhraseDict, phraseDict);
        dictionary = new TreeMap<>();
        phraseDict = new TreeMap<>();
        System.gc();
    }

    private void writeTerms(String path, TreeMap<String, HashMap<Integer, ArrayList<Integer>>> dictionary) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(path)))) {
            for (Map.Entry<String, HashMap<Integer, ArrayList<Integer>>> entry : dictionary.entrySet()) {
                bw.write(entry.getKey());
                for (Integer docID : entry.getValue().keySet())
                    bw.write(DP_SEPARATOR + docID + TC_SEPARATOR
                            + entry.getValue().get(docID).toString().replaceAll("[\\[,\\]]", ""));
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writePhrase(String path, TreeMap<String, Set<Integer>> dictionary) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(path)))) {
            for (Map.Entry<String, Set<Integer>> entry : dictionary.entrySet()) {
                bw.write(entry.getKey() + DP_SEPARATOR + entry.getValue().toString());
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void transferMerge() throws IOException {
        saveParticles();
        // open reader for each particle and maintain priority queue for each particle on disk
        PriorityQueue<InEntryTerm> termEntries = new PriorityQueue<>();
        BufferedReader[] readers = new BufferedReader[numStoredDictionaries];
        initReaders(termEntries, readers, numStoredDictionaries, TEMP_PARTICLES, IndexType.TERM);
        termMerge(termEntries, readers, TERM_INDEX_FLECKS, termPostingPath, TEMP_PARTICLES);

        // repeat for phrase index
        PriorityQueue<InEntryPhrase> phEntries = new PriorityQueue<>();
        readers = new BufferedReader[numStoredPhraseDicts];
        initReaders(phEntries, readers, numStoredPhraseDicts, TEMP_PHRASE_PARTICLES, IndexType.PHRASE);
        phraseMerge(phEntries, readers, PHRASE_INDEX_FLECKS, phrasePostingPath, TEMP_PHRASE_PARTICLES);
    }

    private void initReaders(PriorityQueue entries, BufferedReader[] readers,
                             int numStored, String particlesPath, IndexType type) throws IOException {
        String checkLine;
        for (int i = 0; i < numStored; i++) {
            String pathName = String.format(particlesPath, i);
            readers[i] = new BufferedReader(new FileReader(new File(pathName)));
            checkLine = readers[i].readLine();
            if (checkLine != null) entries.add(type.equals(IndexType.PHRASE) ? new InEntryPhrase(i, checkLine)
                    : new InEntryTerm(i, checkLine));
        }
    }

    @SuppressWarnings("Duplicates")
    private void termMerge(PriorityQueue<InEntryTerm> entries, BufferedReader[] readers,
                           String outPath, TST<Integer> postingPath, String primaryPath) throws IOException {
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

            InEntryTerm nextEntry = entries.poll();
            HashMap<Integer, ArrayList<Integer>> docCoords = new HashMap<>();
            addDocCoords(entries, readers, nextEntry, docCoords, primaryPath);
            postingPath.put(nextEntry.getTerm(), numWritten);
            // check '!entries.isEmpty()' eliminates NullPointerException
            // find and process current term in all files
            while (!entries.isEmpty() && nextEntry.getTerm().equals(entries.peek().getTerm()))
                addDocCoords(entries, readers, entries.poll(), docCoords, primaryPath);

            //System.out.println("before killer time: " + getTime() + showMemory());
            bw.write(nextEntry.getTerm() + coordsToString(docCoords));
            bw.newLine();
            Runtime rt = Runtime.getRuntime();
            if (rt.maxMemory() - rt.freeMemory() > MAX_MEMORY_LOAD) {
                //System.out.println("Time to clean: " + getTime() + showMemory());
                bw.flush();
                bw.close();
                pathName = String.format(outPath, ++numWritten);
                bw = new BufferedWriter(new FileWriter(new File(pathName)));
                System.gc();
            }
        }
    }

    private long getTime() {
        return (System.currentTimeMillis() - startTime) / 1000;
    }

    private String coordsToString(HashMap<Integer, ArrayList<Integer>> docCoords) {
        StringBuilder resStr = new StringBuilder();
        for (Map.Entry<Integer, ArrayList<Integer>> entry : docCoords.entrySet()) {
            resStr.append(DP_SEPARATOR).append(entry.getKey()).append(TC_SEPARATOR);
            for (Integer coord : entry.getValue())
                resStr.append(coord).append(" ");
            resStr.deleteCharAt(resStr.length() - 1);
        }
        //System.out.println("coordsToString");
        return resStr.toString();
    }

    /*
     * possible bug: while being processed, a file was split -> 2 primary files have 2 terms of 1 original file
     * */


    @SuppressWarnings("Duplicates")
    private void addDocCoords(PriorityQueue<InEntryTerm> entries, BufferedReader[] readers,
                              InEntryTerm nextEntry, Map<Integer, ArrayList<Integer>> dCoords, String primaryPath)
            throws IOException {
        // TODO: check possible null ptr
        for (Map.Entry<Integer, ArrayList<Integer>> coords : nextEntry.getDocCoords().entrySet())
            dCoords.merge(coords.getKey(), coords.getValue(), (oldV, newV) -> {
                oldV.addAll(newV);
                return oldV;
            });

        String newLine = readers[nextEntry.getFileInd()].readLine();
        if (newLine == null) {
            readers[nextEntry.getFileInd()].close(); //delete file
            Files.delete(Paths.get(String.format(primaryPath, nextEntry.getFileInd())));
        } else entries.add(new InEntryTerm(nextEntry.getFileInd(), newLine));
    }

    @SuppressWarnings("Duplicates")
    private void phraseMerge(PriorityQueue<InEntryPhrase> entries, BufferedReader[] readers, String outPath,
                             TST<Integer> postingPath, String primaryPath) throws IOException {
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

            InEntryPhrase nextEntry = entries.poll();
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
    }

    private void appendPosting(PriorityQueue<InEntryPhrase> entries, BufferedReader[] readers,
                               InEntryPhrase nextEntry, StringBuilder posting, String primaryPath) throws IOException {
        posting.append(nextEntry.getPostings());
        String newLine = readers[nextEntry.getIndex()].readLine();
        if (newLine == null) {
            readers[nextEntry.getIndex()].close(); //delete file
            //Files.delete(Paths.get(String.format(primaryPath, nextEntry.getIndex())));
        } else entries.add(new InEntryPhrase(nextEntry.getIndex(), newLine));
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

    private class OutEntry {
        private final Integer docID;
        private final Set<Map.Entry<String, ArrayList<Integer>>> vocabulary;
        private final String[] phrases;

        OutEntry(Integer id, Set<Map.Entry<String, ArrayList<Integer>>> v, String[] ph) {
            if (id == null) throw new IllegalArgumentException("null id is not permitted");
            docID = id;
            vocabulary = v;
            phrases = ph;
        }

        int getDocId() {
            return docID;
        }

        Set<Map.Entry<String, ArrayList<Integer>>> getVocabulary() {
            return vocabulary;
        }

        String[] getPhrases() {
            return phrases;
        }
    }

    class InEntryPhrase implements Comparable<InEntryPhrase>, InEntry {
        private int index; // index of a particle file
        private String term;
        private String postings;

        InEntryPhrase(int index, String value) {
            this.index = index;
            int separator = value.indexOf(DP_SEPARATOR);
            term = value.substring(0, separator);
            postings = value.substring(separator + 3);
        }

        @Override
        public int compareTo(@NotNull InEntryPhrase entry) {
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

    class InEntryTerm implements Comparable<InEntryTerm>, InEntry {
        private final int fileInd;
        private final String term;
        private final Map<Integer, ArrayList<Integer>> docCoords;

        public InEntryTerm(int fInd, String line) {
            fileInd = fInd;
            term = line.substring(0, line.indexOf(DP_SEPARATOR));
            docCoords = parseCoords(line);
        }

        @Override
        public int compareTo(InEntryTerm entry) {
            return term.compareTo(entry.term);
        }

        public int getFileInd() {
            return fileInd;
        }

        public String getTerm() {
            return term;
        }

        public Map<Integer, ArrayList<Integer>> getDocCoords() {
            return docCoords;
        }
    }

    private Map<Integer, ArrayList<Integer>> parseCoords(String line) {
        String[] spl_1 = line.split(DP_SEPARATOR); // 0th is the term then docID-positions sets
        Map<Integer, ArrayList<Integer>> result = new HashMap<>();
        for (int i = 1; i < spl_1.length; i++) {
            int coordStart = spl_1[i].indexOf(TC_SEPARATOR);
            String[] coordTokens = spl_1[i].substring(coordStart + TC_SEPARATOR.length()).split("\\s");
            ArrayList<Integer> coordList = new ArrayList<>(coordTokens.length);
            for (String coord : coordTokens)
                coordList.add(Integer.parseInt(coord));
            result.put(Integer.parseInt(spl_1[i].substring(0, coordStart)), coordList);
        }
        return result;
    }

    interface InEntry {
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

    private void writeCache() {
        System.out.println("start writing");
        long p1 = System.currentTimeMillis();
        try (ObjectOutputStream oosTerm = new ObjectOutputStream(new FileOutputStream(new File(TERM_PATHS)))) {
            oosTerm.writeObject(termPostingPath);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        try (ObjectOutputStream oosPhrase = new ObjectOutputStream(new FileOutputStream(new File(PHRASE_PATHS)))) {
            oosPhrase.writeObject(phrasePostingPath);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        try (ObjectOutputStream oosDocId = new ObjectOutputStream(new FileOutputStream(new File(DOC_ID_PATH)))) {
            oosDocId.writeObject(docId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Time to write: " + (System.currentTimeMillis() - p1));
    }
}


class TST<Value> implements Serializable {
    private int n;              // size
    private Node<Value> root;   // root of TST

    private static class Node<Value> implements Serializable {
        private char c;                        // character
        private Node<Value> left, mid, right;  // left, middle, and right subtries
        private Value val;                     // value associated with string
    }

    /**
     * Initializes an empty string symbol table.
     */
    public TST() {
    }

    /**
     * Returns the number of key-value pairs in this symbol table.
     *
     * @return the number of key-value pairs in this symbol table
     */
    public int size() {
        return n;
    }

    /**
     * Does this symbol table contain the given key?
     *
     * @param key the key
     * @return {@code true} if this symbol table contains {@code key} and
     * {@code false} otherwise
     * @throws IllegalArgumentException if {@code key} is {@code null}
     */
    public boolean contains(String key) {
        if (key == null) {
            throw new IllegalArgumentException("argument to contains() is null");
        }
        return get(key) != null;
    }

    /**
     * Returns the value associated with the given key.
     *
     * @param key the key
     * @return the value associated with the given key if the key is in the symbol table
     * and {@code null} if the key is not in the symbol table
     * @throws IllegalArgumentException if {@code key} is {@code null}
     */
    public Value get(String key) {
        if (key == null) {
            throw new IllegalArgumentException("calls get() with null argument");
        }
        if (key.length() == 0) throw new IllegalArgumentException("key must have length >= 1");
        Node<Value> x = get(root, key, 0);
        if (x == null) return null;
        return x.val;
    }

    // return subtrie corresponding to given key
    private Node<Value> get(Node<Value> x, String key, int d) {
        if (x == null) return null;
        if (key.length() == 0) throw new IllegalArgumentException("key must have length >= 1");
        char c = key.charAt(d);
        if (c < x.c) return get(x.left, key, d);
        else if (c > x.c) return get(x.right, key, d);
        else if (d < key.length() - 1) return get(x.mid, key, d + 1);
        else return x;
    }

    /**
     * Inserts the key-value pair into the symbol table, overwriting the old value
     * with the new value if the key is already in the symbol table.
     * If the value is {@code null}, this effectively deletes the key from the symbol table.
     *
     * @param key the key
     * @param val the value
     * @throws IllegalArgumentException if {@code key} is {@code null}
     */
    public void put(String key, Value val) {
        if (key == null) {
            throw new IllegalArgumentException("calls put() with null key");
        }
        if (!contains(key)) n++;
        else if (val == null) n--;       // delete existing key
        root = put(root, key, val, 0);
    }

    private Node<Value> put(Node<Value> x, String key, Value val, int d) {
        char c = key.charAt(d);
        if (x == null) {
            x = new Node<>();
            x.c = c;
        }
        if (c < x.c) x.left = put(x.left, key, val, d);
        else if (c > x.c) x.right = put(x.right, key, val, d);
        else if (d < key.length() - 1) x.mid = put(x.mid, key, val, d + 1);
        else x.val = val;
        return x;
    }

    /**
     * Returns the string in the symbol table that is the longest prefix of {@code query},
     * or {@code null}, if no such string.
     *
     * @param query the query string
     * @return the string in the symbol table that is the longest prefix of {@code query},
     * or {@code null} if no such string
     * @throws IllegalArgumentException if {@code query} is {@code null}
     */
    public String longestPrefixOf(String query) {
        if (query == null) {
            throw new IllegalArgumentException("calls longestPrefixOf() with null argument");
        }
        if (query.length() == 0) return null;
        int length = 0;
        Node<Value> x = root;
        int i = 0;
        while (x != null && i < query.length()) {
            char c = query.charAt(i);
            if (c < x.c) x = x.left;
            else if (c > x.c) x = x.right;
            else {
                i++;
                if (x.val != null) length = i;
                x = x.mid;
            }
        }
        return query.substring(0, length);
    }

    /**
     * Returns all keys in the symbol table as an {@code Iterable}.
     * To iterate over all of the keys in the symbol table named {@code st},
     * use the foreach notation: {@code for (Key key : st.keys())}.
     *
     * @return all keys in the symbol table as an {@code Iterable}
     */
    public Iterable<String> keys() {
        Queue<String> queue = new ArrayDeque<>();
        collect(root, new StringBuilder(), queue);
        return queue;
    }

    /**
     * Returns all of the keys in the set that start with {@code prefix}.
     *
     * @param prefix the prefix
     * @return all of the keys in the set that start with {@code prefix},
     * as an iterable
     * @throws IllegalArgumentException if {@code prefix} is {@code null}
     */
    public Iterable<String> keysWithPrefix(String prefix) {
        if (prefix == null) {
            throw new IllegalArgumentException("calls keysWithPrefix() with null argument");
        }
        Queue<String> queue = new ArrayDeque<>();
        Node<Value> x = get(root, prefix, 0);
        if (x == null) return queue;
        if (x.val != null) queue.add(prefix);
        collect(x.mid, new StringBuilder(prefix), queue);
        return queue;
    }

    // all keys in subtrie rooted at x with given prefix
    private void collect(Node<Value> x, StringBuilder prefix, Queue<String> queue) {
        if (x == null) return;
        collect(x.left, prefix, queue);
        if (x.val != null) queue.add(prefix.toString() + x.c);
        collect(x.mid, prefix.append(x.c), queue);
        prefix.deleteCharAt(prefix.length() - 1);
        collect(x.right, prefix, queue);
    }


    /**
     * Returns all of the keys in the symbol table that match {@code pattern},
     * where . symbol is treated as a wildcard character.
     *
     * @param pattern the pattern
     * @return all of the keys in the symbol table that match {@code pattern},
     * as an iterable, where . is treated as a wildcard character.
     */
    public Iterable<String> keysThatMatch(String pattern) {
        Queue<String> queue = new ArrayDeque<>();
        collect(root, new StringBuilder(), 0, pattern, queue);
        return queue;
    }

    private void collect(Node<Value> x, StringBuilder prefix, int i, String pattern, Queue<String> queue) {
        if (x == null) return;
        char c = pattern.charAt(i);
        if (c == '.' || c < x.c) collect(x.left, prefix, i, pattern, queue);
        if (c == '.' || c == x.c) {
            if (i == pattern.length() - 1 && x.val != null) queue.add(prefix.toString() + x.c);
            if (i < pattern.length() - 1) {
                collect(x.mid, prefix.append(x.c), i + 1, pattern, queue);
                prefix.deleteCharAt(prefix.length() - 1);
            }
        }
        if (c == '.' || c > x.c) collect(x.right, prefix, i, pattern, queue);
    }
}