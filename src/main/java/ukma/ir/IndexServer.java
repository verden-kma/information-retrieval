package ukma.ir;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import edu.stanford.nlp.process.Morphology;
import ukma.ir.data_stuctores.RandomizedQueue;
import ukma.ir.data_stuctores.TST;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import static java.lang.String.valueOf;

public class IndexServer {
    private static final int WORKERS = 3;
    private Path LIBRARY = Paths.get("G:\\project\\library\\custom");
    private static final String TEMP_PARTICLES = "data/dictionary/dp%d.txt";
    private static final String TERM_INDEX_FLECKS = "data/dictionary/indexFleck_%d.txt";
    private static final long WORKERS_MEMORY_LIMIT = Math.round(Runtime.getRuntime().maxMemory() * 0.5);
    private static final long MAX_MEMORY_LOAD = Math.round(Runtime.getRuntime().maxMemory() * 0.7);
    private static final char TERM_DOC_SEP = ':'; // TERM_DOC
    private static final char DOC_COORD_SEP = '>'; // DOC_COORDINATE
    private static final char COORD_SEP = ' ';
//    private static final String TERM_PATHS = "data/cache/term.bin";
//    private static final String DOC_ID_PATH = "data/cache/docId.bin";

    private final BiMap<String, Integer> docId; // path - docId
    // term - id of the corresponding index file
    private final TST<Integer> termPostingPath;
    private TST<String> reversedTermPostingPath;
    private static IndexServer instance; //effectively final
    private static final Morphology MORPH = new Morphology();

    // building-time variables
    private int numStoredDictionaries;
    private final CountDownLatch completion = new CountDownLatch(WORKERS);
    // hash function is effective for Integer
    private TreeMap<String, HashMap<Integer, ArrayList<Integer>>> dictionary;
    private Set<String> vocabulary;
    private static final Object locker = new Object();

    private IndexServer() {
        termPostingPath = new TST<>();
        docId = HashBiMap.create();
    }

    // there is no need to have multiple instances of this class as it is not supposed to be stored in collections
    // creating multiple instances and running them simultaneously may use more threads than expected
    // it can be refactored to take an array of strings if the files are spread across multiple files
    public static IndexServer getInstance() {
        synchronized (locker) {
            if (instance == null) instance = new IndexServer();
            return instance;
        }
    }

    // phrase index is removed, so at this stage there is no need for the enum but it will change
    public enum IndexType {
        TERM, COORDINATE, JOKER
    }

    public boolean containsElement(String term, IndexType type) {
        switch (type) {
            case TERM:
                return termPostingPath.contains(term);
            case COORDINATE:
                return termPostingPath.contains(term);
            default:
                throw new IllegalArgumentException("incorrect type");
        }
    }

    /**
     * @param term normalized token
     * @return set of postings with list of positions associated with each document
     * @throws IOException
     */
    public Map<Integer, ArrayList<Integer>> getTermDocCoord(String term) throws IOException {
        if (!containsElement(term, IndexType.COORDINATE))
            throw new NoSuchElementException("no term \"" + term + "\" found");
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
     * @return postings list or empty list if no postings found
     */                // long operation!!!
    public ArrayList<Integer> getPostings(String term, IndexType type) {
        if (!containsElement(term, type)) throw new NoSuchElementException("No such element found!");
        String path;
        switch (type) {
            case TERM:
                path = String.format(TERM_INDEX_FLECKS, termPostingPath.get(term));
                break;
            default:
                throw new IllegalArgumentException("incorrect type");
        }

        try (BufferedReader br = new BufferedReader(new FileReader(new File(path)))) {
            String search = br.readLine();
            while (search != null) {
                if (search.startsWith(term)) {
                    String postings = search.substring(search.indexOf(TERM_DOC_SEP) + 1); // length of a char
                    ArrayList<Integer> pList = new ArrayList<>();
                    for (String docID : postings.split(valueOf(TERM_DOC_SEP)))
                        pList.add(Integer.parseInt(docID.substring(0, docID.indexOf(DOC_COORD_SEP))));
                    return pList;
                }
                search = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new NoSuchElementException("cannot find file specified");
        }
        throw new NoSuchElementException("index file is found but it does not contain the element specified");
    }

    public Iterable<String> startWith(String prefix) {
        return termPostingPath.keysWithPrefix(prefix);
    }

    public Iterable<String> endWith(String suffix) {
        StringBuilder reverser = new StringBuilder(suffix.length());
        reverser.append(suffix).reverse();
        // TST returns sorted keys and so makes it inefficient to add them int another TST
        Collection<String> reversed = reversedTermPostingPath.keysWithPrefix(reverser.toString());
        List<String> straight = new ArrayList<>(reversed.size());
        for (String rev : reversed)
            straight.add(reversedTermPostingPath.get(rev));
        return straight;
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
            System.out.println("start transferMerge: " + getTime());
            transferMerge();
            //System.out.println("start building REVERSED");
            //buildReversed();
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.nanoTime();
        System.out.println("multi time: " + (endTime - startTime) / 1e9);
    }

    private class FileProcessor implements Runnable {
        // random is used to prevent situation when all threads want to mergeOut simultaneously
        private final long WORKER_MEMORY = Math.round(WORKERS_MEMORY_LIMIT * (0.7 + 0.6 * Math.random()));
        private final File[] files;

        FileProcessor(File[] docs) {
            files = docs;
        }

        @Override
        public void run() {
            Morphology morph = new Morphology();
            Map<String, ArrayList<Integer>> vocabulary;
//            List<OutEntry> outers = new ArrayList<>();
            for (File docFile : files) {

                vocabulary = new TreeMap<>();
                int docID = docId.get(docFile.toString());
                TermProvider terms = new TermProvider(docFile, morph);

                for (int n = 0; terms.hasNextTerm(); n++) {
                    String term = terms.nextTerm();
                    vocabulary.putIfAbsent(term, new ArrayList<>());
                    vocabulary.get(term).add(n);

                    Runtime rt = Runtime.getRuntime();
                    if (rt.maxMemory() - rt.freeMemory() > WORKER_MEMORY) {
                        mergeOut(new OutEntry(docID, vocabulary.entrySet()));
//                        outers.add(new OutEntry(docID, vocabulary.entrySet()));
//                        mergeOut(outers);
                        /* outers.clear(); breaks correctness*/
//                        outers = new ArrayList<>();
                        vocabulary = new TreeMap<>();
                    }
                }
                //outers.add(new OutEntry(docID, vocabulary.entrySet()));
                mergeOut(new OutEntry(docID, vocabulary.entrySet()));
            }
            System.out.println("task completed, total: " + files.length + " time: " + getTime());
            completion.countDown();
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

    private static String normalize(String token, Morphology m) {
        return m.stem(token.toLowerCase().replaceAll("\\W", ""));
    }

    /*
     * the second versions of 'mergeOut' is intended to facilitate optimization of passing a list<OutEntry> so that
     *     time used for synchronization reduces, but in practise it just words randomly and is buggy
     * */
    private synchronized void mergeOut(OutEntry microMaps) {
        replenishDictionary(dictionary, microMaps.getVocabulary(), microMaps.getDocId());
        Runtime rt = Runtime.getRuntime();
        if (rt.maxMemory() - rt.freeMemory() > MAX_MEMORY_LOAD) {
            //System.out.println("mergeOut save: " + showMemory());
            saveParticles();
            //System.out.println("saved: " + showMemory());
        }
    }

//    /** IMPORTANT IDEA, DO NOT DELETE!!!
//     * merges small local dictionaries into a big global
//     *
//     * @param microMaps - list of particle of dictionary built during a processing period
//     */
//    private synchronized void mergeOut(List<OutEntry> microMaps) {
//        for (OutEntry out : microMaps) {
//            replenishDictionary(dictionary, out.getVocabulary(), out.getDocId());
//
//            Runtime rt = Runtime.getRuntime();
//            if (rt.maxMemory() - rt.freeMemory() > MAX_MEMORY_LOAD) {
//                //System.out.println("mergeOut save: " + showMemory());
//                saveParticles();
//                //System.out.println("saved: " + showMemory());
//            }
//        }
//    }

    private void replenishDictionary(TreeMap<String, HashMap<Integer, ArrayList<Integer>>> dictionary,
                                     Set<Map.Entry<String, ArrayList<Integer>>> vocabulary, int docId) {
        for (Map.Entry<String, ArrayList<Integer>> entry : vocabulary) {
            dictionary.putIfAbsent(entry.getKey(), new HashMap<>());
            dictionary.get(entry.getKey()).merge(docId, entry.getValue(), (oldVal, newVal) -> {
                oldVal.addAll(newVal);
                return oldVal;
            });
        }
    }

    private void saveParticles() {
        String pathNameDict = String.format(TEMP_PARTICLES, numStoredDictionaries++);
        writeTerms(pathNameDict, dictionary);
        vocabulary.addAll(dictionary.keySet()); // check memory errors
        dictionary = new TreeMap<>();
        System.gc();
    }

    private void writeTerms(String path, TreeMap<String, HashMap<Integer, ArrayList<Integer>>> dictionary) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(path)))) {
            for (Map.Entry<String, HashMap<Integer, ArrayList<Integer>>> entry : dictionary.entrySet()) {
                bw.write(entry.getKey());
                for (Integer docID : entry.getValue().keySet())
                    bw.write(TERM_DOC_SEP + docID + DOC_COORD_SEP
                            + entry.getValue().get(docID).toString().replaceAll("[\\[,\\]]", ""));
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void transferMerge() throws IOException {
        saveParticles();
        dictionary = null;

        RandomizedQueue<Tuple<String, Integer>> tstElems = new RandomizedQueue<>();
        int numFlecks = 0;
        int numTermsPerFleck = (int) Math.sqrt(vocabulary.size());
        vocabulary = null;
        String path = String.format(TERM_INDEX_FLECKS, numFlecks);
        BufferedWriter fleck = new BufferedWriter(new FileWriter(new File(path)));

        PriorityQueue<PTP> particleTermsPQ = initPTP();
        for (int i = 0; !particleTermsPQ.isEmpty(); i++) {
            if (i > numTermsPerFleck) { // each fleck contains no more then sqrt() terms
                path = String.format(TERM_INDEX_FLECKS, ++numFlecks);
                fleck.close();
                fleck = new BufferedWriter(new FileWriter(new File(path)));
                i = 0;
            }
            PTP termHead = particleTermsPQ.poll();
            tstElems.enqueue(new Tuple<>(termHead.getCurrTerm(), numFlecks));

            PriorityQueue<PTP> docIdPQ = new PriorityQueue<>(PTP.getComparator(PTP.State.DOC_ID));
            termHead.getNext();
            docIdPQ.add(termHead);
            while (!particleTermsPQ.isEmpty() && termHead.equalsTerm(particleTermsPQ.peek())) {// come through all terms
                PTP sameDocId = particleTermsPQ.poll();
                sameDocId.getNext(); // state == post doc
                docIdPQ.add(sameDocId); // there will be lots of elements but only few duplicates
                                        // (customer's file was split into 2+ particles)
            }

            // they will all have the same term
            fleck.write(termHead.getCurrTerm());
            fleck.write(TERM_DOC_SEP);

            PriorityQueue<PTP> posPQ = new PriorityQueue<>(PTP.getComparator(PTP.State.POSITION));
            PTP docIdHead = docIdPQ.poll();
            docIdHead.getNext();
            posPQ.add(docIdHead);
            while (!docIdPQ.isEmpty()) {
                while (!docIdPQ.isEmpty() && /*if duplicate is present*/ docIdHead.equalsDocId(docIdPQ.peek())) {// come through all docs of current term
                    PTP diffPosition = docIdPQ.poll();
                    diffPosition.getNext(); // state == post pos (possible term or doc or EOF)
                    posPQ.add(diffPosition);
                }

                //TODO: further code is buggy: no correct ending for merged line of term postings, no addition of non-empty PTPs to their queues

                fleck.write(termHead.getCurrDocID());
                fleck.write(DOC_COORD_SEP);

                while (!posPQ.isEmpty()) { // come through all coords of current doc
                    PTP nextPos = posPQ.poll();
                    fleck.write(nextPos.getCurrPos());
                    fleck.write(COORD_SEP);

                    while (nextPos.getCurrState() == PTP.State.POSITION) {
                        fleck.write(nextPos.getNext());
                        fleck.write(COORD_SEP);
                    }

                    switch (nextPos.getCurrState()) {
                        case TERM:
                            nextPos.getNext(); // switch to the next state
                            particleTermsPQ.add(nextPos);
                        case DOC_ID:
                            nextPos.getNext(); // switch to the next state
                            docIdPQ.add(nextPos);
                        case EOF:
                            System.out.println("processed next dp");
                    }

                }

            }

        }
    }

    private PriorityQueue<PTP> initPTP() throws IOException {
        PriorityQueue<PTP> ptpPQ = new PriorityQueue<>(PTP.getComparator(PTP.State.TERM));
        for (int i = 0; i < numStoredDictionaries; i++) {
            PTP nextPTP = new PTP(new File(String.format(TEMP_PARTICLES, i)));
            nextPTP.getNext(); // change state to DocID and set cTerm
            ptpPQ.add(nextPTP);
        }
        return ptpPQ;
    }


    static class PTP {

        // ParticleTokenProvider
        enum State { // state to which the next char refers ":" and ">" change the state
            TERM, DOC_ID, POSITION, EOF
        }

        private final StringBuilder builder = new StringBuilder();
        private final BufferedReader br;
        private State currentState;
        private String cTerm;
        private String cDocID;
        private String cPos;
        private String nextTerm;

        PTP(File particle) throws IOException {
            br = new BufferedReader(new FileReader(particle));
            currentState = State.TERM;
        }

        String getNext() {
            try {
                return fill();
            } catch (IOException e) {
                throw new RuntimeException("Internal error", e);
            }
        }

        private String fill() throws IOException {
            assert nextTerm == null || (currentState == State.TERM);

            if (nextTerm != null && currentState == State.TERM) {
                String res = nextTerm;
                nextTerm = null;
                currentState = State.DOC_ID;
                return res;
            }

            char nextChar = (char) br.read();
            if (nextChar == (char) -1) return null;
            while (nextChar != TERM_DOC_SEP && nextChar != DOC_COORD_SEP && nextChar != COORD_SEP
                    && nextChar != '\n' && nextChar != (char) -1) {
                builder.append(nextChar);
                nextChar = (char) br.read();
            }

            String update = builder.toString();
            if (currentState == State.EOF) return nextTerm = update;

            switch (currentState) {
                case TERM:
                    assert (nextChar == TERM_DOC_SEP);
                    cTerm = update;
                    currentState = State.DOC_ID;
                    break;
                case DOC_ID:
                    assert (nextChar == DOC_COORD_SEP);
                    cDocID = update;
                    currentState = State.POSITION;
                    break;
                case POSITION:
                    cPos = update;
                    if (nextChar == TERM_DOC_SEP) currentState = State.DOC_ID;
                    assert (nextChar != DOC_COORD_SEP);
                    break;
            }
            builder.setLength(0);
            if (nextChar == '\n') {
                // suspect EOF, if not EOF then save next term and change state to TERM
                // read saved term with next invocation
                currentState = State.EOF;
                if (fill() == null) br.close();
                else currentState = State.TERM;
            }
            return update;
        }

        static Comparator<PTP> getComparator(State mode) {
            switch (mode) {
                case TERM:
                    Comparator.comparing(PTP::getCurrTerm);
                case DOC_ID:
                    return Comparator.comparing(PTP::getCurrDocID);
                case POSITION:
                    return Comparator.comparing(PTP::getCurrPos);
                default:
                    throw new IllegalArgumentException("incorrect state passed");
            }
        }

        boolean equalsTerm(PTP peek) {
            return cTerm.equals(peek.cTerm);
        }

        public boolean equalsDocId(PTP peek) {
            return cDocID.equals(peek.cDocID);
        }

        State getCurrState() {
            return currentState;
        }

        String getCurrTerm() {
            return cTerm;
        }

        String getCurrDocID() {
            return cDocID;
        }

        String getCurrPos() {
            return cPos;
        }
    }

//    // buggy in memory
//    //TODO: rewrite
//    private void transferMerge() throws IOException {
//        saveParticles();
//        dictionary = null;
//        // open reader for each particle and maintain priority queue for each particle on disk
//        PriorityQueue<InEntryTerm> termEntries = new PriorityQueue<>();
//        BufferedReader[] readers = new BufferedReader[numStoredDictionaries];
//        initReaders(termEntries, readers, numStoredDictionaries, TEMP_PARTICLES);
//        // use randomized queue to enable efficient construction of TST
//        RandomizedQueue<Tuple<String, Integer>> rq = new RandomizedQueue<>();
//        termMerge(termEntries, readers, TERM_INDEX_FLECKS, rq, TEMP_PARTICLES);
//        for (int i = 0; i < rq.size(); i++) {
//            Tuple<String, Integer> postPath = rq.dequeue();
//            termPostingPath.put(postPath.getV1(), postPath.getV2());
//        }
//    }
//
//    private void initReaders(PriorityQueue<InEntryTerm> entries, BufferedReader[] readers,
//                             int numStored, String particlesPath) throws IOException {
//        String checkLine;
//        for (int i = 0; i < numStored; i++) {
//            String pathName = String.format(particlesPath, i);
//            readers[i] = new BufferedReader(new FileReader(new File(pathName)));
//            checkLine = readers[i].readLine();
//            if (checkLine != null) entries.add(new InEntryTerm(i, checkLine));
//            else readers[i].close();
//        }
//    }
//
//    // causes OutOfMemory error (postingPaths takes too much space)
//    private void termMerge(PriorityQueue<InEntryTerm> entries, BufferedReader[] readers,
//                           String outPath, RandomizedQueue<Tuple<String, Integer>> postingPaths, String primaryPath) throws IOException {
//        int numWritten = 0;
//        // initialize writer for the first dictionary fleck
//        String pathName = String.format(outPath, numWritten);
//        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(pathName)));
//
//        while (!entries.isEmpty()) {
//            InEntryTerm nextEntry = entries.poll();
//
//            System.out.println("Current term: " + nextEntry.getTerm());
//
//            HashMap<Integer, ArrayList<Integer>> docCoords = new HashMap<>();
//            addDocCoords(entries, readers, nextEntry, docCoords, primaryPath);
//            postingPaths.enqueue(new Tuple<>(nextEntry.getTerm(), numWritten));
//            // check '!entries.isEmpty()' eliminates NullPointerException
//            // find and process current term in all files
//            while (!entries.isEmpty() && nextEntry.getTerm().equals(entries.peek().getTerm()))
//                addDocCoords(entries, readers, entries.poll(), docCoords, primaryPath);
//
//            bw.write(nextEntry.getTerm() + coordsToString(docCoords));
//            bw.newLine();
//            Runtime rt = Runtime.getRuntime();
//            if (rt.maxMemory() - rt.freeMemory() > MAX_MEMORY_LOAD) {
//                System.out.println("Time to clean: " + System.currentTimeMillis() + COORD_SEP + showMemory());
//                bw.flush();
//                bw.close();
//                pathName = String.format(outPath, ++numWritten);
//                bw = new BufferedWriter(new FileWriter(new File(pathName)));
//                System.gc();
//            }
//        }
//        bw.flush();
//        bw.close();
//    }
//
//    private String coordsToString(HashMap<Integer, ArrayList<Integer>> docCoords) {
//        StringBuilder resStr = new StringBuilder();
//        for (Map.Entry<Integer, ArrayList<Integer>> entry : docCoords.entrySet()) {
//            resStr.append(TERM_DOC_SEP).append(entry.getKey()).append(DOC_COORD_SEP);
//            for (Integer coord : entry.getValue())
//                resStr.append(coord).append(COORD_SEP);
//            resStr.deleteCharAt(resStr.length() - 1);
//        }
//        return resStr.toString();
//    }

    /**
     * parses string representation of term's docID-coords set into a map
     *
     * @param line representing line postings with associated coordinates
     * @return HashMap of posting-coords sets
     */
    private Map<Integer, ArrayList<Integer>> parseCoords(String line) {
        String[] spl_1 = line.split(valueOf(TERM_DOC_SEP)); // 0th is the term then docID-positions sets
        Map<Integer, ArrayList<Integer>> result = new HashMap<>();
        for (int i = 1; i < spl_1.length; i++) {
            int coordStart = spl_1[i].indexOf(DOC_COORD_SEP);
            String[] coordTokens = spl_1[i].substring(coordStart + 1).split("\\s"); // 1 = length of DOC_COORD_SEP
            ArrayList<Integer> coordList = new ArrayList<>(coordTokens.length);
            for (String coord : coordTokens)
                coordList.add(Integer.parseInt(coord));
            result.put(Integer.parseInt(spl_1[i].substring(0, coordStart)), coordList);
        }
        return result;
    }

    private void addDocCoords(PriorityQueue<InEntryTerm> entries, BufferedReader[] readers,
                              InEntryTerm nextEntry, Map<Integer, ArrayList<Integer>> docCoords, String primaryPath)
            throws IOException {
        for (Map.Entry<Integer, ArrayList<Integer>> coords : nextEntry.getDocCoords().entrySet())
            docCoords.merge(coords.getKey(), coords.getValue(), (oldV, newV) -> {
                oldV.addAll(newV);
                return oldV;
            });

        String newLine = readers[nextEntry.getFileInd()].readLine();
        if (newLine == null) {
            readers[nextEntry.getFileInd()].close(); //delete file
            // Files.delete(Paths.get(String.format(primaryPath, nextEntry.getFileInd())));
        } else entries.add(new InEntryTerm(nextEntry.getFileInd(), newLine));
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

        OutEntry(Integer id, Set<Map.Entry<String, ArrayList<Integer>>> v) {
            if (id == null) throw new IllegalArgumentException("null id is not permitted");
            docID = id;
            vocabulary = v;
        }

        int getDocId() {
            return docID;
        }

        Set<Map.Entry<String, ArrayList<Integer>>> getVocabulary() {
            return vocabulary;
        }
    }


    class InEntryTerm implements Comparable<InEntryTerm> {
        private final int fileInd;
        private final String term;
        private final Map<Integer, ArrayList<Integer>> docCoords;

        public InEntryTerm(int fInd, String line) {
            fileInd = fInd;
            term = line.substring(0, line.indexOf(TERM_DOC_SEP));
            docCoords = parseCoords(line);
        }

        @Override
        public int compareTo(InEntryTerm entry) {
            if (entry == null) throw new IllegalArgumentException("entry is null");
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

    private void buildReversed() {
        reversedTermPostingPath = new TST<>();
        StringBuilder reverser = new StringBuilder(15);
        for (String key : termPostingPath.keys()) {
            reverser.append(key);
            reverser.reverse();
            reversedTermPostingPath.put(reverser.toString(), key);
            reverser.setLength(0);
        }
    }


    private long startTime = System.currentTimeMillis();

    private String showMemory() {
        Runtime rt = Runtime.getRuntime();
        return String.format("FREE memory: %.2f%%", (double) rt.freeMemory() / rt.maxMemory() * 100);
    }

    private long getTime() {
        return (System.currentTimeMillis() - startTime) / 1000;
    }
}


class Tuple<T, E> {
    private T v1;
    private E v2;

    public Tuple(T v1, E v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    public T getV1() {
        return v1;
    }

    public void setV1(T v1) {
        this.v1 = v1;
    }

    public E getV2() {
        return v2;
    }

    public void setV2(E v2) {
        this.v2 = v2;
    }
}