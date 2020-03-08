package ukma.ir.index;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import edu.stanford.nlp.process.Morphology;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import ukma.ir.index.helpers.IndexBody;
import ukma.ir.index.helpers.TermData;

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
    private static final String INDEX_FLECKS = "data/dictionary/indexFleck_%d.txt";
    private static final long WORKERS_MEMORY_LIMIT = Math.round(Runtime.getRuntime().maxMemory() * 0.5);
    private static final long MAX_MEMORY_LOAD = Math.round(Runtime.getRuntime().maxMemory() * 0.7);
    private static final char NEXT_DOC_SEP = ':'; // TERM_DOC
    private static final char DOC_COORD_SEP = '>'; // DOC_COORDINATE
    private static final char COORD_SEP = ' ';
    private static final char TF_SEP = '#'; // TERM_FREQUENCY
//    private static final String TERM_PATHS = "data/cache/term.bin";
//    private static final String DOC_ID_PATH = "data/cache/docId.bin";

    private final BiMap<String, Integer> docId; // path - docId
    // term - id of the corresponding index file
    private IndexBody index;
    private static IndexServer instance; //effectively final
    private static final Morphology MORPH = new Morphology();

    // building-time variables
    private int numStoredDictionaries;
    private final CountDownLatch completion = new CountDownLatch(WORKERS);
    // hash function is effective for Integer
    // <term, Hash<docID, positions>> size of map == df, size of positions == tf
    private TreeMap<String, HashMap<Integer, ArrayList<Integer>>> dictionary;
    private static final Object locker = new Object();

    private IndexServer() {
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

    // for GUI
    public enum IndexType {
        TERM, COORDINATE, JOKER
    }

    // #region Deprecated

    /**
     * FIXME: 07-Mar-20 implement and use IndexBody's methods
     */
    public boolean containsElement(String term) {
        throw new NotImplementedException();
//        switch (type) {
//            case TERM:
//                return termPostingPath.contains(term);
//            case COORDINATE:
//                return termPostingPath.contains(term);
//            default:
//                throw new IllegalArgumentException("incorrect type");
//        }
    }

    /**
     * FIXME: 07-Mar-20 RandomAccessFile instead of reading the whole file
     */
    public Map<Integer, ArrayList<Integer>> getTermDocCoord(String term) throws IOException {
        throw new NotImplementedException();
//        if (!containsElement(term, IndexType.COORDINATE))
//            throw new NoSuchElementException("no term \"" + term + "\" found");
//        try (Stream<String> lines = Files.lines(Paths.get(String.format(INDEX_FLECKS, termPostingPath.get(term))))) {
//            String target = lines
//                    .filter(line -> line.startsWith(term))
//                    .toArray(String[]::new)[0];
//            return parseCoords(target);
//        }
    }

    /**
     * FIXME: 07-Mar-20 RandomAccessFile instead of reading the whole file
     */
    public ArrayList<Integer> getPostings(String term) {
        throw new NotImplementedException();
//        if (!containsElement(term, type)) throw new NoSuchElementException("No such element found!");
//        String path;
//        switch (type) {
//            case TERM:
//                path = String.format(INDEX_FLECKS, termPostingPath.get(term));
//                break;
//            default:
//                throw new IllegalArgumentException("incorrect type");
//        }
//
//        try (BufferedReader br = new BufferedReader(new FileReader(new File(path)))) {
//            String search = br.readLine();
//            while (search != null) {
//                if (search.startsWith(term)) {
//                    String postings = search.substring(search.indexOf(NEXT_DOC_SEP) + 1); // length of a char
//                    ArrayList<Integer> pList = new ArrayList<>();
//                    for (String docID : postings.split(valueOf(NEXT_DOC_SEP)))
//                        pList.add(Integer.parseInt(docID.substring(0, docID.indexOf(DOC_COORD_SEP))));
//                    return pList;
//                }
//                search = br.readLine();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//            throw new NoSuchElementException("cannot find file specified");
//        }
//        throw new NoSuchElementException("index file is found but it does not contain the element specified");
    }

    /**
     * FIXME: 07-Mar-20 adapt for dictionary as a string
     */
    public Iterable<String> startWith(String prefix) {
        throw new NotImplementedException();
        // return termPostingPath.keysWithPrefix(prefix);
    }

    /**
     * FIXME: 07-Mar-20 adapt for dictionary as a string
     */
    public Iterable<String> endWith(String suffix) {
        throw new NotImplementedException();
//        StringBuilder reverser = new StringBuilder(suffix.length());
//        reverser.append(suffix).reverse();
//        // TST returns sorted keys and so makes it inefficient to add them into another TST
//        Collection<String> reversed = reversedTermPostingPath.keysWithPrefix(reverser.toString());
//        List<String> straight = new ArrayList<>(reversed.size());
//        for (String rev : reversed)
//            straight.add(reversedTermPostingPath.get(rev));
//        return straight;
    }

    // #endregion

    public String getDocName(int docID) {
        return Paths.get(docId.inverse().get(docID)).getFileName().toString();
    }

    public Path getDocPath(int docID) {
        return Paths.get(docId.inverse().get(docID));
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
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.nanoTime();
        System.out.println("time: " + (endTime - startTime) / 1e9);
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
            //List<OutEntry> outers = new ArrayList<>();
            for (File docFile : files) {
                vocabulary = new TreeMap<>(); // term - positions in current doc, tf of a term == length of the list
                int docID = docId.get(docFile.toString());
                TermProvider terms = new TermProvider(docFile, morph);

                for (int n = 0; terms.hasNextTerm(); n++) {
                    String term = terms.nextTerm();
                    vocabulary.putIfAbsent(term, new ArrayList<>());
                    vocabulary.get(term).add(n);

                    Runtime rt = Runtime.getRuntime();
                    if (rt.maxMemory() - rt.freeMemory() > WORKER_MEMORY) {
                        mergeOut(new OutEntry(docID, vocabulary.entrySet()));
                        vocabulary = new TreeMap<>();
                        //outers.add(new OutEntry(docID, vocabulary.entrySet()));
                        //mergeOut(outers);
                        //outers = new ArrayList<>();
                    }
                }
                mergeOut(new OutEntry(docID, vocabulary.entrySet()));
                //outers.add(new OutEntry(docID, vocabulary.entrySet()));
            }
            System.out.println("task completed, total: " + files.length + " time: " + getTime());
            completion.countDown();
        }

    }

    /*
     * the second versions of 'mergeOut' is intended to facilitate optimization of passing a list<OutEntry> so that
     *     time used for synchronization is reduced, but in practise it just words randomly and is buggy
     * */
    private synchronized void mergeOut(OutEntry microMaps) {
        for (Map.Entry<String, ArrayList<Integer>> entry : microMaps.getVocabulary()) {
            dictionary.putIfAbsent(entry.getKey(), new HashMap<>());
            dictionary.get(entry.getKey()).merge(microMaps.getDocId(), entry.getValue(), (oldVal, newVal) -> {
                oldVal.addAll(newVal);
                return oldVal;
            });
        }

        Runtime rt = Runtime.getRuntime();
        if (rt.maxMemory() - rt.freeMemory() > MAX_MEMORY_LOAD) saveParticles();
    }
//    /**
//     * IMPORTANT IDEA, DO NOT DELETE!!!
//     * merges small local dictionaries into a big global
//     *
//     * @param microMaps - list of particle of dictionary built during a processing period
//     */
//    private synchronized void mergeOut(List<OutEntry> microMaps) {
//        while (!microMaps.isEmpty()) {
//            OutEntry out = microMaps.get(microMaps.size() - 1);
//            replenishDictionary(dictionary, out.getVocabulary(), out.getDocId());
//            microMaps.remove(microMaps.size() - 1); // clear unnecessary memory
//            Runtime rt = Runtime.getRuntime();
//            if (rt.maxMemory() - rt.freeMemory() > MAX_MEMORY_LOAD) {
//                saveParticles();
//            }
//        }
//    }

    private void saveParticles() {
        String pathNameDict = String.format(TEMP_PARTICLES, numStoredDictionaries++);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(pathNameDict)))) {
            for (Map.Entry<String, HashMap<Integer, ArrayList<Integer>>> entry : dictionary.entrySet()) {
                bw.write(entry.getKey());
                // <term, Map<docID, positions>> size of map == df, size of positions == tf

                for (Integer docID : new TreeSet<>(entry.getValue().keySet())) {
                    bw.write(NEXT_DOC_SEP);
                    bw.write(valueOf(docID));
                    bw.write(TF_SEP);
                    bw.write(valueOf(entry.getValue().get(docID).size()));
                    bw.write(DOC_COORD_SEP);

                    Iterator<Integer> postings = entry.getValue().get(docID).iterator();
                    if (postings.hasNext())
                        while (true) {
                            bw.write(valueOf(postings.next()));
                            if (!postings.hasNext()) break;
                            bw.write(COORD_SEP);
                        }
                    entry.getValue().remove(docID);
                }
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        dictionary = new TreeMap<>();
        System.gc();
    }

    private void transferMerge() throws IOException {
        saveParticles();
        dictionary = null;

        List<TermData> termData = new ArrayList<>();
        int prevTupleIndex = 0;
        int numFlecks = 0;
        int numTermsPerFleck = 1000;
        String path = String.format(INDEX_FLECKS, numFlecks);
        BufferedOutputStream fleck = new BufferedOutputStream(new FileOutputStream(new File(path)));
        long writtenBytes = 0; //for fleck

        System.out.println("Start transfer Merge");

        PriorityQueue<PTP> termsPQ = initPTP();
        for (int i = 0; !termsPQ.isEmpty(); i++) {
            if (i > numTermsPerFleck) {
                path = String.format(INDEX_FLECKS, ++numFlecks);
                fleck.close();

                String editPath = String.format(INDEX_FLECKS, termData.get(prevTupleIndex).getFleckID());
                RandomAccessFile raf = new RandomAccessFile(editPath, "rw");
                while (prevTupleIndex < termData.size()) {
                    TermData nextTermData = termData.get(prevTupleIndex++);
                    raf.seek(nextTermData.getFleckPos());
                    raf.writeInt(nextTermData.getDocFr());
                }
                raf.close();

                fleck = new BufferedOutputStream(new FileOutputStream(new File(path)));
                writtenBytes = 0;
                i = 0;
            }

            for (PTP p : termsPQ)
                assert p.getCurrState() == PTP.State.DOC_ID;

            PTP termHead = termsPQ.poll();

            assert termHead != null;

            TermData currentTermTermData = new TermData(termHead.getCurrTerm(), numFlecks, writtenBytes);

            PriorityQueue<PTP> docIdPQ = new PriorityQueue<>(PTP.getComparator(PTP.State.DOC_ID));
            docIdPQ.add(termHead.getNext());

            Set<Integer> docsFr = new HashSet<>();
            while (!termsPQ.isEmpty() && termHead.equalsTerm(termsPQ.peek())) {// come through all terms
                PTP sameTerm = termsPQ.poll();

                assert sameTerm != null;

                docsFr.add(sameTerm.getNext().getCurrDocID()); // state == post docID
                docIdPQ.add(sameTerm); // there will be lots of elements but only few duplicates
                // (customer's file was split into 2+ particles)
            }

            for (PTP p : docIdPQ)
                assert p.getCurrState() == PTP.State.TERM_FR;

            // they will all have the same term
            // write filler bytes for int docFr
            for (int j = 0; j < Integer.BYTES; j++) {
                fleck.write(0xff);
                writtenBytes++;
            }
            int deltaDocId = 0;
            while (!docIdPQ.isEmpty()) {
                PriorityQueue<PTP> posPQ = new PriorityQueue<>(PTP.getComparator(PTP.State.POSITION));
                PTP docIdHead = docIdPQ.poll();

                assert docIdHead != null;

                int totalTermFr = docIdHead.getNext().getCurrTermFr(); // state == POSITION
                posPQ.add(docIdHead.getNext()); // state == post POSITION (possible term or docId or EOF)
                while (!docIdPQ.isEmpty() && docIdHead.equalsDocId(docIdPQ.peek())) {// come through all docs of current term
                    PTP sameDocId = docIdPQ.poll();

                    assert sameDocId != null;

                    totalTermFr += sameDocId.getNext().getCurrTermFr();
                    posPQ.add(sameDocId.getNext());
                }

                deltaDocId = docIdHead.getCurrDocID() - deltaDocId;
                writtenBytes += intToFleck(fleck, deltaDocId);
                writtenBytes += intToFleck(fleck, totalTermFr);

                while (!posPQ.isEmpty()) { // come through all coords of current doc
                    PTP nextPos = posPQ.poll();

                    assert nextPos != null; // nextPos cannot be null because at least docIdHead is in posPQ
                    int deltaPos = nextPos.getCurrPos();
                    writtenBytes += intToFleck(fleck, deltaPos);

                    while (nextPos.getCurrState() == PTP.State.POSITION) {
                        deltaPos = nextPos.getNext().getCurrPos() - deltaPos;
                        writtenBytes += intToFleck(fleck, deltaPos);
                    }

                    switch (nextPos.getCurrState()) {
                        case TERM:
                            // switch to the post-TERM state (update term)
                            termsPQ.add(nextPos.getNext());
                            break;
                        case DOC_ID:
                            // switch to the post-DOC_ID state (update docId)
                            docIdPQ.add(nextPos.getNext());
                            break;
                        case EOF:
                            break;
                    }
                }
            }
            currentTermTermData.setDocFr(docsFr.size());
            termData.add(currentTermTermData);
        }
        TermData[] sortedTermData = termData.toArray(new TermData[0]);
        index = new IndexBody(sortedTermData, INDEX_FLECKS);
    }

    private long intToFleck(BufferedOutputStream fleck, int number) throws IOException {
        byte[] vlc = new byte[5]; // 2^4*7 < 2^31 < 2^5*7

        byte written = 5;
        while (true) {
            vlc[--written] = (byte) (number % 128);
            if (number <= 128) break;
            number /= 128;
        }
        vlc[vlc.length - 1] += 128;

        fleck.write(vlc, written, vlc.length - written);
        return written;
    }

    private PriorityQueue<PTP> initPTP() throws IOException {
        PriorityQueue<PTP> ptpPQ = new PriorityQueue<>(PTP.getComparator(PTP.State.TERM));
        for (int i = 0; i < numStoredDictionaries; i++) {
            PTP nextPTP = new PTP(new File(String.format(TEMP_PARTICLES, i)));
            nextPTP.getNext(); // change state to DOC_FR and set cTerm
            ptpPQ.add(nextPTP);
        }
        return ptpPQ;
    }

    // ParticleTokenProvider
    static class PTP {

        enum State { // state to which the next char refers ":" and ">" change the state
            TERM, DOC_ID, TERM_FR, POSITION, EOF
        }

        private static final boolean isWindows = System.lineSeparator().length() != 1;
        private final StringBuilder builder = new StringBuilder();
        private final BufferedReader br;
        private State currentState;
        private String cTerm;
        private String nextTerm;
        private int cDocID;
        private int cPos;
        private int cTermFr;

        PTP(File particle) throws IOException {
            br = new BufferedReader(new FileReader(particle));
            currentState = State.TERM;
        }

        PTP getNext() {
            try {
                return fill();
            } catch (IOException e) {
                throw new RuntimeException("Internal error", e);
            }
        }

        private PTP fill() throws IOException {
            if (nextTerm != null && currentState == State.TERM) {
                cTerm = nextTerm;
                nextTerm = null;
                currentState = State.DOC_ID;
                return this;
            }

            char nextChar = (char) br.read();
            if (nextChar == (char) -1) return null;
            while (nextChar != NEXT_DOC_SEP && nextChar != DOC_COORD_SEP
                    && nextChar != TF_SEP && nextChar != COORD_SEP
                    && nextChar != System.lineSeparator().charAt(0) && nextChar != (char) -1) {
                builder.append(nextChar);
                nextChar = (char) br.read();
            }

            String update = builder.toString();
            if (currentState == State.EOF) {
                builder.setLength(0);
                nextTerm = update;
                return this;
            }

            switch (currentState) {
                case TERM:
                    assert (nextChar == NEXT_DOC_SEP);
                    cTerm = update;
                    currentState = State.DOC_ID;
                    break;
                case DOC_ID:
                    assert (nextChar == TF_SEP);
                    cDocID = Integer.parseInt(update);
                    currentState = State.TERM_FR;
                    break;
                case TERM_FR:
                    assert (nextChar == DOC_COORD_SEP);
                    cTermFr = Integer.parseInt(update);
                    currentState = State.POSITION;
                    break;
                case POSITION:
                    assert (nextChar != DOC_COORD_SEP);
                    cPos = Integer.parseInt(update);
                    if (nextChar == NEXT_DOC_SEP) currentState = State.DOC_ID;
                    break;
            }
            builder.setLength(0);
            if (nextChar == System.lineSeparator().charAt(0)) {
                if (isWindows) br.skip(1);
                // suspect EOF, if not EOF then save next term and change state to TERM
                // read saved term with next invocation
                currentState = State.EOF;
                if (fill() == null) br.close();
                else currentState = State.TERM;
            }
            return this;
        }

        static Comparator<PTP> getComparator(State mode) {
            switch (mode) {
                case TERM:
                    return Comparator.comparing(PTP::getCurrTerm);
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

        boolean equalsDocId(PTP peek) {
            return cDocID == peek.cDocID;
        }

        State getCurrState() {
            return currentState;
        }

        String getCurrTerm() {
            return cTerm;
        }

        int getCurrDocID() {
            return cDocID;
        }

        int getCurrPos() {
            return cPos;
        }

        int getCurrTermFr() {
            return cTermFr;
        }
    }

    /**
     * parses string representation of term's docID-coords set into a map
     *
     * @param line representing line postings with associated coordinates
     * @return HashMap of posting-coords sets
     */
    private Map<Integer, ArrayList<Integer>> parseCoords(String line) {
        throw new NotImplementedException();
//        String[] spl_1 = line.split(valueOf(NEXT_DOC_SEP)); // 0th is the term then docID-positions sets
//        Map<Integer, ArrayList<Integer>> result = new HashMap<>();
//        for (int i = 1; i < spl_1.length; i++) {
//            int coordStart = spl_1[i].indexOf(DOC_COORD_SEP);
//            String[] coordTokens = spl_1[i].substring(coordStart + 1).split("\\s"); // 1 = length of DOC_COORD_SEP
//            ArrayList<Integer> coordList = new ArrayList<>(coordTokens.length);
//            for (String coord : coordTokens)
//                coordList.add(Integer.parseInt(coord));
//            result.put(Integer.parseInt(spl_1[i].substring(0, coordStart)), coordList);
//        }
//        return result;
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

    private String showMemory() {
        Runtime rt = Runtime.getRuntime();
        return String.format("FREE memory: %.2f%%", (double) rt.freeMemory() / rt.maxMemory() * 100);
    }

    private long startTime = System.currentTimeMillis();

    private long getTime() {
        return (System.currentTimeMillis() - startTime) / 1000;
    }
}