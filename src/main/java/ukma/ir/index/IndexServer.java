package ukma.ir.index;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import edu.stanford.nlp.process.Morphology;
import ukma.ir.index.helpers.DocVector;
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
import static ukma.ir.index.helpers.VLC.writeVLC;

/*
TODO: [docFr] [(docID TermFr)_0 ... (docID TermFr)_n-1] [(coord_0 ... coord_i-1)_0 ... (coord_0 ... coord_j-1)_n-1]
         ||              ||                   ||
          n               i                    j
*/

public class IndexServer {

    static {
        // no time for AppData path
        String path = "data/dictionary";
        File dirs = new File(path);
        dirs.mkdirs();
    }

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
    private static IndexServer instance; // effectively final
    private static final Morphology MORPH = new Morphology();
    private DocVector[] docVectors;
    private HashMap<Integer, DocVector[]> clusters;

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


    public boolean containsElement(String term) {
        return index.containsElement(term);
    }

    public int[][] getTermData(String term) {
        return index.getTermData(term);
    }

    public int[] getPostings(String term) {
        return index.getPostings(term);
    }

    public String[] startWith(String prefix) {
        return index.startWith(prefix);
    }

    public String[] endWith(String suffix) {
        return index.endWith(suffix);
    }

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

    private long startTime = System.currentTimeMillis();

    private long getTime() {
        return (System.currentTimeMillis() - startTime) / 1000;
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
            System.out.println("build vectors");
            showFreeMemory();
            docVectors = new DocVector[documents.length];
            buildDocVectors();
            System.out.println("build clusters, time: " + getTime());
            buildClusters();
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.nanoTime();
        System.out.println("time: " + (endTime - startTime) / 1e9);
    }

    private void showFreeMemory() {
        Runtime rt = Runtime.getRuntime();
        System.out.println("Free memory: " + (double) rt.freeMemory() / rt.maxMemory());
    }

    private void buildClusters() {
        Map<Integer, Integer> leaders = chooseLeaders();
        Follower[] followers = new Follower[docVectors.length - leaders.size()];
        for (int i = 0; i < followers.length; i++) followers[i] = new Follower();

        for (int leader : leaders.keySet()) {
            for (int flr = 0; flr < docVectors.length; flr++) {
                if (leaders.containsKey(flr)) continue;
                double sim = docVectors[leader].cosineSimilarity(docVectors[flr]);
                if (followers[flr - flr / leaders.size()].sim < sim) {
                    leaders.put(followers[flr].leader, Math.max(leaders.get(followers[flr].leader) - 1, 0));
                    followers[flr].leader = leader;
                    leaders.put(followers[flr].leader, leaders.get(followers[flr].leader) + 1);
                    followers[flr].sim = (float) sim;
                }
            }
        }
        System.out.println("stage 1");
        clusters = new HashMap<>(leaders.size());
        for (Map.Entry<Integer, Integer> leaderStat : leaders.entrySet()) {
            clusters.put(leaderStat.getKey(), new DocVector[leaderStat.getValue()]);
        }

        System.out.println("stage 2");

        int[] leaderIndices = new int[leaders.size()];
        for (int i = 0; i < docVectors.length; i++) {
            if (leaders.containsKey(i)) continue;
            Follower flr = followers[i - i / leaders.size()];
            clusters.get(flr.leader)[leaderIndices[flr.leader]++] = docVectors[i];
        }
        System.out.println("stage 3");

    }

    private Map<Integer, Integer> chooseLeaders() {
        int step = (int) Math.sqrt(docVectors.length);
        int nextLeader = 0;
        HashMap<Integer, Integer> leaders = new HashMap<>();
        for (int i = 0; i < step; i++)
            leaders.put(nextLeader += step, 0);
        return leaders;
    }

    private void buildDocVectors() {
        for (int i = 0; i < docVectors.length; i++)
            docVectors[i] = new DocVector(i);

        Iterator<String> vocabulary = index.iterator();
        for (int i = 0; vocabulary.hasNext(); i++) {
            int[][] termData = index.getTermData(vocabulary.next());
            int termFr = termData.length;
            for (int[] termDatum : termData) {
                int docID = termDatum[0];
                docVectors[docID].addTermScore(i, termFr * Math.log((double) docVectors.length / (termData[0].length - 1)));
            }
        }
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

    // suspect bugs on writing vlc after docFr 35th byte -> int vlc vlc [ok] -> int [wrong] ...
    private void transferMerge() throws IOException {
        saveParticles();
        dictionary = null;

        List<TermData> termData = new ArrayList<>();
        int tupleIndex = 0;
        int numFlecks = 0;
        int numTermsPerFleck = 10000;
        String path = String.format(INDEX_FLECKS, numFlecks);
        BufferedOutputStream fleck = new BufferedOutputStream(new FileOutputStream(new File(path)));
        long writtenBytes = 0; //for in-fleck position

        System.out.println("Start transfer Merge");

        PriorityQueue<PTP> termsPQ = initPTP();
        for (int i = 0; !termsPQ.isEmpty(); i++) {

            for (PTP p : termsPQ)
                assert p.getCurrState() == PTP.State.DOC_ID;

            PTP termHead = termsPQ.poll();

            assert termHead != null;

            TermData currentTermData = new TermData(termHead.getCurrTerm(), numFlecks, writtenBytes);

            PriorityQueue<PTP> docIdPQ = new PriorityQueue<>(PTP.getComparator(PTP.State.DOC_ID));
            docIdPQ.add(termHead.getNext()); // state == post docID
// add termHead.term to docsFr
            Set<Integer> docsFr = new HashSet<>();
            while (!termsPQ.isEmpty() && termHead.equalsTerm(termsPQ.peek())) {// come through all terms
                PTP sameTerm = termsPQ.poll();

                assert sameTerm != null;

                docIdPQ.add(sameTerm.getNext());  // state == post docID
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

            int prevDocId = 0;
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

                docsFr.add(docIdHead.getCurrDocID());
                writtenBytes += writeVLC(fleck, -(prevDocId - (prevDocId = docIdHead.getCurrDocID())));
                writtenBytes += writeVLC(fleck, totalTermFr);

                while (!posPQ.isEmpty()) { // come through all coords of current doc
                    PTP nextPos = posPQ.poll();

                    assert nextPos != null; // nextPos cannot be null because at least docIdHead is in posPQ
                    writtenBytes += writeVLC(fleck, nextPos.getCurrPos());

                    while (nextPos.getCurrState() == PTP.State.POSITION) {
                        writtenBytes += writeVLC(fleck, -(nextPos.getCurrPos() - nextPos.getNext().getCurrPos()));
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

            assert (docsFr.size() != 0);
            currentTermData.setDocFr(docsFr.size()); // == 0
            termData.add(currentTermData);

            if (i > numTermsPerFleck || termsPQ.isEmpty()) {
                fleck.close();

                String editPath = String.format(INDEX_FLECKS, termData.get(tupleIndex).getFleckID());
                RandomAccessFile raf = new RandomAccessFile(editPath, "rw");
                while (tupleIndex < termData.size()) {
                    TermData nextTermData = termData.get(tupleIndex++);
                    raf.seek(nextTermData.getFleckPos());
                    raf.writeInt(nextTermData.getDocFr());
                }
                raf.close();

                if (!termsPQ.isEmpty()) {
                    path = String.format(INDEX_FLECKS, ++numFlecks);
                    fleck = new BufferedOutputStream(new FileOutputStream(new File(path)));
                    writtenBytes = 0;
                    i = 0;
                }
            }
        }
        TermData[] sortedTermData = termData.toArray(new TermData[0]);
        index = new IndexBody(sortedTermData, INDEX_FLECKS);
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
                if (lineTokens[nextTokenIndex] != null
                        && (lineTokens[nextTokenIndex].length() < 12 // let any char sequences < 12 be in index to facilitate search by codes/numbers
                        || !lineTokens[nextTokenIndex].matches("(\\w*\\d)+\\w*")))
                    return true; // not let too long codes that are likely to be useless, but allow long words
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

}

class Follower {
    int leader;
    float sim;
}