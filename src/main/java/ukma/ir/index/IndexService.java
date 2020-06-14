package ukma.ir.index;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import edu.stanford.nlp.process.Morphology;
import ukma.ir.index.helpers.*;
import ukma.ir.index.helpers.containers.CoordVector;
import ukma.ir.index.helpers.containers.DocVector;
import ukma.ir.index.helpers.containers.TermData;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import static java.lang.String.valueOf;
import static ukma.ir.index.helpers.utils.VLC.writeVLC;

/*
TODO: <term> [docFr] [(docID TermFr)_0 ... (docID TermFr)_n-1] [(coord_0 ... coord_i-1)_0 ... (coord_0 ... coord_j-1)_n-1]
                ||              ||                   ||
                 n               i                    j
*/

// -Xms5G -Xmx5G -XX:+UseG1GC -XX:+UseStringDeduplication
public class IndexService {

    static {
        String rootPath;
        String osName = System.getenv("AppData").toUpperCase();
        if (osName.contains("WIN")) {
            rootPath = Paths.get(System.getenv("user.home"), "info-search").toString();
        } else if (osName.contains("MAC")) {
            rootPath = System.getProperty("user.home") + "/Library/Application Support/info-search";
        } else {
            rootPath = Paths.get(System.getProperty("user.home"), "info-search").toString();
        }
        APP_DATA_PATH = rootPath;
        TEMP_PARTICLES = Paths.get(rootPath, "data/dictionary/dp%d.txt").toString();
        INDEX_FLECKS = Paths.get(rootPath, "data/dictionary/indexFleck_%d.txt").toString();
        Path dictionaryPath = Paths.get(rootPath, "data/dictionary");
        File dirs = dictionaryPath.toFile();
        if (!dirs.exists()) dirs.mkdirs();
    }

    public static final String APP_DATA_PATH;
    private static final String TEMP_PARTICLES;// = "data/dictionary/dp%d.txt";
    private static final String INDEX_FLECKS;// = "data/dictionary/indexFleck_%d.txt";
    private static final int WORKERS = Runtime.getRuntime().availableProcessors();
    private static final long WORKERS_MEMORY_LIMIT = Math.round(Runtime.getRuntime().maxMemory() * 0.5);
    private static final long MAX_MEMORY_LOAD = Math.round(Runtime.getRuntime().maxMemory() * 0.7);
    private static final char NEXT_DOC_SEP = ':'; // TERM_DOC
    private static final char DOC_COORD_SEP = '>'; // DOC_COORDINATE
    private static final char COORD_SEP = ' ';
    private static final char TF_SEP = '#'; // TERM_FREQUENCY
    private Path library;

    private final BiMap<String, Integer> docId; // path - docId
    // term - id of the corresponding index file
    private IndexBody index;
    private static volatile IndexService instance; // effectively final once initialized
    private static final Morphology MORPH = new Morphology();
    private DocVector[] docVectors;
    private Map<Integer, DocVector[]> clusters;

    // building-time variables
    private int numStoredParticles;
    private final CountDownLatch completion = new CountDownLatch(WORKERS);
    // hash function is effective for Integer
    // <term, Hash<docID, positions>> size of map == df, size of positions == tf
    private TreeMap<String, HashMap<Integer, ArrayList<Integer>>> dictionary;
    private static final Object locker = new Object();
    private static long startTime = System.currentTimeMillis();

    private static long getTime() {
        return (System.currentTimeMillis() - startTime) / 1000;
    }

    private IndexService(IndexBody index, DocVector[] docVectors, Map<Integer, DocVector[]> clusters, BiMap<String, Integer> docId) {
        this.index = index;
        this.docVectors = docVectors;
        this.clusters = clusters;
        this.docId = docId;
    }

    private IndexService(Path libraryPath) {
        docId = HashBiMap.create();
        library = libraryPath;
        buildInvertedIndex();
    }

    public static IndexService loadCache() throws CacheException {
        synchronized (locker) {
            if (!CacheManager.filesPresent()) throw new CacheException("Cache file(s) missing.");
            try {
                IndexBody index = CacheManager.loadCache(CacheManager.Fields.INDEX);
                DocVector[] docVectors = CacheManager.loadCache(CacheManager.Fields.DOC_VECTORS);
                Map<Integer, DocVector[]> clusters = CacheManager.loadCache(CacheManager.Fields.CLUSTERS);
                BiMap<String, Integer> docId = CacheManager.loadCache(CacheManager.Fields.PATH_DOC_ID_MAP);
                return instance = new IndexService(index, docVectors, clusters, docId);
            } catch (IOException | ClassNotFoundException e) {
                throw new CacheException("Cache data has been corrupted.", e);
            }
        }
    }

    public static IndexService buildIndex(Path path) {
        synchronized (locker) {
            return instance = new IndexService(path);
        }
    }

    public static boolean hasInstance() {
        return instance != null;
    }

    public static IndexService getInstance() {
        synchronized (locker) {
            if (instance == null) throw new IndexNotBuiltException();
            return instance;
        }
    }

    public static void clearCache() {
        CacheManager.deleteCache();
    }

    // for GUI
    public enum IndexType {
        BOOLEAN, COORDINATE, JOKER/*, CLUSTER*/
    }

    public boolean containsElement(String term) {
        return index.containsElement(term);
    }

    public CoordVector[] getTermData(String term) {
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

    private void buildInvertedIndex() {
        System.out.println("start building");
        long startTime = System.nanoTime();
        dictionary = new TreeMap<>();

        try (Stream<Path> files = Files.walk(library)) {
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
            long avLength = (documents[0].length() + documents[documents.length - 1].length() + documents[documents.length / 2].length()) / 3;
            long avWordsNum = avLength / 5;
            System.out.println("start transferMerge: " + getTime());
            transferMerge(avWordsNum);
            showFreeMemory();
            System.out.println("build vectors: " + getTime());
            docVectors = Clusterizer.buildDocVectors(documents.length, index);
            System.out.println("build clusters, time: " + getTime());
            clusters = Clusterizer.buildClusters(docVectors);
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.nanoTime();
        System.out.println("Total time: " + (endTime - startTime) / 1e9);
        CacheManager.saveCache(docId, index, docVectors, clusters);
    }

    public DocVector buildQueryVector(String[] query) {
        // use map of term - quantity (tf for query)
        Map<Integer, Double> queryData = new HashMap<>(query.length);
        for (int i = 0; i < query.length; i++) {
            String term = query[i];
            CoordVector[] termData = index.getTermData(term);
            if (termData == null) continue; // if not from vocabulary then skip
            queryData.put(index.getTermDictPos(term), Math.log((double) docVectors.length / (termData[0].length)));
        }
        return new DocVector(queryData);
    }

    public List<Integer> getCluster(DocVector inDoc) {
        int closestLeader = -1;
        double sim = 0;
        for (Integer nextLeader : clusters.keySet()) {
            double nextSim = docVectors[nextLeader].cosineSimilarity(inDoc);
            if (nextSim > sim) {
                closestLeader = nextLeader;
                sim = nextSim;
            }
        }

        if (closestLeader == -1) return null;
        TreeMap<Double, Integer> sortedCluster = new TreeMap<>();
        sortedCluster.put(sim, closestLeader);
        DocVector[] followers = clusters.get(closestLeader);
        if (followers != null)
            for (DocVector vec : followers) {
                double flrSim = vec.cosineSimilarity(inDoc);
                sortedCluster.put(flrSim, vec.getOrdinal());
            }
        List<Integer> result = new ArrayList<>(sortedCluster.size());
        for (Map.Entry<Double, Integer> entry : sortedCluster.entrySet())
            result.add(entry.getValue());

        return result;
    }

    private void showFreeMemory() {
        Runtime rt = Runtime.getRuntime();
        System.out.println("Free memory: " + (double) rt.freeMemory() / rt.maxMemory());
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
                    }
                }
                mergeOut(new OutEntry(docID, vocabulary.entrySet()));
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

    private void saveParticles() {
        String pathNameDict = String.format(TEMP_PARTICLES, numStoredParticles++);
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

    private void transferMerge(long numTermsPerFleck) throws IOException {
        saveParticles();
        dictionary = null;

        List<TermData> termData = new ArrayList<>();
        int tupleIndex = 0;
        int numFlecks = 0;
        String path = String.format(INDEX_FLECKS, numFlecks);
        BufferedOutputStream fleck = new BufferedOutputStream(new FileOutputStream(new File(path)));
        long writtenBytes = 0; //for in-fleck position

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

                int lastCoord = 0;
                int currCoord;
                int coordsWritten = 0;
                while (!posPQ.isEmpty()) { // come through all coords of current doc
                    PTP nextPos = posPQ.poll();

                    // nextPos cannot be null because at least docIdHead is in posPQ
                    currCoord = nextPos.getCurrPos();
                    writtenBytes += writeVLC(fleck, currCoord - lastCoord);
                    lastCoord = currCoord;
                    coordsWritten++;

                    while (nextPos.getCurrState() == PTP.State.POSITION) {
                        currCoord = nextPos.getNext().getCurrPos();
                        writtenBytes += writeVLC(fleck, currCoord - lastCoord);
                        lastCoord = currCoord;
                        coordsWritten++;
                    }

                    switch (nextPos.getCurrState()) {
                        case TERM:
                            // switch to the post-BOOLEAN state (update term)
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
                assert (coordsWritten == totalTermFr);
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
        TermData[] unsortedTermData = termData.toArray(new TermData[0]);
        index = new IndexBody(unsortedTermData, INDEX_FLECKS);
    }

    private PriorityQueue<PTP> initPTP() throws IOException {
        PriorityQueue<PTP> ptpPQ = new PriorityQueue<>(PTP.getComparator(PTP.State.TERM));
        for (int i = 0; i < numStoredParticles; i++) {
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

        private static final boolean IS_WINDOWS = System.lineSeparator().length() != 1;
        private final StringBuilder builder = new StringBuilder();
        private final BufferedReader br;
        private final Path path;
        private State currentState;
        private String cTerm;
        private String nextTerm;
        private int cDocID;
        private int cPos;
        private int cTermFr;

        PTP(File particle) throws IOException {
            br = new BufferedReader(new FileReader(particle));
            path = particle.toPath();
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
                if (IS_WINDOWS) br.skip(1);
                // suspect EOF, if not EOF then save next term and change state to BOOLEAN
                // read saved term with next invocation
                currentState = State.EOF;
                if (fill() == null) {
                    br.close();
                    Files.delete(path);
                } else currentState = State.TERM;
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
            byte hasNext = checkNextTerm();
            while (hasNext == 0) hasNext = checkNextTerm();
            return hasNext == 1;
        }

        /**
         * checks next token for presence and validness
         *
         * @return -1 if next term is absent<br>1 if present<br>0 if next term is not valid
         */
        private byte checkNextTerm() {
            if (lineTokens == null || nextTokenIndex == lineTokens.length) {
                try {
                    String nextLine = br.readLine();
                    if (nextLine == null) return -1;
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
                    return 1; // not let too long codes (may be hex) that are likely to be useless, but allow long words
                nextTokenIndex++;
            }
            return 0;
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