package ukma.ir;

import ukma.ir.index.IndexService;
import ukma.ir.index.helpers.containers.CoordVector;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class QueryProcessor {
    private static final Object locker = new Object();
    private static QueryProcessor instance;
    private final IndexService indexService;

    public static QueryProcessor initQueryProcessor(IndexService index) {
        synchronized (locker) {
            if (instance == null)
                return instance = new QueryProcessor(index);
            throw new IllegalStateException("QueryProcessor has already been initialized.");
        }
    }

    public static QueryProcessor getInstance() {
        synchronized (locker) {
            if (instance == null)
                throw new IllegalStateException("QueryProcessor has not been initialized.");
            return instance;
        }
    }

    private QueryProcessor(IndexService service) {
        indexService = service;
    }

    // they said java is too verbose...
    private void decode(int[] encoded) {
        for (int i = 0, nextDoc = 0; i < encoded.length; encoded[i] = nextDoc += encoded[i++]) ;
    }

    private void addAllDecoded(Collection<Integer> collection, int[] postings) {
        for (int i = 0, nextDoc = 0; i < postings.length; collection.add(nextDoc += postings[i++])) ;
    }

    /**
     * @param q - trimmed query to be processed
     * @return list of documents that match a given query
     */
    public List<String> processBooleanQuery(String q) {
        if (!q.matches("\\s*\\w[\\w\\s]*")) throw new IllegalArgumentException("incorrect input");
//        if (!q.matches("^\\s*(NOT)?\\s+\\w+?\\s*((AND(\\s+NOT)?|OR)\\s+\\w+?\\s*)*$")) throw new IllegalArgumentException("incorrect input");
        Set<Integer> reminders = new HashSet<>();
        Arrays.stream(q.split("\\s*OR\\s*"))
                .map(union -> union.split("\\s*AND\\s*"))
                .forEach(unionParticle -> {
                    List<String> includeTerms = new ArrayList<>();
                    List<String> excludeTerms = new ArrayList<>();
                    for (String token : unionParticle)
                        if (token.startsWith("NOT")) // normalizer might return null
                            excludeTerms.add(IndexService.normalize(token.substring(token.lastIndexOf(' ') + 1)));
                        else includeTerms.add(IndexService.normalize(token));

                    // if there are terms to include and one of these terms is in dictionary (in this case - 0th),
                    // i.e. input is not empty
                    // then set posting for this term as a base for further intersections
                    // else do no intersection as the result is empty if any entry is empty
                    Set<Integer> inDocs;
                    int startIndex = firstNotNull(includeTerms);
                    if (startIndex != -1) {
                        int[] postings = indexService.getPostings(includeTerms.get(startIndex));
                        inDocs = new HashSet<>(postings.length, 1); // do not expand
                        addAllDecoded(inDocs, postings);

                        includeTerms.forEach(term -> {
                            int[] postingArr = indexService.getPostings(term);
                            decode(postingArr);
                            List<Integer> postList = new ArrayList<>(postingArr.length);
                            for (int doc : postingArr) // couldn't find moore efficient way to intersect
                                postList.add(doc);
                            inDocs.retainAll(postList);
                        });
                    } else return;

                    Set<Integer> exDocs = new HashSet<>();
                    for (String term : excludeTerms) {
                        if (term == null) continue;
                        int[] posting = indexService.getPostings(term);
                        if (posting != null)
                            addAllDecoded(exDocs, posting);
                    }

                    inDocs.removeAll(exDocs);
                    reminders.addAll(inDocs);
                });

        List<String> result = new ArrayList<>(reminders.size());
        reminders.forEach(id -> result.add(indexService.getDocName(id)));
        result.sort(String::compareTo);
        return result;
    }

    private static <E> int firstNotNull(List<E> list) {
        // not index access because of possible LinkedList
        int index = 0;
        for (E item : list)
            if (item != null) return index;
            else index++;
        return -1;
    }

    public List<String> processPositionalQuery(String query) {
        //example: term1 /1 term2 /2 term3
        if (!query.matches("\\w+(\\s+/\\d+\\s+\\w+)*"))
            throw new IllegalArgumentException("Wrong input format");
        String[] tokens = query.split("\\s+");
        String[] terms = new String[tokens.length / 2 + 1];
        int[] distance = new int[tokens.length / 2];

        for (int i = 0, j = 0; i < tokens.length; j++, i += 2) {
            terms[j] = IndexService.normalize(tokens[i]);
            if (!indexService.containsElement(terms[j])) return new ArrayList<>(0);
        }

        for (int i = 1, j = 0; i < tokens.length; j++, i += 2)
            distance[j] = Integer.parseInt(tokens[i].substring(1));

        Set<Integer> relevant = new TreeSet<>();
        for (int i = 0; i + 1 < terms.length; i++)
            positionalIntersect(terms[i], terms[i + 1], distance[i])
                    .forEach(arr -> relevant.add(arr[0]));

        return relevant.stream().map(indexService::getDocPath).map(Path::getFileName)
                .map(Path::toString).sorted().collect(Collectors.toList());
    }

    /**
     * decode data of each vector
     *
     * @param v - vector
     * @return array of decoded docIDs of the passed vector
     */
    private int[] decodeCoordVectors(CoordVector[] v) {
        int[] docIDs = new int[v.length];
        for (int i = 0, nextDoc = 0; i < docIDs.length; i++)
            docIDs[i] = nextDoc += v[i].getDocID();

        for (int i = 0; i < v.length; i++) {//i = 152 - wrong coords
            decode(v[i].getCoords());
        }
        return docIDs;
    }

    private List<int[]> positionalIntersect(String t1, String t2, int dist) {
        ArrayList<int[]> answer = new ArrayList<>();
        CoordVector[] v1 = indexService.getTermData(t1);
        CoordVector[] v2 = indexService.getTermData(t2);

        int[] docIDs1 = decodeCoordVectors(v1);
        int[] docIDs2 = decodeCoordVectors(v2);

        for (int i = 0, j = 0; i < docIDs1.length && j < docIDs2.length; ) {
            if (docIDs1[i] == docIDs2[j]) {
                Queue<Integer> candidates = new ArrayDeque<>();
                for (Integer pos1 : v1[i].getCoords()) {
                    for (Integer pos2 : v2[j].getCoords())
                        if (Math.abs(pos1 - pos2) <= dist) candidates.add(pos2);
                        else if (pos2 > pos1) break;


                    while (candidates.size() > 0 && Math.abs(candidates.peek() - pos1) > dist) candidates.remove();
                    for (int k = 0; k < candidates.size(); k++)
                        answer.add(new int[]{docIDs1[i], pos1, candidates.remove()});
                }
                i++;
                j++;
            } else if (docIDs1[i] < docIDs2[j]) i++;
            else j++;
        }
        return answer;
    }

    // a*b*c && *abc && abc* && *ab*c && a*bc*
    // NOT *abc* / ab**c
    public Collection<String> processJokerQuery(String query) {
        String jokerWordPattern = "(\\*?\\w+(\\*\\w+)?)|(\\w+\\*?\\w*(\\w\\*\\w*)?)";
        String queryPattern = jokerWordPattern + "(\\s+(" + jokerWordPattern + "))*";
        if (!query.matches(queryPattern)) return new ArrayList<>(0);
        String[] tokens = query.split("\\s+");
        for (String token : tokens)
            if (!token.matches(jokerWordPattern))
                throw new IllegalArgumentException('"' + query + '"' + " is not a valid joker query");

        for (int i = 0; i < tokens.length; i++)
            tokens[i] = tokens[i].toLowerCase(); // stemmer: searchings -> searching; searching -> search

        Set<Integer> validFiles = new HashSet<>();
        for (String term : tokens) {
            Set<Integer> termContribution = new HashSet<>();
            Set<String> matchPref = new TreeSet<>();
            Set<String> matchSuf = new TreeSet<>();

            if (term.indexOf('*') == -1) {
                String normTerm = IndexService.normalize(term);
                if (normTerm == null) continue;
                int[] postings = indexService.getPostings(normTerm);
                addAllDecoded(termContribution, postings);
                validFiles.retainAll(termContribution);
                continue;
            }

            if (term.charAt(0) != '*') {
                matchPref.addAll(Arrays.asList(indexService.startWith(term.substring(0, term.indexOf('*')))));
                // no terms match given non-empty prefix
                if (matchPref.isEmpty()) return new ArrayList<>(0);
            }

            if (term.charAt(term.length() - 1) != '*') {
                matchSuf.addAll(Arrays.asList(indexService.endWith(term.substring(term.lastIndexOf('*') + 1))));
                if (matchSuf.isEmpty()) return new ArrayList<>(0);
            }

            Set<String> primeMatch;
            if (matchPref.isEmpty())
                primeMatch = matchSuf;
            else if (matchSuf.isEmpty())
                primeMatch = matchPref;
            else {
                matchPref.retainAll(matchSuf);
                primeMatch = matchPref;
            }

            Collection<String> valid;
            if (term.indexOf('*') != term.lastIndexOf('*')) {
                String pattern = ".*" + term.substring(term.indexOf('*') + 1, term.lastIndexOf('*')) + ".*";
                valid = primeMatch.stream().filter(t -> t.matches(pattern)).collect(Collectors.toList());
            } else valid = primeMatch;

            for (String v : valid) {
                int[] postings = indexService.getPostings(v);
                for (int i = 0, nextDoc = 0; i < postings.length; termContribution.add(nextDoc += postings[i++])) ;
            }

            if (validFiles.isEmpty()) validFiles.addAll(termContribution); // set base
            else validFiles.retainAll(termContribution); // intersect
            if (validFiles.isEmpty()) return new ArrayList<>(0); // check for empty intersection
        }
        List<String> result = new ArrayList<>();
        for (Integer docID : validFiles)
            result.add(indexService.getDocName(docID));
        return result;
    }
}