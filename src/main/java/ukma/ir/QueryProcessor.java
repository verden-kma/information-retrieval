package ukma.ir;

import ukma.ir.index.IndexServer;
import ukma.ir.index.helpers.DocVector;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryProcessor {

    private final IndexServer indexService = IndexServer.getInstance();

    private void decode(int[] encoded) {
        for (int i = 0, nextDoc = 0; i < encoded.length; encoded[i] = nextDoc += encoded[i++]) ;
    }

    private void addAllDecoded(Collection<Integer> collection, int[] postings) {
        for (int i = 0, nextDoc = 0; i < postings.length; collection.add(nextDoc += postings[i++])) ;
    }

    /**
     * @param q - query to be processed
     * @return list of documents that match a given query
     */
    public List<String> processBooleanQuery(String q) {
        if (!q.matches("\\w[\\w\\s]*")) throw new IllegalArgumentException("incorrect input");
        Set<Integer> reminders = new HashSet<>();
        Arrays.stream(q.split(" OR "))
                .map(union -> union.split(" AND "))
                .forEach(unionParticle -> {
                    List<String> includeTerms = new ArrayList<>();
                    List<String> excludeTerms = new ArrayList<>();
                    for (String token : unionParticle)
                        if (token.startsWith("NOT "))
                            excludeTerms.add(IndexServer.normalize(token.substring(4)));
                        else includeTerms.add(IndexServer.normalize(token));

                    // if there are terms to include and one of these terms is in dictionary (in this case - 0th),
                    // i.e. input is not empty
                    // then set posting for this term as a base for further intersections
                    // else do no intersection as the result is empty if any entry is empty
                    Set<Integer> inDocs;
                    if (!includeTerms.isEmpty() && indexService.containsElement(includeTerms.get(0))) {
                        int[] postings = indexService.getPostings(includeTerms.get(0));
                        inDocs = new HashSet<>(postings.length, 1); // do not expand
                        addAllDecoded(inDocs, postings);

                        includeTerms.forEach(term -> {
                            int[] posting = indexService.getPostings(term);
                            if (posting != null) { // intersect
                                decode(posting);
                                for (int p : posting) // retainAll
                                    if (!inDocs.contains(p))
                                        inDocs.remove(p);
                            }
                        });
                    } else return;

                    Set<Integer> exDocs = new HashSet<>();
                    for (String term : excludeTerms) {
                        if (term == null) continue; // normalizer might return null
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

    public List<String> processPositionalQuery(String query) throws IOException {
        if (!query.matches("\\w+(\\s+/\\d+\\s+\\w+)*")) throw new IllegalArgumentException("Wrong input format");
        String[] tokens = query.trim().split("\\s+");
        String[] terms = new String[tokens.length / 2 + 1];
        int[] distance = new int[tokens.length / 2];

        for (int i = 0, j = 0; i < tokens.length; j++, i += 2) {
            terms[j] = IndexServer.normalize(tokens[i]);
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

    private List<int[]> positionalIntersect(String t1, String t2, int dist) {
        ArrayList<int[]> answer = new ArrayList<>();
        int[][] tData_1 = indexService.getTermData(t1);
        int[][] tData_2 = indexService.getTermData(t2);

        int[] docIDs1 = new int[tData_1.length];
        for (int i = 0, nextDoc = 0; i < docIDs1.length; i++)
            docIDs1[i] = nextDoc += tData_1[i][0];

        for (int i = 0; i < tData_1.length; i++) {
            tData_1[i] = Arrays.copyOfRange(tData_1[i], 1, tData_1[i].length);
            decode(tData_1[i]);
        }

        int[] docIDs2 = new int[tData_1.length];
        for (int j = 0, nextDoc = 0; j < tData_2.length; j++)
            docIDs2[j] = nextDoc += tData_2[j][0];

        for (int j = 0; j < tData_2.length; j++) {
            tData_2[j] = Arrays.copyOfRange(tData_2[j], 1, tData_2[j].length);
            decode(tData_2[j]);
        }

        for (int i = 0, j = 0; i < docIDs1.length && j < docIDs2.length; ) {
            if (docIDs1[i] == docIDs2[j]) {
                Queue<Integer> candidates = new ArrayDeque<>();
                int[] coords1 = tData_1[i];
                int[] coords2 = tData_2[j];

                for (Integer pos1 : coords1) {
                    for (Integer pos2 : coords2)
                        if (pos1 - pos2 < dist) candidates.add(pos2);
                        else if (pos2 > pos1) break;

                    while (candidates.size() > 0 && candidates.peek() - pos1 > dist) candidates.remove();
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

    // a*b*c && *abc && abc*
    // NOT *abc* || ab**c
    // TODO: use simple boolean retrieval to facilitate mixed search
    public Collection<String> processJokerQuery(String query) {
        String[] tokens = query.split("\\s+");
        for (String token : tokens)
            if (!token.matches("(\\*\\w+(\\*\\w+)?)|(\\w+\\*\\w*(\\w\\*\\w*)?)"))
                throw new IllegalArgumentException('"' + query + '"' + " is not a valid joker query");

        for (int i = 0; i < tokens.length; i++)
            tokens[i] = tokens[i].toLowerCase(); // stemmer: searchings -> searching; searching -> search

        Set<Integer> validFiles = new HashSet<>();
        for (String term : tokens) {
            Set<Integer> termContribution = new HashSet<>();
            Set<String> matchPref = new TreeSet<>();
            Set<String> matchSuf = new TreeSet<>();

            if (term.charAt(0) != '*') {
                matchPref.addAll(Arrays.asList(indexService.startWith(term.substring(0, term.indexOf("*")))));
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

    public List<String> processClusterQuery(String q) {
        if (!q.matches("[\\w\\s]+")) throw new IllegalArgumentException("wrong query pattern");
        String[] query = Stream.of(q.split("\\s+"))
                .map(IndexServer::normalize)
                .filter(Objects::nonNull)
                .toArray(String[]::new);
        if (query.length == 0) throw new IllegalArgumentException("invalid words");

        DocVector queryVector = indexService.buildQueryVector(query);
        List<Integer> docIDs = indexService.getCluster(queryVector);
        if (docIDs == null) return new ArrayList<>(0);
        List<String> result = new ArrayList<>(docIDs.size());
        for (Integer docID : docIDs)
            result.add(indexService.getDocName(docID));
        return result;
    }

    private List<Integer> intersect(Integer[] basePost, Integer[] post) {
        List<Integer> res = new ArrayList<>();
        int baseLeap = (int) Math.sqrt(basePost.length);
        int newLeap = (int) Math.sqrt(post.length);

        int baseIndex = 0;
        int anotherIndex = 0;
        while (baseIndex < basePost.length && anotherIndex < post.length) {
            if (basePost[baseIndex].equals(post[anotherIndex])) {
                res.add(basePost[baseIndex++]);
                anotherIndex++;
            } else if (basePost[baseIndex].compareTo(post[anotherIndex]) > 0) {
                int skippedIndex = anotherIndex + newLeap;
                if (skippedIndex < post.length && basePost[baseIndex].compareTo(post[skippedIndex]) >= 0)
                    do {
                        anotherIndex = skippedIndex;
                        skippedIndex += newLeap;
                    } while (skippedIndex < post.length && basePost[baseIndex].compareTo(post[skippedIndex]) >= 0);
                else anotherIndex++;
            } else {
                int skippedIndex = baseIndex + baseLeap;
                if (skippedIndex < basePost.length && basePost[skippedIndex].compareTo(post[anotherIndex]) <= 0)
                    do {
                        baseIndex = skippedIndex;
                        skippedIndex += baseLeap;
                    } while (skippedIndex < basePost.length && basePost[skippedIndex].compareTo(post[anotherIndex]) <= 0);
                else anotherIndex++;
            }
        }
        return res;
    }
}