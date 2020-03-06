package ukma.ir;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class QueryProcessor {

    private final IndexServer indexService = IndexServer.getInstance();

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
                    if (!includeTerms.isEmpty() && indexService.containsElement(includeTerms.get(0), IndexServer.IndexType.TERM)) {
                        inDocs = new HashSet<>(indexService.getPostings(includeTerms.get(0), IndexServer.IndexType.TERM));
                        includeTerms.forEach(term -> {
                            ArrayList<Integer> posting = indexService.getPostings(term, IndexServer.IndexType.TERM);
                            if (posting != null) inDocs.retainAll(posting); // intersect
                        });
                    }
                    else return;
                    Set<Integer> exDocs = new HashSet<>();
                    for (String term : excludeTerms) {
                        if (term == null) continue; // normalizer might return null
                        ArrayList<Integer> posting = indexService.getPostings(term, IndexServer.IndexType.TERM);
                        if (posting != null) exDocs.addAll(posting);
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
            if (!indexService.containsElement(terms[j], IndexServer.IndexType.TERM)) return new ArrayList<>(0);
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

    private List<int[]> positionalIntersect(String t1, String t2, int dist) throws IOException {
        ArrayList<int[]> answer = new ArrayList<>();
        Map<Integer, ArrayList<Integer>> docCoordMap1 = indexService.getTermDocCoord(t1);
        Map<Integer, ArrayList<Integer>> docCoordMap2 = indexService.getTermDocCoord(t2);
        Integer[] docs1 = docCoordMap1.keySet().toArray(new Integer[0]);
        Integer[] docs2 = docCoordMap2.keySet().toArray(new Integer[0]);
        for (int i = 0, j = 0; i < docs1.length && j < docs2.length; ) {
            if (docs1[i].equals(docs2[j])) {
                Queue<Integer> candidates = new ArrayDeque<>();
                ArrayList<Integer> coords1 = docCoordMap1.get(docs1[i]);
                ArrayList<Integer> coords2 = docCoordMap2.get(docs2[j]);
                for (Integer pos1 : coords1) {
                    for (Integer pos2 : coords2)
                        if (pos1 - pos2 < dist) candidates.add(pos2);
                        else if (pos2 > pos1) break;

                    while (candidates.size() > 0 && candidates.peek() - pos1 > dist) candidates.remove();
                    for (int k = 0; k < candidates.size()/* && k < 10*/; k++)
                        answer.add(new int[]{docs1[i], pos1, candidates.remove()});
                }
                i++;
                j++;
            } else if (docs1[i].compareTo(docs2[j]) < 0) i++;
            else j++;
        }
        return answer;
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
                for (String pref : indexService.startWith(term.substring(0, term.indexOf("*"))))
                    matchPref.add(pref);
                // no terms match given non-empty prefix
                if (matchPref.isEmpty()) return new ArrayList<>(0);
            }

            if (term.charAt(term.length() - 1) != '*') {
                for (String suf : indexService.endWith(term.substring(term.lastIndexOf('*') + 1)))
                    matchSuf.add(suf);
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

            for (String v : valid)
                termContribution.addAll(indexService.getPostings(v, IndexServer.IndexType.TERM));

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