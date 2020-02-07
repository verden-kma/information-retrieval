package ukma.ir;

import java.util.*;

public class QueryProcessor {

    private final IndexServer indexService = IndexServer.getInstance();

    /**
     * @param q - query to be processed
     * @return list of documents that match a given query
     */
    public List<String> processBooleanQuery(String q) {
        Set<Integer> reminders = new HashSet<>();
        Arrays.stream(q.split("OR"))
                .map(union -> union.split("AND"))
                .forEach(unionParticle -> {
                    List<String> includeTerms = new ArrayList<>();
                    List<String> excludeTerms = new ArrayList<>();
                    for (String token : unionParticle)
                        if (token.startsWith("NOT"))
                            excludeTerms.add(indexService.normalize(token));
                        else includeTerms.add(indexService.normalize(token));

                    // if there are terms to include and one of these terms is in dictionary (in this case - 0th),
                    // i.e. input is not empty
                    // then set posting for this term as a base for further intersections
                    // else do no intersection as the result is empty if any entry is empty
                    Set<Integer> inDocs = new HashSet<>();
                    if (!includeTerms.isEmpty() && indexService.containsTerm(includeTerms.get(0))) {
                        inDocs.addAll(indexService.getTermPostings(includeTerms.get(0))); // set base
                        includeTerms.forEach(term -> {
                            ArrayList<Integer> posting = indexService.getTermPostings(term);
                            if (posting != null) inDocs.retainAll(posting); // intersect
                        });
                    }

                    Set<Integer> exDocs = new HashSet<>();
                    for (String term : excludeTerms) {
                        if (term == null) continue; // normalizer might return null
                        ArrayList<Integer> posting = indexService.getTermPostings(term);
                        if (posting != null) exDocs.addAll(posting);
                    }

                    inDocs.removeAll(exDocs);
                    reminders.addAll(inDocs);
                });

        List<String> result = new ArrayList<>(reminders.size());
        reminders.forEach(id -> result.add(indexService.getDocName(id)));
        return result;
    }

    public List<String> processPhraceQuery(String query) {
        return null;
    }

    public List<String> processPositionalQuery(String query) {
        return null;
    }
}
