package ukma.ir;

import java.io.*;
import java.nio.file.Path;
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
                            excludeTerms.add(IndexServer.normalize(token));
                        else includeTerms.add(IndexServer.normalize(token));

                    // if there are terms to include and one of these terms is in dictionary (in this case - 0th),
                    // i.e. input is not empty
                    // then set posting for this term as a base for further intersections
                    // else do no intersection as the result is empty if any entry is empty
                    Set<Integer> inDocs = new HashSet<>();
                    if (!includeTerms.isEmpty() && indexService.containsElement(includeTerms.get(0), IndexServer.IndexType.PHRASE)) {
                        inDocs.addAll(indexService.getPostings(includeTerms.get(0), IndexServer.IndexType.PHRASE)); // set base
                        includeTerms.forEach(term -> {
                            ArrayList<Integer> posting = indexService.getPostings(term, IndexServer.IndexType.PHRASE);
                            if (posting != null) inDocs.retainAll(posting); // intersect
                        });
                    }

                    Set<Integer> exDocs = new HashSet<>();
                    for (String term : excludeTerms) {
                        if (term == null) continue; // normalizer might return null
                        ArrayList<Integer> posting = indexService.getPostings(term, IndexServer.IndexType.PHRASE);
                        if (posting != null) exDocs.addAll(posting);
                    }

                    inDocs.removeAll(exDocs);
                    reminders.addAll(inDocs);
                });

        List<String> result = new ArrayList<>(reminders.size());
        reminders.forEach(id -> result.add(indexService.getDocName(id)));
        return result;
    }

    public List<Path> processPhraceQuery(String query) {
        String[] tokens = query.split("\\s+");
        for (int i = 0; i < tokens.length; i++)
            tokens[i] = IndexServer.normalize(tokens[i]);

        List<String> phrases = new ArrayList<>();
        String firstWord = null;
        boolean hasFuncWords = false;

        StringBuilder normilizedQuery = new StringBuilder(query.length());

        for (String term : tokens) {
            normilizedQuery.append(term).append(' ');
            if (firstWord == null && !IndexServer.isFuncWord(term)) firstWord = term;
            else if (IndexServer.isFuncWord(term)) hasFuncWords = true;
            else { // secondWord == null && !funcWords.contains(term)
                if (hasFuncWords) phrases.add("* " + firstWord + " " + term);
                else phrases.add(firstWord + " " + term);
            }
        }

        if (phrases.size() == 0) throw new IllegalArgumentException("no phrases inside the query: \"" + query + "\"");

        List<Integer> commonPostings = indexService.getPostings(phrases.get(0), IndexServer.IndexType.PHRASE);
        for (int i = 1; i < phrases.size(); i++)
            commonPostings = intersect(commonPostings, indexService.getPostings(phrases.get(i), IndexServer.IndexType.PHRASE));

        return filterValidDocs(indexService, commonPostings, normilizedQuery.toString());
    }

    // possible to parallelize
    private List<Path> filterValidDocs(IndexServer is, List<Integer> docIDs, String query) {
        int numReadLines = 20;
        List<Path> validDocs = new ArrayList<>();
        label:
        for (Integer id : docIDs) {
            Path doc = is.getDocPath(id);

            try (BufferedReader br = new BufferedReader(new FileReader(doc.toFile()))) {
                String nextLine = "";
                while (nextLine != null) {
                    StringBuilder textSlice = new StringBuilder();
                    for (int i = 0; i < numReadLines; i++) {
                        nextLine = br.readLine();
                        if (nextLine == null) break;
                        textSlice.append(nextLine);
                    }
                    if (textSlice.toString().contains(query)) {
                        validDocs.add(doc);
                        continue label;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return validDocs;
    }

    private List<Integer> intersect(List<Integer> basePost, List<Integer> post) {
        List<Integer> res = new ArrayList<>();
        int baseLeap = (int) Math.sqrt(basePost.size());
        int newLeap = (int) Math.sqrt(post.size());

        int baseIndex = 0;
        int anotherIndex = 0;
        while (baseIndex < basePost.size() && anotherIndex < post.size()) {
            if (basePost.get(baseIndex).equals(post.get(anotherIndex))) {
                res.add(basePost.get(baseIndex++));
                anotherIndex++;
            }
            if (basePost.get(baseIndex).compareTo(post.get(anotherIndex)) > 0) {
                int skippedIndex = anotherIndex + newLeap;
                if (skippedIndex < post.size() && basePost.get(baseIndex).compareTo(post.get(skippedIndex)) >= 0)
                    do {
                        anotherIndex = skippedIndex;
                        skippedIndex += newLeap;
                    } while (skippedIndex < post.size() && basePost.get(baseIndex).compareTo(post.get(skippedIndex)) >= 0);
                else anotherIndex++;
            } else {
                int skippedIndex = baseIndex + baseLeap;
                if (skippedIndex < basePost.size() && basePost.get(skippedIndex).compareTo(post.get(anotherIndex)) <= 0)
                    do {
                        baseIndex = skippedIndex;
                        skippedIndex += baseLeap;
                    } while (skippedIndex < basePost.size() && basePost.get(skippedIndex).compareTo(post.get(anotherIndex)) <= 0);
                else anotherIndex++;
            }
        }
        return res;
    }

    public List<String> processPositionalQuery(String query) {
        return null;
    }
}
