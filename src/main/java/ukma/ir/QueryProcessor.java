package ukma.ir;

public class QueryProcessor {

    //    /**
//     * NEEDS EDITING!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
//     *
//     * @param q - query to be processed
//     * @return list of documents that match a given query
//     */
//    public List<String> processQuery(String q) {
//        //TODO: add overlaps every sqrt(n)
//        Set<Integer> reminders = new TreeSet<>();
//        Arrays.stream(q.split("OR"))
//                .map(union -> union.split("AND"))
//                .forEach(unionParticle -> {
//                    List<String> includeTerms = new ArrayList<>();
//                    List<String> excludeTerms = new ArrayList<>();
//                    for (String token : unionParticle)
//                        if (token.startsWith("NOT"))
//                            excludeTerms.add(new Sentence(token.substring(3)).lemma(0));
//                        else includeTerms.add(new Sentence(token).lemma(0));
//
//                    // if there are terms to include and one of these terms is in dictionary (in this case - 0th),
//                    // i.e. input is not empty
//                    // then set posting for this term as a base for further intersections
//                    // else do no intersection as the result is empty if any entry is empty
//                    Set<Integer> inTerms = new TreeSet<>();
//                    if (!includeTerms.isEmpty() && dictionary.containsKey(includeTerms.get(0))) {
//                        inTerms.addAll(dictionary.get(includeTerms.get(0))); // set base
//                        includeTerms.forEach(term -> {
//                            ArrayList<Integer> posting = dictionary.get(term);
//                            if (posting != null) inTerms.retainAll(posting); // intersect
//                        });
//                    }
//
//                    Set<Integer> exTerms = new TreeSet<>();
//                    for (String term : excludeTerms) {
//                        ArrayList<Integer> posting = dictionary.get(term);
//                        if (posting != null) exTerms.addAll(posting);
//                    }
//
//                    inTerms.removeAll(exTerms);
//                    reminders.addAll(inTerms);
//                });
//
//        List<String> result = new ArrayList<>(reminders.size());
//        reminders.forEach(id -> result.add(docId.inverse().get(id)));
//        return result;
//    }
}
