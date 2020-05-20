package ukma.ir.index.helpers;

import java.util.*;

public class Clusterizer {
    public static Map<Integer, DocVector[]> buildClusters(DocVector[] docVectors) {
        int step = (int) Math.sqrt(docVectors.length);
        HashMap<Integer, Integer> leaderFollowers = chooseLeaders(docVectors, step);
        Follower[] followers = new Follower[docVectors.length - leaderFollowers.size()];
        for (int i = 0; i < followers.length; i++) followers[i] = new Follower();

        //for each leader walk through all docVectors and associate with each leader the followers that are most similar to it
        for (int currLead : leaderFollowers.keySet()) {
            for (int flr = 0; flr < docVectors.length; flr++) {
                if (leaderFollowers.containsKey(flr)) continue;

                double sim = docVectors[currLead].cosineSimilarity(docVectors[flr]);
                int followerInd = flr - flr / step - 1; // 0th doc is decided to be leader, therefore -1

                if (followers[followerInd].sim < sim) {
                    if (followers[followerInd].leader != -1) {
                        Integer oldLeader = followers[followerInd].leader;
                        Integer reducedFollowers = leaderFollowers.get(followers[followerInd].leader) - 1;
                        leaderFollowers.put(oldLeader, reducedFollowers);
                    }
                    followers[followerInd].leader = currLead;
                    followers[followerInd].sim = (float) sim;
                    leaderFollowers.put(currLead, leaderFollowers.get(currLead) + 1); // add follower to a leader's count
                }
            }
        }

        Map<Integer, DocVector[]> clusters = new HashMap<>(leaderFollowers.size());
        for (Map.Entry<Integer, Integer> leaderStat : leaderFollowers.entrySet()) {
            clusters.put(leaderStat.getKey(), new DocVector[leaderStat.getValue()]);
        }

        List<Integer> individuals = new LinkedList<>();
        Set<Integer> leaders = leaderFollowers.keySet();
        HashMap<Integer, Integer> leaderIndices = leaderFollowers; // Leader - Index; different name - different perception(aim)
        for (Integer leader : leaders) {
            leaderIndices.put(leader, 0);
        }
        for (int vec = 0; vec < docVectors.length; vec++) {
            if (leaders.contains(vec)) continue;
            Follower flr = followers[vec - vec / step - 1];
            if (flr.leader == -1) {
                individuals.add(vec);
                continue;
            }
            int index = leaderIndices.get(flr.leader);
            leaderIndices.put(flr.leader, index + 1);
            clusters.get(flr.leader)[index] = docVectors[vec];
        }
        for (Integer individual : individuals)
            clusters.put(individual, null);
        return clusters;
    }

    private static HashMap<Integer, Integer> chooseLeaders(DocVector[] docVectors, int step) {
        HashMap<Integer, Integer> leaders = new HashMap<>();
        for (int nextLeader = 0; nextLeader < docVectors.length; nextLeader += step)
            leaders.put(nextLeader, 0);
        return leaders;
    }

    //fixme: suspect wrong tf-idf implementation
    public static DocVector[] buildDocVectors(int documents, IndexBody index) {
        DocVector[] docVectors = new DocVector[documents];
        for (int i = 0; i < docVectors.length; i++)
            docVectors[i] = new DocVector(i);

        Iterator<String> vocabulary = index.iterator();
        for (int i = 0; vocabulary.hasNext(); i++) {

            CoordVector[] termData = index.getTermData(vocabulary.next());
            int termFr = termData.length;
            for (CoordVector termDatum : termData) {
                docVectors[termDatum.getDocID()].addTermScore(i,
                        termFr * Math.log((double) documents / termData[0].length));
            }
        }
        for (DocVector v : docVectors)
            v.finishBuild();

        return docVectors;
    }
}

class Follower {
    int leader = -1;
    float sim = 0;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Follower follower = (Follower) o;
        return leader == follower.leader;
    }

    @Override
    public int hashCode() {
        return Objects.hash(leader);
    }
}
