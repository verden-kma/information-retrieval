package ukma.ir.index.helpers;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class IndexBody {
    private int[][] dict; // 0th - start index of term, 1st - fleckID, 2nd - in-fleck of pos (1/2), 3rd - (2/2) 4rd - docFr
    private int[][] revDict;
    private String vocabStr;
    private String reVocabStr;

    public IndexBody(TermData[] sortedTermData) {
        Quick3string.sort(sortedTermData);
        buildDictionary(sortedTermData);
    }

    public boolean containsElement(String term) {
        throw new NotImplementedException();
    }

    public String[] startsWith(String prefix) {
        throw new NotImplementedException();
    }

    public String[] endsWith(String suffix) {
        throw new NotImplementedException();
    }

    /**
     * look through the index to find term's docIDs
     *
     * @param term in index to search
     * @return array of docIDs which contain term
     */
    public int[] getPostings(String term) {
        throw new NotImplementedException();
    }

    /**
     * find documents and positions at which term resides
     *
     * @param term in index to search
     * @return 2-dim array, 1st dim - docIDs, 2nd dim - coords
     */
    public int[][] getTermDocCoords(String term) {
        throw new NotImplementedException();
    }

    private void buildDictionary(TermData[] sortedTermData) {
        StringBuilder vocabStr = new StringBuilder();
        dict = new int[sortedTermData.length][5];
        for (int i = 0; i < sortedTermData.length; i++) {
            int[] termData = dict[i];
            TermData currTermData = sortedTermData[i];
            termData[0] = vocabStr.length();
            termData[1] = currTermData.getFleckID();
            termData[2] = (int) (currTermData.getFleckPos() >> 32); //first 4 bytes
            termData[3] = (int) currTermData.getFleckPos(); // last 4 bytes
            termData[4] = currTermData.getDocFr();
            vocabStr.append(currTermData.getTerm());
        }
        this.vocabStr = vocabStr.toString();

        vocabStr.setLength(0);
        CharSequence[] revTerms = new CharSequence[sortedTermData.length];
        char oldIndexSep = '%';
        for (int i = 0; i < sortedTermData.length; i++) {
            StringBuilder reverser = new StringBuilder(sortedTermData[i].getTerm().length() + 1 + intDigits(i));
            reverser.append(sortedTermData[i].getTerm()).reverse().append(oldIndexSep).append(i);
            sortedTermData[i] = null;
            revTerms[i] = reverser;
        }

        Quick3string.sort(revTerms);
        StringBuilder reVocabStr = new StringBuilder();
        revDict = new int[revTerms.length][2];
        for (int i = 0; i < revTerms.length; i++) {
            revDict[i][0] = reVocabStr.length();
            int sepIndex = -1;
            while (revTerms[i].charAt(++sepIndex) != oldIndexSep) ;
            revDict[i][1] = Integer.parseInt(revTerms[i].subSequence(sepIndex++, revTerms[i].length()).toString()); // java 9 ???
            reVocabStr.append(revTerms[i].subSequence(0, sepIndex));
        }
        this.reVocabStr = reVocabStr.toString();
    }

    /**
     * @param term     to look for
     * @param dict     - positions of terms beginnings
     * @param vocabStr - vocabulary corresponding with "dict"
     * @param prefix   if true then search for prefix match
     *                 if false then search for complete match
     * @return long which is 2 ints: fist 4 bytes = position of the term in "dict",
     * last 4 bytes = previous cut of log search
     */
    private long findTerm(String term, int[][] dict, String vocabStr, final boolean prefix) {
        int lo = 0, hi = dict.length;
        int lastCut = dict.length;
        while (lo <= hi) {
            int mid = lo + (hi - lo) >> 1;
            lastCut = (lastCut - mid) & 0x7fffffff; // better than "return (a < 0) ? -a : a"
            int start = dict[mid][0];
            int end = mid + 1 < dict.length ? dict[mid + 1][0] : vocabStr.length() - 1;
            int cmp = cmpStrChars(term, vocabStr, start, end, prefix);
            if (cmp < 0) hi = mid - 1;
            else if (cmp > 0) lo = mid + 1;
            else {
                long res = mid;
                res <<= 32;
                res |= lastCut;
                return res;
            }
        }
        return -1;
    }

    private int findBound(String term, long termData, final int[][] dict, String vocabStr, boolean findTop) {
        int base = (int) (termData >> 32);
        int lo = findTop ? (int) (base - (termData & 0x00000000ffffffffL)) : base;
        int hi = findTop ? base : (int) (base + (termData & 0x00000000ffffffffL));
        int mid;
        do {
            mid = lo + (hi - lo) >> 1;
            int start = dict[mid][0];
            int end = mid + 1 < dict.length ? dict[mid + 1][0] : vocabStr.length() - 1;

            boolean inRange = cmpStrChars(term, vocabStr, start, end, true) == 0;
            if (inRange && findTop || !inRange && !findTop) hi = mid - 1;
            else lo = mid + 1;
        } while (lo <= hi);
        int start = dict[mid][0];
        int end = mid + 1 < dict.length ? dict[mid + 1][0] : vocabStr.length() - 1;
        if (cmpStrChars(term, vocabStr, start, end, true) != 0) return findTop ? mid + 1 : mid - 1;
        return mid;
    }

    // top in sorted order
//    private int findTopBound(String term, long termData, final int[][] dict, String vocabStr) {
//        // get index of term and look a step higher
//        int hi = (int) (termData >> 32);
//        int lo = (int) (hi - (termData & 0x00000000ffffffffL));
//
//        assert hi != -1;
//        int mid;
//        do {
//            mid = lo + (hi - lo) >> 1;
//            int start = dict[mid][0];
//            int end = dict[mid + 1][0];
//            if (cmpStrChars(term, vocabStr, start, end, true) == 0) hi = mid - 1;
//            else lo = mid + 1;
//        } while (lo <= hi);
//        if (cmpStrChars(term, vocabStr, dict[mid][0], dict[mid + 1][0], true) != 0) return mid + 1;
//        return mid;
//    }
//
//    private int findBottomBound(String term, long termData, final int[][] dict, String vocabStr) {
//        int lo = (int) (termData >> 32);
//        int hi = (int) (lo + (termData & 0x00000000ffffffffL));
//        assert lo != -1;
//        int mid;
//        do {
//            mid = lo + (hi - lo) >> 1;
//            int start = dict[mid][0];
//            int end = mid + 1 < dict.length ? dict[mid + 1][0] : vocabStr.length() - 1;
//            if (cmpStrChars(term, vocabStr, start, end, true) == 0) lo = mid + 1;
//            else hi = mid - 1;
//        } while (lo <= hi);
//        int start = dict[mid][0];
//        int end = mid + 1 < dict.length ? dict[mid + 1][0] : vocabStr.length() - 1;
//        if (cmpStrChars(term, vocabStr, start, end, true) != 0) return mid - 1;
//        return mid;
//    }

//    private long startWith(String prefix, int[][] dict, String vocabStr) {
//
//    }

    private int cmpStrChars(final String term, final String vocabStr, final int start, final int end, final boolean prefix) {
        final int termLen = term.length();
        final int segmentLen = end - start + 1;
        for (int i = 0; i < Math.min(termLen, segmentLen); i++) { // final - prompt for compiler to optimize
            int diff = term.charAt(i) - vocabStr.charAt(start + i);
            if (diff != 0) return diff;
        }
        if (prefix || termLen == segmentLen) return 0;
        return termLen - segmentLen;
    }

    private int intDigits(int number) {
        if (number < 100000) {
            if (number < 100) {
                if (number < 10) {
                    return 1;
                } else {
                    return 2;
                }
            } else {
                if (number < 1000) {
                    return 3;
                } else {
                    if (number < 10000) {
                        return 4;
                    } else {
                        return 5;
                    }
                }
            }
        } else {
            if (number < 10000000) {
                if (number < 1000000) {
                    return 6;
                } else {
                    return 7;
                }
            } else {
                if (number < 100000000) {
                    return 8;
                } else {
                    if (number < 1000000000) {
                        return 9;
                    } else {
                        return 10;
                    }
                }
            }
        }
    }


}
