package ukma.ir.index.helpers;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.NoSuchElementException;

public class IndexBody {
    private int[][] dict; // 0th - start index of term, 1st - fleckID, 2nd - in-fleck start pos (1/2), 3rd - (2/2) 4rd - docFr
    private int[][] revDict;
    private String vocabStr;
    private String reVocabStr;
    private final String FLECK_PATH;

    public IndexBody(TermData[] sortedTermData, String fleckPath) {
        FLECK_PATH = fleckPath;
        Quick3string.sort(sortedTermData);
        buildDictionary(sortedTermData);
    }

    public boolean containsElement(String term) {
        return findTerm(term, dict, vocabStr, false) != -1;
    }

    public String[] startWith(String prefix) {
        long sample = findTerm(prefix, dict, vocabStr, true);
        if (sample == -1) throw new NoSuchElementException("No term with prefix \"" + prefix + "\" found.");

        return edgeWith(prefix, sample, dict, vocabStr);
    }

    public String[] endWith(String suffix) {
        long sample = findTerm(suffix, revDict, reVocabStr, true);
        if (sample == -1) throw new NoSuchElementException("No term with suffix \"" + suffix + "\" found.");

        String[] reversed = edgeWith(suffix, sample, revDict, reVocabStr);
        // possible optimization: avoid creation of 1 String -> reverse using char[] while populating "matched[i]"
        StringBuilder directer = new StringBuilder(12);
        for (int i = 0; i < reversed.length; i++) {
            reversed[i] = directer.append(reversed[i]).reverse().toString();
            directer.setLength(0);
        }
        return reversed;
    }

    private String[] edgeWith(String suffix, long sample, int[][] revDict, String reVocabStr) {
        int topBound = findBound(suffix, sample, revDict, reVocabStr, true);
        int bottomBound = findBound(suffix, sample, revDict, reVocabStr, false);
        assert (topBound < bottomBound);
        String[] matched = new String[bottomBound - topBound];
        for (int i = 0; i < matched.length; i++) {
            int begin = revDict[i][0];
            int end = i + 1 < revDict.length ? revDict[i + 1][0] : reVocabStr.length();
            matched[i] = reVocabStr.substring(begin, end);
        }
        return matched;
    }

    /**
     * look through the index to find term's docIDs
     *
     * @param term in index to search
     * @return array of docIDs which contain term
     * @throws IOException if index data has been corrupted
     */
    public int[] getPostings(String term) throws IOException {
        int[][] fullInfo = getTermDocCoords(term);
        int[] res = new int[fullInfo.length];
        for (int i = 0; i < res.length; i++) {
            res[i] = fullInfo[i][0];
        }
        return res;
    }

    //TODO: 1) do not store docFr in memory
    // 2) use buffer https://stackoverflow.com/questions/5614206/buffered-randomaccessfile-java
    /**
     * find documents and positions at which term resides
     *
     * @param term in index to search
     * @return 2-dim array, 1st dim - number of a docID, 2nd dim - docId, term frequency, coords
     * @throws IOException if index data has been corrupted
     */
    public int[][] getTermDocCoords(String term) throws IOException{
        long termData = findTerm(term, dict, vocabStr, false);
        if (termData == -1) throw new NoSuchElementException("Term \"" + term + "\" is absent.");
        int[] infoRow = dict[(int) (termData >> 32)];
        String path = String.format(FLECK_PATH, infoRow[1]);
        long inFleckPos = infoRow[2];
        inFleckPos <<= 32;
        inFleckPos |= infoRow[3];
        try (RandomAccessFile raf = new RandomAccessFile(path, "r")) {
            raf.seek(inFleckPos);
            int docFr = raf.readInt();
            int[][] termStat = new int[docFr][];
            assert (docFr == infoRow[4]);
            for (int i = 0; i < docFr; i++) {
                int docID = readVLC(raf);
                int termFr = readVLC(raf);
                termStat[i] = new int[termFr+2];
                termStat[i][0] = docID;
                termStat[i][1] = termFr;
                for (int j = 2; j < termFr + 2; j++) {
                    termStat[i][j] = readVLC(raf);
                }
            }
            return termStat;
        }
    }

    private int readVLC(RandomAccessFile raf) throws IOException {
        int res = 0;
        while (true) {
            byte next = raf.readByte();
            if (next >= 0) res = res * 128 + next;
            else return res * 128 + (next & 0b01111111);
        }
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
            int end = mid + 1 < dict.length ? dict[mid + 1][0] : vocabStr.length();
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
            int end = mid + 1 < dict.length ? dict[mid + 1][0] : vocabStr.length();

            boolean inRange = cmpStrChars(term, vocabStr, start, end, true) == 0;
            if (inRange && findTop || !inRange && !findTop) hi = mid - 1;
            else lo = mid + 1;
        } while (lo <= hi);
        int start = dict[mid][0];
        int end = mid + 1 < dict.length ? dict[mid + 1][0] : vocabStr.length();
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

    /**
     * compare passed term and a segment of sequence in range [start;end)
     *
     * @param term     to compare against a segment
     * @param sequence to take segment
     * @param start    position of the first char of segment in the sequence
     * @param end      position of the last + 1 char
     * @param prefix   if true then look for a match of the first term.length() chars else - full match
     * @return 0 if term equals sequence, int > 0 if term > sequence, int < 0 otherwise
     */
    private int cmpStrChars(final CharSequence term, final CharSequence sequence, final int start, final int end, final boolean prefix) {
        final int termLen = term.length();
        final int segmentLen = end - start;
        for (int i = 0; i < Math.min(termLen, segmentLen); i++) { // final - prompt for compiler to optimize
            int diff = term.charAt(i) - sequence.charAt(start + i);
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
