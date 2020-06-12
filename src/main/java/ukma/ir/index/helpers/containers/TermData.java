package ukma.ir.index.helpers.containers;

public class TermData implements CharSequence {
    private final String term;
    private final int fleckID;
    private final long fleckPos;
    private int docFr;

    public TermData(String term, int fleck, long pos) {
        this.term = term;
        fleckID = fleck;
        fleckPos = pos;
    }

    public int getDocFr() {
        return docFr;
    }

    public void setDocFr(int docFr) {
        this.docFr = docFr;
    }

    public String getTerm() {
        return term;
    }


    public int getFleckID() {
        return fleckID;
    }


    public long getFleckPos() {
        return fleckPos;
    }

    @Override
    public int length() {
        return term.length();
    }

    @Override
    public char charAt(int index) {
        return term.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return term.subSequence(start, end);
    }

    @Override
    public String toString() {
        return term;
    }
}