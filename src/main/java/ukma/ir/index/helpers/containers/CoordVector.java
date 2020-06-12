package ukma.ir.index.helpers.containers;

import java.util.Objects;

public class CoordVector {
    private final int docID;
    private final int[] coords;
    public final int length;

    public CoordVector(int docID, int[] coords) {
        this.docID = docID;
        this.coords = coords;
        length = coords.length;
    }

    public int getDocID() {
        return docID;
    }

    public int[] getCoords() {
        return coords;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CoordVector that = (CoordVector) o;
        return docID == that.docID;
    }

    @Override
    public int hashCode() {
        return Objects.hash(docID);
    }
}
