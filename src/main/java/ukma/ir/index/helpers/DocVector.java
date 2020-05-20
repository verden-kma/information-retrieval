package ukma.ir.index.helpers;


import javafx.scene.control.Alert;

import java.io.*;
import java.util.Map;

public class DocVector {

    static {
        // no time for AppData path
        String path = "data/doc_vectors";
        File dirs = new File(path);
        dirs.mkdirs();
    }

    private final static String PATH_TEMPLATE = ("data/doc_vectors/vec%d.bin");
    private final int ordinal;
    private final String filePath;
    private int entries;
    private RandomAccessFile bridge;
    private double squareSum;
    private boolean isBuilding = true;

    public DocVector(int docID) {
        ordinal = docID;
        filePath = String.format(PATH_TEMPLATE, docID);
        try {
            bridge = new RandomAccessFile(filePath, "rw");
        } catch (Exception e) {
            new Alert(Alert.AlertType.ERROR, "Debug: DocVector internal error\n" + e.getMessage()).show();
        }
    }

    // TODO: refactor code so that query vectors are not the same objects as doc vectors
    public DocVector(Map<Integer, Double> query) {
        this(-1);
        for (Map.Entry<Integer, Double> termData : query.entrySet())
            addTermScore(termData.getKey(), termData.getValue());
        finishBuild();
    }

    void addTermScore(int termNum, double tfIdf) {
        if (!isBuilding) throw new IllegalStateException("vector is already built");
        try {
            bridge.writeInt(termNum);
            bridge.writeFloat((float) tfIdf);
            squareSum += Math.pow(tfIdf, 2);
            entries++;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void finishBuild() {
        isBuilding = false;
        try {
            bridge.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public double cosineSimilarity(DocVector v) {
        if (isBuilding) throw new IllegalStateException("vector is still being built");
        if (v == null) throw new IllegalArgumentException("parameter is null");
        if (entries == 0 || v.entries == 0) return 0;
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(filePath)));
             DataInputStream vDis = new DataInputStream(new BufferedInputStream(new FileInputStream(v.filePath)))) {
            double score = 0;

            int index = dis.readInt();
            int vIndex = vDis.readInt();

            if (entries == 1 || v.entries == 1)
                return index * vIndex / (Math.sqrt(squareSum) * Math.sqrt(v.squareSum));

            int i = 1, j = 1;
            while (i < entries && j < v.entries) {
                if (index == vIndex) {
                    score += dis.readFloat() * vDis.readFloat();

                    index = dis.readInt();
                    vIndex = vDis.readInt();

                    i++;
                    j++;
                } else if (index > vIndex) {
                    vDis.skipBytes(Float.BYTES);
                    vIndex = vDis.readInt();
                    j++;
                } else { // if (index < vIndex)
                    dis.skipBytes(Float.BYTES);
                    index = dis.readInt();
                    i++;
                }
            }
            return score / (Math.sqrt(squareSum) * Math.sqrt(v.squareSum));
        } catch (IOException e) {
            throw new RuntimeException("internal logic error, IOError", e);
        }
    }

    public int getOrdinal() {
        return ordinal;
    }
}
