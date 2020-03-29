package ukma.ir.index.helpers;


import javafx.scene.control.Alert;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class DocVector {

    static {
        // no time for AppData path
        String path = "data/doc_vectors";
        File dirs = new File(path);
        dirs.mkdirs();
    }

    private final static String PATH_TEMPLATE = ("data/doc_vectors/vec%d.bin");
    private final String filePath;
    private int size;
    private RandomAccessFile bridge;

    public DocVector(int docID) {
        filePath = String.format(PATH_TEMPLATE, docID);
        try {
            bridge = new RandomAccessFile(filePath, "rw");
        } catch (Exception e) {
            new Alert(Alert.AlertType.ERROR, "Debug: DocVector internal error\n" + e.getMessage()).show();
        }
    }

    public void addTermScore(int termNum, double tfIdf) {
        try {
            bridge.writeInt(termNum);
            bridge.writeFloat((float) tfIdf);
            size++;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public double cosineSimilarity(DocVector v) {
        if (v == null) throw new IllegalArgumentException("parameter is null");
        if (size == 0 || v.size == 0) return 0;
        try {


            final long currFilePos = bridge.getFilePointer();
            final long vCurrFilePos = v.bridge.getFilePointer();

            long fileLength = new File(filePath).length();
            long vFileLength = new File(v.filePath).length();
            double score = 0;
            int leap = (int) Math.sqrt(size);
            int vLeap = (int) Math.sqrt(v.size);
            bridge.seek(0);
            v.bridge.seek(0);

            int index = bridge.readInt();
            float tfIdf = bridge.readFloat();
            double denum = 0;
            int vIndex = v.bridge.readInt();
            float vTfIdf = v.bridge.readFloat();
            double vDenum = 0;

            int i = 0, j = 0;
            while (i < size && j < v.size) {
                if (index == vIndex) {
                    score += tfIdf * vTfIdf;
                    denum += Math.pow(tfIdf, 2);
                    vDenum += Math.pow(vTfIdf, 2);

                    index = bridge.readInt();
                    vIndex = v.bridge.readInt();
                    tfIdf = bridge.readFloat();
                    vTfIdf = bridge.readFloat();
                    i++;
                    j++;
                } else if (index > vIndex) {
                    long oldPos = v.bridge.getFilePointer();
                    long tryPos = oldPos + vLeap * (Integer.BYTES + Float.BYTES);
                    v.bridge.seek(tryPos);
                    int vTryIndex = v.bridge.readInt();
                    if (tryPos < vFileLength && index >= vTryIndex) {// if possible to skip then skip while possible
                        do {
                            vIndex = vTryIndex;
                            oldPos = v.bridge.getFilePointer();
                            assert (oldPos == tryPos);
                            tryPos = oldPos + vLeap * (Integer.BYTES + Float.BYTES);
                        } while (tryPos < vFileLength && index >= (vTryIndex = v.bridge.readInt()));

                        // last skip cased filePointer to be 1 skip ahead from last correct
                        v.bridge.seek(oldPos); // reset to last correct filePointer
                    } else {
                        v.bridge.seek(oldPos + Float.BYTES); // skip to next term
                    }
                } else { // if (index < vIndex)
                    long oldPos = bridge.getFilePointer();
                    long tryPos = oldPos + leap * (Integer.BYTES + Float.BYTES);
                    bridge.seek(tryPos);
                    int tryIndex = bridge.readInt();
                    if (tryPos < fileLength && tryIndex <= vIndex) {
                        // if possible to skip then skip while possible
                        do {
                            index = tryIndex;
                            oldPos = bridge.getFilePointer();
                            assert (oldPos == tryPos);
                            tryPos = oldPos + leap * (Integer.BYTES + Float.BYTES);
                        } while (tryPos < fileLength && (tryIndex = bridge.readInt()) <= vIndex);

                        // last skip cased filePointer to be 1 skip ahead from last correct
                        bridge.seek(oldPos); // reset to last correct filePointer
                    } else {
                        bridge.seek(oldPos + Float.BYTES); // skip to next term
                    }
                }
            }

            bridge.seek(currFilePos);
            v.bridge.seek(vCurrFilePos);
            return score / (Math.sqrt(denum) * Math.sqrt(vDenum));
        } catch (IOException e) {
            throw new RuntimeException("internal logic error, IOError", e);
        }
    }

    @Override
    public void finalize() {
        try {
            bridge.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
