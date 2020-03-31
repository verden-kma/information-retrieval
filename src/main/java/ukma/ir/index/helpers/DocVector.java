package ukma.ir.index.helpers;


import javafx.scene.control.Alert;

import java.io.*;

public class DocVector {

    static {
        // no time for AppData path
        String path = "data/doc_vectors";
        File dirs = new File(path);
        dirs.mkdirs();
    }

    private final static String PATH_TEMPLATE = ("data/doc_vectors/vec%d.bin");
    private final String filePath;
    private int entries;
    private RandomAccessFile bridge;
    private double squareSum;
    private boolean isBuilding = true;

    DocVector(int docID) {
        filePath = String.format(PATH_TEMPLATE, docID);
        try {
            bridge = new RandomAccessFile(filePath, "rw");
        } catch (Exception e) {
            new Alert(Alert.AlertType.ERROR, "Debug: DocVector internal error\n" + e.getMessage()).show();
        }
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

    double cosineSimilarity(DocVector v) {
        if (isBuilding) throw new IllegalStateException("vector is still being built");
        if (v == null) throw new IllegalArgumentException("parameter is null");
        if (entries == 0 || v.entries == 0) return 0;
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(filePath)));
             DataInputStream vDis = new DataInputStream(new BufferedInputStream(new FileInputStream(v.filePath)))) {
            double score = 0;

            int index = dis.readInt();
            int vIndex = vDis.readInt();

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

//    /**
//     * java, where is <code>ref</code>?
//     * @param bridge RandomAccessFile with lower file position (inState -> Float -> ... -> Float -> outState
//     * @param fileLength length of file that <code>bridge</code> is associated with
//     * @param loInd index of file for lower processed vector
//     * @param hiInd index of file for higher processed vector
//     * @param leap number of entries to leap over
//     * @return skipData which is 2 integers:<br> 1st - number of leaped entries<br>2nd-advanced <code>loInd</code>
//     * @throws IOException
//     */
//    private long trySkip(RandomAccessFile bridge, long fileLength, int loInd, int hiInd, int leap) throws IOException {
//        long oldPos = bridge.getFilePointer();
//        long tryPos = oldPos + Float.BYTES + leap * (Integer.BYTES + Float.BYTES); // go to int position and jump
//        int leaped = 0;
//
//        if (tryPos < fileLength) {
//            bridge.seek(tryPos);
//            int tryIndex = bridge.readInt();
//            // at this stage only int index is read, file pointer is before float tfIdf
//            if (tryIndex <= hiInd) { // if possible to skip then skip while possible
//                do {
//                    loInd = tryIndex;
//                    oldPos = bridge.getFilePointer(); // = tryPos
//                    leaped += leap;
//                    tryPos = oldPos + leap * (Integer.BYTES + Float.BYTES);
//                    if (tryPos >= fileLength) break;
//                    bridge.seek(tryPos);
//                    bridge.skipBytes(Float.BYTES);
//                    tryIndex = bridge.readInt();
//                } while (hiInd >= tryIndex);
//
//            }
//            // last skip caused filePointer to be 1 skip ahead from last correct
//            bridge.seek(oldPos); // reset to last correct filePointer
//        }
//        long res = leaped;
//        res <<= 32;
//        res |= loInd;
//        return res;
//    }
}
