package ukma.ir.index.helpers;

import com.google.common.collect.BiMap;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class CacheManager {
    private static final Path DOC_ID_PATH = Paths.get("data/cache/docIdMap.bin");
    private static final Path INDEX_PATH = Paths.get("data/cache/index.bin");
    private static final Path VECTORS_PATH = Paths.get("data/cache/vectors.bin");
    private static final Path CLUSTERS_PATH = Paths.get("data/cache/clusters.bin");

    static {
        // no time for AppData path
        String path = "data/cache";
        File dirs = new File(path);
        if (!dirs.exists())
            dirs.mkdirs();
    }

    public enum Fields {
        PATH_DOC_ID_MAP,
        INDEX,
        DOC_VECTORS,
        CLUSTERS
    }

    public static boolean filesPresent() {
        return DOC_ID_PATH.toFile().exists() && INDEX_PATH.toFile().exists()
                && VECTORS_PATH.toFile().exists() && CLUSTERS_PATH.toFile().exists();
    }

    public static void saveCache(BiMap<String, Integer> docId, IndexBody index, DocVector[] docVectors, Map<Integer, DocVector[]> clusters) {
        try {
            FileOutputStream fos = new FileOutputStream(DOC_ID_PATH.toFile());
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(docId);
            oos.close();

            fos = new FileOutputStream(INDEX_PATH.toFile());
            oos = new ObjectOutputStream(fos);
            oos.writeObject(index);
            oos.close();

            fos = new FileOutputStream(VECTORS_PATH.toFile());
            oos = new ObjectOutputStream(fos);
            oos.writeObject(docVectors);
            oos.close();

            fos = new FileOutputStream(CLUSTERS_PATH.toFile());
            oos = new ObjectOutputStream(fos);
            oos.writeObject(clusters);
            oos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T loadCache(Fields field) throws IOException, ClassNotFoundException {
        FileInputStream fis = null;
        switch (field) {
            case INDEX:
                fis = new FileInputStream(INDEX_PATH.toFile());
                break;
            case PATH_DOC_ID_MAP:
                fis = new FileInputStream(DOC_ID_PATH.toFile());
                break;
            case DOC_VECTORS:
                fis = new FileInputStream(VECTORS_PATH.toFile());
                break;
            case CLUSTERS:
                fis = new FileInputStream(CLUSTERS_PATH.toFile());
                break;
        }
        ObjectInputStream ois = new ObjectInputStream(fis);
        T res = (T) ois.readObject();
        ois.close();
        return res;
    }

    public static void deleteCache() {
        try {
            Files.deleteIfExists(INDEX_PATH);
            Files.deleteIfExists(DOC_ID_PATH);
            Files.deleteIfExists(VECTORS_PATH);
            Files.deleteIfExists(CLUSTERS_PATH);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
