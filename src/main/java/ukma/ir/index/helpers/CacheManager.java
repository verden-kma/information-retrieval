package ukma.ir.index.helpers;

import com.google.common.collect.BiMap;
import ukma.ir.index.IndexService;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CacheManager {
    private static final Path DOC_ID_PATH = Paths.get(IndexService.APP_DATA_PATH, "data/cache/docIdMap.bin");
    private static final Path INDEX_PATH = Paths.get(IndexService.APP_DATA_PATH, "data/cache/index.bin");
    private static final Path VECTORS_PATH = Paths.get(IndexService.APP_DATA_PATH, "data/cache/vectors.bin");

    static {
        Path cachePath = Paths.get(IndexService.APP_DATA_PATH, "data/cache");
        File cacheDir = cachePath.toFile();
        if (!cacheDir.exists())
            cacheDir.mkdirs();
    }

    public enum Fields {
        PATH_DOC_ID_MAP,
        INDEX
    }

    public static boolean filesPresent() {
        return DOC_ID_PATH.toFile().exists() && INDEX_PATH.toFile().exists()
                && VECTORS_PATH.toFile().exists();
    }

    public static void saveCache(BiMap<String, Integer> docId, IndexBody index) {
        try {
            FileOutputStream fos = new FileOutputStream(DOC_ID_PATH.toFile());
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(docId);
            oos.close();

            fos = new FileOutputStream(INDEX_PATH.toFile());
            oos = new ObjectOutputStream(fos);
            oos.writeObject(index);
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
