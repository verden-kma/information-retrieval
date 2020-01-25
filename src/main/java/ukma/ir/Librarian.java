package ukma.ir;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Stream;

public class Librarian {

    public static ArrayList<ArrayList<String>> getDictionary() {
        return dictionary;
    }

    private static ArrayList<ArrayList<String>> dictionary;
    private static int words; // num of words with duplicates
    private static long collectionSize; // on disk

    public static void build(String inPath) {
        dictionary = new ArrayList<>();
        words = 0;
        collectionSize = 0;
        try (Stream<Path> paths = Files.walk(Paths.get(inPath))) {
            paths.filter(Files::isRegularFile)
                    .forEach(Librarian::addToDictionary);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Incorrect path.");
        }
    }

    private static void addToDictionary(final Path path) {
        try (BufferedReader br = new BufferedReader(new FileReader(path.toFile()))) {
            br.lines()
                    .map(line -> line.split("\\s"))
                    .flatMap(Arrays::stream)
                    .forEach(token -> {
                        boolean found = false;
                        for (ArrayList<String> list : dictionary)
                            if (list.get(0).equals(token)) {
                                found = true;
                                if (!list.get(list.size() - 1).equals(path.getFileName().toString()))
                                    list.add(path.getFileName().toString());
                            }
                        if (!found) {
                            ArrayList<String> newPosting = new ArrayList<>();
                            newPosting.add(token);
                            newPosting.add(path.getFileName().toString());
                            dictionary.add(newPosting);
                        }
                        words++;
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void write(String outputPath) {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(outputPath + "/storage.bin"))) {
            oos.writeObject(dictionary);
        } catch (IOException e) {
            e.printStackTrace();
        }

        collectionSize = new File(outputPath + "/storage.bin").length();

        //read object
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(outputPath + "/storage.bin"))) {
            ArrayList<ArrayList<String>> check = (ArrayList<ArrayList<String>>) ois.readObject();
            assert (dictionary.equals(check));
            System.out.println(check);
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }
    }

    public static int getSizeTerms() {
        if (dictionary == null) return 0;
        return dictionary.size();
    }

    public static int totalWords() {
        return words;
    }

    public static long collectionOnStorageBytes() {
        return collectionSize;
    }
}
