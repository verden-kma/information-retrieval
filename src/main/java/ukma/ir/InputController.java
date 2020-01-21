package ukma.ir;

import javafx.fxml.FXML;
import javafx.scene.control.TextField;

public class InputController {

    @FXML
    private TextField inputField;

    @FXML
    private TextField outputField;

    @FXML
    String getInputPath() {
        return inputField.getText();
    }

    @FXML
    String getOutputPath() {
        return outputField.getText();
    }

    @FXML
    void buildDictionary() {
        long startTime = System.nanoTime();
        Librarian.build(getInputPath());
//        Librarian.write(getOutputPath());
//        Librarian.build("C:\\Users\\Andrew\\Desktop\\files");
//        Librarian.write("C:\\Users\\Andrew\\Desktop");
//        System.out.println("collectionOnStorageBytes: " + Librarian.collectionOnStorageBytes());
//        System.out.println("totalWords: " + Librarian.totalWords());
//        System.out.println("SizeTerms: " + Librarian.getSizeTerms());

        long endTime = System.nanoTime();
        System.out.println("single time: " + (endTime - startTime) / 1e9);

        ProLibrarian librarian = ProLibrarian.getInstance("C:\\Users\\Andrew\\Desktop\\OurData", "C:\\Users\\Andrew\\Desktop");
        librarian.makeDictionary();

        System.out.println(librarian.getDictionary().size());
    }
}
