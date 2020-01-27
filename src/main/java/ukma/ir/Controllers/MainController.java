package ukma.ir.Controllers;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.TextField;
import ukma.ir.App;
import ukma.ir.ProLibrarian;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class MainController {

    private App entry;
    private ObservableList<String> queryResponse = FXCollections.observableArrayList();
    @FXML
    private TextField inputField;

    @FXML
    void search() {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter("output.txt"))) {
            ProLibrarian librarian = ProLibrarian.getInstance();
            queryResponse.clear();
            queryResponse.addAll(librarian.processQuery(inputField.getText()));
            entry.showResult();
//            for (Map.Entry entry : librarian.getIndex().entrySet()) {
//                bw.write(valueOf(entry));
//                bw.newLine();
//            }
//            System.out.println(librarian.getIndex().size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void takeEntry(App app) {
        entry = app;
    }

    public ObservableList<String> getModel() {
        return queryResponse;
    }
}
