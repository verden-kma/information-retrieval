package ukma.ir.controllers;


import javafx.fxml.FXML;
import javafx.scene.control.Alert;
import javafx.scene.control.TextField;
import ukma.ir.App;
import ukma.ir.QueryProcessor;
import ukma.ir.index.IndexService;
import ukma.ir.index.helpers.CacheException;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class StarterController {
    private App entry;

    @FXML
    private TextField pathPicker;

    @FXML
    void buildIndex() {
        try {
            String passedText = pathPicker.getText();
            if (passedText == null || passedText.matches("\\s*")) {
                new Alert(Alert.AlertType.ERROR, "Path must be specified.").show();
                return;
            }
            Path libPath = Paths.get(passedText);
            IndexService index = IndexService.buildIndex(libPath);
            QueryProcessor.initQueryProcessor(index);
            entry.showMain();
        }catch (InvalidPathException e) {
            new Alert(Alert.AlertType.ERROR, "Invalid path.").show();
        }
    }

    @FXML
    void loadIndex() {
        try {
            IndexService index = IndexService.loadCache();
            QueryProcessor.initQueryProcessor(index);
        } catch (CacheException e) {
            e.printStackTrace();
            new Alert(Alert.AlertType.ERROR, "Failed to load index from cache.\n" + e.getMessage()).show();
            return;
        }
        entry.showMain();
    }

    public void takeEntry(App app) {
        entry = app;
    }
}

