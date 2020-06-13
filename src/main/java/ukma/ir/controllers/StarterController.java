package ukma.ir.controllers;


import de.jensd.fx.glyphs.GlyphsDude;
import de.jensd.fx.glyphs.fontawesome.FontAwesomeIcon;
import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.TextField;
import javafx.scene.layout.BorderPane;
import javafx.scene.text.Text;
import ukma.ir.App;
import ukma.ir.QueryProcessor;
import ukma.ir.index.IndexService;
import ukma.ir.index.helpers.CacheException;

import java.io.FileNotFoundException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class StarterController {
    private App entry;


    @FXML
    private TextField pathPicker;

    @FXML
    private Button buildBtn;

    @FXML
    private Button loadBtn;

    @FXML
    private BorderPane spinnerPane;

    @FXML
    void buildIndex() {
        final String passedText = pathPicker.getText();
        if (passedText == null || passedText.matches("\\s*")) {
            new Alert(Alert.AlertType.ERROR, "Path must be specified.").show();
            return;
        }

        Task<IndexService> indexTask = new Task<IndexService>() {
            @Override
            protected IndexService call() throws Exception {
                Path libPath = Paths.get(passedText);
                if (!libPath.toFile().exists()) throw new FileNotFoundException();
                return IndexService.buildIndex(libPath);
            }
        };

        indexTask.setOnSucceeded((stateEvent) -> {
            QueryProcessor.initQueryProcessor(indexTask.getValue());
            Platform.runLater(entry::showMain);
        });

        indexTask.setOnFailed((stateEvent) -> {
            Throwable cause = indexTask.getException();
            if (cause instanceof InvalidPathException) {
                new Alert(Alert.AlertType.ERROR, "Invalid path.").show();
            } else if (cause instanceof FileNotFoundException) {
                new Alert(Alert.AlertType.INFORMATION, "Specified folder must exist.").show();
            } else if (cause instanceof OutOfMemoryError) {
                new Alert(Alert.AlertType.INFORMATION, "Collection specified is too big.").show();
            } else {
                new Alert(Alert.AlertType.ERROR, "Unknown error occurred.").show();
            }
            Platform.runLater(this::toggleSpinner);
        });

        toggleSpinner();
        new Thread(indexTask).start();
    }

    @FXML
    void loadIndex() {
        Task<IndexService> indexTask = new Task<IndexService>() {
            @Override
            protected IndexService call() throws Exception {
                return IndexService.loadCache();
            }
        };

        indexTask.setOnSucceeded((stateEvent) -> {
            QueryProcessor.initQueryProcessor(indexTask.getValue());
            Platform.runLater(entry::showMain);
        });

        indexTask.setOnFailed((stateEvent) -> {
            Throwable cause = indexTask.getException();
            if (cause instanceof CacheException) {
                new Alert(Alert.AlertType.ERROR, "Failed to load index from cache.\n"
                        + cause.getMessage()).show();
            } else if (cause instanceof OutOfMemoryError) {
                new Alert(Alert.AlertType.INFORMATION, "Collection specified is too big.").show();
            } else {
                new Alert(Alert.AlertType.ERROR, "Unknown error occurred.").show();
            }
            Platform.runLater(this::toggleSpinner);
        });

        toggleSpinner();
        new Thread(indexTask).start();
    }

    public void takeEntry(App app) {
        entry = app;
        Text testIcon = GlyphsDude.createIcon(FontAwesomeIcon.SPINNER, "300px");
        spinnerPane.setBottom(testIcon);
        spinnerPane.setVisible(false);
    }

    private void toggleSpinner() {
        if (spinnerPane.isDisabled()) {
            spinnerPane.disableProperty().set(false);
            spinnerPane.setVisible(true);
            pathPicker.disableProperty().set(true);
            buildBtn.disableProperty().set(true);
            loadBtn.disableProperty().set(true);
        } else {
            spinnerPane.setVisible(false);
            spinnerPane.disableProperty().set(true);
            pathPicker.disableProperty().set(false);
            buildBtn.disableProperty().set(false);
            loadBtn.disableProperty().set(false);
        }
    }
}

