package ukma.ir.controllers;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.Alert;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.TextField;
import ukma.ir.App;
import ukma.ir.QueryProcessor;
import ukma.ir.index.IndexService;

import java.util.HashMap;
import java.util.Map;

public class MainController {
    private static final Map<IndexService.IndexType, String> PROMPTS = new HashMap<>(IndexService.IndexType.values().length);
    private static final IndexService.IndexType INITIAL_TYPE = IndexService.IndexType.BOOLEAN;
    private App entry;
    private QueryProcessor processor;
    private final ObservableList<String> queryResponse = FXCollections.observableArrayList();
    @FXML
    private TextField inputField;
    @FXML
    private ChoiceBox<IndexService.IndexType> searchMode;

    static {
        PROMPTS.put(IndexService.IndexType.BOOLEAN, "e.g. pen AND pineapple OR apple AND NOT pen");
        PROMPTS.put(IndexService.IndexType.COORDINATE, "e.g. private /3 membership");
        PROMPTS.put(IndexService.IndexType.JOKER, "e.g. pneumono*scopic*coniosis");
    }

    public void initView() {
        searchMode.getItems().addAll(IndexService.IndexType.values());
        searchMode.setValue(INITIAL_TYPE);
        inputField.setPromptText(PROMPTS.get(INITIAL_TYPE));
        searchMode.setOnAction((actionEvent) -> inputField.setPromptText(PROMPTS.
                get(searchMode.getSelectionModel().getSelectedItem())));
    }

    @FXML
    void search() {
        if (processor == null) processor = QueryProcessor.getInstance();
        try {
            String query = inputField.getText();
            if (query == null || query.matches("\\s*")) {
                new Alert(Alert.AlertType.ERROR, "empty query is not allowed").show();
                return;
            }
            query = query.trim();

            switch (searchMode.getSelectionModel().getSelectedItem()) {
                case BOOLEAN:
                    queryResponse.setAll(processor.processBooleanQuery(query));
                    break;
                case COORDINATE:
                    queryResponse.setAll(processor.processPositionalQuery(query));
                    break;
                case JOKER:
                    queryResponse.setAll(processor.processJokerQuery(query));
                    break;
//                case CLUSTER:
//                    queryResponse.setAll(processor.processClusterQuery(query));
            }
            entry.showResult();
        } catch (IllegalArgumentException e) {
            new Alert(Alert.AlertType.ERROR, e.getMessage()).show();

        } catch (Exception e) {
            e.printStackTrace();
            new Alert(Alert.AlertType.ERROR, "Unknown error has occurred!").show();
        }
    }

    public void takeEntry(App app) {
        entry = app;
    }

    public ObservableList<String> getModel() {
        return queryResponse;
    }
}
