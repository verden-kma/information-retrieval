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

public class MainController {

    private App entry;
    private final QueryProcessor qp = new QueryProcessor(IndexService.getInstance());
    private ObservableList<String> queryResponse = FXCollections.observableArrayList();
    @FXML
    private TextField inputField;
    @FXML
    private ChoiceBox<IndexService.IndexType> searchMode;

    public void initVisual() {
        searchMode.getItems().addAll(IndexService.IndexType.values());
        searchMode.setValue(IndexService.IndexType.TERM);
    }

    @FXML
    void search() {
        try {
            String query = inputField.getText();
            if (query == null || query.matches("\\s*")) {
                new Alert(Alert.AlertType.ERROR, "empty query is not allowed").show();
                return;
            }
            query = query.trim();

            switch (searchMode.getSelectionModel().getSelectedItem()) {
                case TERM:
                    queryResponse.setAll(qp.processBooleanQuery(query));
                    break;
                case COORDINATE:
                    queryResponse.setAll(qp.processPositionalQuery(query));
                    break;
                case JOKER:
                    queryResponse.setAll(qp.processJokerQuery(query));
                    break;
                case CLUSTER:
                    queryResponse.setAll(qp.processClusterQuery(query));
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
