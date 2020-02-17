package ukma.ir.Controllers;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.Alert;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.TextField;
import ukma.ir.App;
import ukma.ir.IndexServer;
import ukma.ir.QueryProcessor;

public class MainController {

    private App entry;
    private ObservableList<String> queryResponse = FXCollections.observableArrayList();
    @FXML
    private TextField inputField;
    @FXML
    private ChoiceBox<IndexServer.IndexType> searchMode;

    public void initVisual() {
        searchMode.getItems().addAll(IndexServer.IndexType.TERM, IndexServer.IndexType.COORDINATE, IndexServer.IndexType.JOKER);
        searchMode.setValue(IndexServer.IndexType.TERM);
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

            QueryProcessor qp = new QueryProcessor();
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
