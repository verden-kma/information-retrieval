package ukma.ir.Controllers;

import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.ListView;
import ukma.ir.App;


public class ResultController {

    private App entry;
    @FXML
    private ListView<String> matchList;

    @FXML
    void setMainScene() {
        entry.showMain();
    }

    public void bindList(ObservableList<String> matched) {
        matchList.setItems(matched);
    }

    public void takeEntry(App app) {
        entry = app;
    }
}
