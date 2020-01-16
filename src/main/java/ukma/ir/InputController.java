package ukma.ir;

import javafx.fxml.FXML;
import javafx.scene.control.TextField;

public class InputController {
    private String inputPath;
    private String outputPath;

    @FXML
    private TextField inputField;

    @FXML
    private TextField outputField;

    @FXML
    void getInputPath() {
        inputPath = inputField.getText();
    }

    @FXML
    void getOutputPath() {
        outputPath = outputField.getText();
    }

}
