package ukma.ir;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.image.Image;
import javafx.stage.Stage;
import ukma.ir.controllers.MainController;
import ukma.ir.controllers.ResultController;
import ukma.ir.controllers.StarterController;

public class App extends Application {
    private Scene mainScene, resultScene;
    private Stage primary;

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) throws Exception {
        primary = primaryStage;
        FXMLLoader starterLoader = new FXMLLoader();
        starterLoader.setLocation(getClass().getResource("/fxml/starter-frame.fxml"));
        Scene starterScene = new Scene(starterLoader.load());
        StarterController sc = starterLoader.getController();
        sc.takeEntry(this);

        FXMLLoader mainLoader = new FXMLLoader();
        mainLoader.setLocation(getClass().getResource("/fxml/main-frame.fxml"));
        mainScene = new Scene(mainLoader.load());
        MainController mc = mainLoader.getController();
        mc.takeEntry(this);
        mc.initView();

        FXMLLoader resultLoader = new FXMLLoader();
        resultLoader.setLocation(getClass().getResource("/fxml/result-frame.fxml"));
        resultScene = new Scene(resultLoader.load());
        ResultController rc = resultLoader.getController();
        rc.takeEntry(this);

        primaryStage.setScene(starterScene);
        primaryStage.setTitle("IR system");

        // bind list of answers from main controller to the list view in
        rc.bindList(mc.getModel());
        primaryStage.getIcons().add(new Image(getClass().getResource("/icons/app-icon.png").toString()));
        primaryStage.show();
    }

    public void showMain() {
        primary.setScene(mainScene);
    }

    public void showResult() {
        primary.setScene(resultScene);
    }
}
