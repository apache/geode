package com.pivotal.jvsd.fx;

import com.pivotal.jvsd.controller.RootController;
import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.layout.Pane;
import javafx.stage.Stage;

import java.io.IOException;

/**
 * @author Jens Deppe
 */
public class Main extends Application {

  private Stage primaryStage;

  private RootController controller;

  @Override
  public void start(Stage stage) throws IOException {
    primaryStage = stage;
    primaryStage.setTitle("jVSD");

    FXMLLoader loader = new FXMLLoader(getClass().getResource("/jvsd.fxml"));
    Pane rootLayout = loader.load();

    Scene scene = new Scene(rootLayout);
    scene.getStylesheets().add("/jvsd.css");

    controller = loader.getController();

    ChartManager.getInstance().setRootController(controller);

    primaryStage.setScene(scene);
    primaryStage.show();
  }

  public static void main(String[] args) throws IOException {
    StatFileManager.getInstance().add(args);
    launch(args);
  } 
}

