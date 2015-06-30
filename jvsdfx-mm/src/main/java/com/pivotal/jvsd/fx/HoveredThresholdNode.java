package com.pivotal.jvsd.fx;

import javafx.scene.Cursor;
import javafx.scene.control.Label;
import javafx.scene.layout.StackPane;

public class HoveredThresholdNode extends StackPane {
  public HoveredThresholdNode(String labelString) {
    setPrefSize(2, 2);

    final Label label = createDataThresholdLabel(labelString);

    setOnMouseEntered(mouseEvent -> {
      getChildren().setAll(label);
      setCursor(Cursor.NONE);
      toFront();
    });
    setOnMouseExited(mouseEvent -> {
      getChildren().clear();
      setCursor(Cursor.CROSSHAIR);
    });
  }

  private Label createDataThresholdLabel(String labelString) {
    final Label label = new Label(labelString);
    // TODO style, colors, etc.
    label.getStyleClass().addAll("chart-line-symbol", "chart-series-line");
    label.setStyle("-fx-font-size: 8pt;");

    label.setMinSize(Label.USE_PREF_SIZE, Label.USE_PREF_SIZE);
    return label;
  }
}