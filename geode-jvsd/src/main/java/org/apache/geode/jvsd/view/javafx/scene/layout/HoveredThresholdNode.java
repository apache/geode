/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.jvsd.view.javafx.scene.layout;

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