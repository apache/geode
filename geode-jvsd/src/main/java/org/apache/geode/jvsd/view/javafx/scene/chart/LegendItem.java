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
package org.apache.geode.jvsd.view.javafx.scene.chart;

import java.lang.reflect.Field;

import javafx.scene.Node;
import javafx.scene.control.Label;

import com.sun.javafx.charts.Legend;

/**
 * Extends {@link Legend.LegendItem} to expose label field as property.
 * 
 * @author jbarrett
 *
 */
public class LegendItem extends Legend.LegendItem {

  static final Field labelField;
  static {
    Field field = null;
    try {
      field = Legend.LegendItem.class.getDeclaredField("label");
      field.setAccessible(true);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    labelField = field;
  }

  public LegendItem(String text) {
    super(text);

  }

  public LegendItem(String text, Node symbol) {
    super(text, symbol);
  }

  public Label getLabel() {
    try {
      return (Label) labelField.get(this);
    } catch (IllegalArgumentException | IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

}