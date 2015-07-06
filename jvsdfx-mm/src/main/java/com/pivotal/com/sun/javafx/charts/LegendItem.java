package com.pivotal.com.sun.javafx.charts;

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