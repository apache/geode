package com.pivotal.jvsd.util;

import org.jfree.chart.ChartPanel;
import org.jfree.chart.panel.AbstractOverlay;
import org.jfree.chart.panel.Overlay;

import javax.swing.*;
import java.awt.*;
import java.awt.geom.Rectangle2D;

/**
 * @author Jens Deppe
 */
public class PanelOverlay extends AbstractOverlay implements Overlay {

  private JPanel panel;

  public PanelOverlay() {
    super();
  }

  public PanelOverlay(JPanel panel, ChartPanel chart) {
    this();
    this.panel = panel;
    this.panel.setOpaque(true);

    // So that we can do absolute positioning
    chart.setLayout(null);
    chart.add(panel);
  }

  @Override
  public void paintOverlay(Graphics2D g2, ChartPanel chart) {
    Rectangle2D dataArea = chart.getScreenDataArea();
    Dimension size = this.panel.getPreferredSize();
    this.panel.setBounds(
        (int) (dataArea.getWidth() - size.width + dataArea.getX()),
        10, size.width, size.height);
  }
}
