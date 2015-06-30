package com.pivotal.jvsd.util;

import org.jfree.chart.plot.DefaultDrawingSupplier;

/**
 * @author Jens Deppe
 */
public class VSDDrawingSupplier extends DefaultDrawingSupplier {
  public VSDDrawingSupplier() {

    super(DefaultDrawingSupplier.DEFAULT_PAINT_SEQUENCE,
        DefaultDrawingSupplier.DEFAULT_FILL_PAINT_SEQUENCE,
        DefaultDrawingSupplier.DEFAULT_OUTLINE_PAINT_SEQUENCE,
        DefaultDrawingSupplier.DEFAULT_STROKE_SEQUENCE,
        DefaultDrawingSupplier.DEFAULT_OUTLINE_STROKE_SEQUENCE,
        DefaultDrawingSupplier.DEFAULT_SHAPE_SEQUENCE);
  }
}
