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

import java.util.ArrayList;
import java.util.List;

import javafx.animation.KeyFrame;
import javafx.animation.KeyValue;
import javafx.animation.Timeline;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.DoubleProperty;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.ObjectPropertyBase;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleDoubleProperty;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.event.Event;
import javafx.event.EventHandler;
import javafx.event.EventType;
import javafx.geometry.Rectangle2D;
import javafx.scene.Cursor;
import javafx.scene.Node;
import javafx.scene.chart.Axis;
import javafx.scene.chart.ValueAxis;
import javafx.scene.input.MouseButton;
import javafx.scene.input.MouseEvent;
import javafx.scene.input.ScrollEvent;
import javafx.scene.layout.Region;
import javafx.scene.paint.Color;
import javafx.scene.shape.Line;
import javafx.scene.shape.Rectangle;
import javafx.scene.shape.StrokeType;
import javafx.util.Duration;

/**
 * Line chart with crosshairs, zooming and panning.
 * 
 * @author jbarrett
 * 
 * @param <X>
 * @param <Y>
 */
public class AdvancedLineChart<X extends Number, Y extends Number> extends MultiAxisLineChart<X, Y> {

  /** Selected series */
  private final ObjectProperty<Series<X, Y>> selectedSeries = new ObjectPropertyBase<Series<X, Y>>() {
    {
      addListener(new ChangeListener<Series<X, Y>>() {
        @Override
        public void changed(ObservableValue<? extends Series<X, Y>> observable, Series<X, Y> oldValue, Series<X, Y> newValue) {
          if (null != oldValue) {
            oldValue.getNode().getStyleClass().remove("advanced-chart-selected-series");
            final LegendItem legendItem = oldValue.getLegendItem();
            legendItem.getLabel().getStyleClass().remove("advanced-chart-selected-series");
          }
          if (null != newValue) {
            newValue.getNode().toFront();
            newValue.getNode().getStyleClass().add("advanced-chart-selected-series");
            final LegendItem legendItem = newValue.getLegendItem();
            legendItem.getLabel().getStyleClass().add("advanced-chart-selected-series");
          }
          requestChartLayout();
        }
      });
    }

    @Override
    public Object getBean() {
      return AdvancedLineChart.this;
    }

    @Override
    public String getName() {
      return "selectedSeries";
    }
  };

  public final Series<X, Y> getSelectedSeries() {
    return selectedSeries.get();
  }

  public final void setSelectedSeries(Series<X, Y> value) {
    selectedSeries.set(value);
  }

  public final ObjectProperty<Series<X, Y>> selectedSeriesProperty() {
    return selectedSeries;
  }

  private final Node chart;

  // BEGIN Crosshair properties
  private final DoubleProperty crosshairX = new SimpleDoubleProperty();
  private final DoubleProperty crosshairY = new SimpleDoubleProperty();
  final Line crosshairLineX;
  final Line crosshairLineY;
  // END Crosshair properties

  // BEGIN Panning properties
  private double panningLastX;
  private double panningLastY;
  private final SimpleBooleanProperty panning = new SimpleBooleanProperty(false);
  // END Panning properties

  // BEGIN Zooming properties
  private final SimpleDoubleProperty rectX = new SimpleDoubleProperty();
  private final SimpleDoubleProperty rectY = new SimpleDoubleProperty();
  private final SimpleBooleanProperty zooming = new SimpleBooleanProperty(false);

  private final DoubleProperty zoomDurationMillis = new SimpleDoubleProperty(700.0);
  private final BooleanProperty zoomAnimated = new SimpleBooleanProperty(true);
  private final BooleanProperty mouseWheelZoomAllowed = new SimpleBooleanProperty(true);

  private static enum ZoomMode {
    Horizontal, Vertical, Both
  }

  private ZoomMode zoomMode;

  private final Rectangle selectRect;
  private final Timeline zoomAnimation = new Timeline();
  // END Zooming properties

  public AdvancedLineChart(Axis<X> xAxis, Axis<Y> yAxis) {
    super(xAxis, yAxis);

    // Get the chart region
    chart = getChartChildren().get(0).getParent();

    // TODO style

    crosshairLineX = new Line(0, 0, this.getWidth(), 0);
    crosshairLineX.endXProperty().bind(this.widthProperty());
    crosshairLineX.setStroke(Color.ORANGE);
    crosshairLineX.setCursor(Cursor.CROSSHAIR);
    crosshairLineX.setMouseTransparent(true);
    crosshairLineX.translateYProperty().bind(crosshairY);
    getPlotChildren().add(crosshairLineX);

    crosshairLineY = new Line(0, 0, 0, this.getHeight());
    crosshairLineY.endYProperty().bind(this.heightProperty());
    crosshairLineY.setStroke(Color.ORANGE);
    crosshairLineY.setCursor(Cursor.CROSSHAIR);
    crosshairLineY.setMouseTransparent(true);
    crosshairLineY.translateXProperty().bind(crosshairX);
    getPlotChildren().add(crosshairLineY);

    selectRect = new Rectangle(0, 0, 0, 0);
    selectRect.setFill(Color.DODGERBLUE);
    selectRect.setMouseTransparent(true);
    selectRect.setOpacity(0.3);
    selectRect.setStroke(Color.rgb(0, 0x29, 0x66));
    selectRect.setStrokeType(StrokeType.INSIDE);
    selectRect.setStrokeWidth(3.0);
    selectRect.widthProperty().bind(rectX.subtract(selectRect.translateXProperty()));
    selectRect.heightProperty().bind(rectY.subtract(selectRect.translateYProperty()));
    selectRect.visibleProperty().bind(zooming);
    getPlotChildren().add(selectRect);

    chart.addEventHandler(MouseEvent.MOUSE_MOVED, new EventHandler<MouseEvent>() {
      @Override
      public void handle(MouseEvent event) {
        // TODO change events on transforms?
        // TODO bind to transform property because axis change
        final double x = event.getX() - getXShift(getXAxis());
        final double y = event.getY() - getYShift(getPrimaryYAxis());

        // TODO do as properties? Crosshair.visible property?
        if (y > 0 && y < getPrimaryYAxis().getHeight() && x > 0 && x < getXAxis().getWidth()) {
          crosshairX.set(x);
          crosshairY.set(y);
          setCursor(Cursor.CROSSHAIR);
          crosshairLineX.setVisible(true);
          crosshairLineY.setVisible(true);
        } else {
          setCursor(Cursor.DEFAULT);
          crosshairLineX.setVisible(false);
          crosshairLineY.setVisible(false);
        }
      }
    });

    chart.addEventHandler(MouseEvent.MOUSE_PRESSED, new EventHandler<MouseEvent>() {
      @Override
      public void handle(MouseEvent mouseEvent) {
        if (isZoomEvent(mouseEvent))
          setupZooming(mouseEvent.getX(), mouseEvent.getY());
        mouseEvent.consume();
      }
    });

    chart.addEventHandler(MouseEvent.DRAG_DETECTED, new EventHandler<MouseEvent>() {
      @Override
      public void handle(MouseEvent mouseEvent) {
        if (isPanEvent(mouseEvent)) {
          startPanning(mouseEvent.getX(), mouseEvent.getY());
          mouseEvent.consume();
        } else if (isZoomEvent(mouseEvent)) {
          startZooming();
          mouseEvent.consume();
        }
      }
    });

    chart.addEventHandler(MouseEvent.MOUSE_DRAGGED, new EventHandler<MouseEvent>() {
      @Override
      public void handle(MouseEvent mouseEvent) {
        if (panning.get()) {
          doPanning(mouseEvent.getX(), mouseEvent.getY());
          mouseEvent.consume();
        } else if (zooming.get()) {
          doZomming(mouseEvent.getX(), mouseEvent.getY());
          mouseEvent.consume();
        }
      }
    });

    chart.addEventHandler(MouseEvent.MOUSE_RELEASED, new EventHandler<MouseEvent>() {
      @Override
      public void handle(MouseEvent mouseEvent) {
        if (panning.get()) {
          stopPanning();
          mouseEvent.consume();
        } else if (zooming.get()) {
          stopZooming();
          mouseEvent.consume();
        }
      }
    });

    chart.addEventHandler(ScrollEvent.ANY, new MouseWheelZoomHandler());

  }

  public final double getCrosshairX() {
    return crosshairX.getValue();
  }

  public final DoubleProperty crosshairXProperty() {
    return crosshairX;
  }

  public final double getCrosshairY() {
    return crosshairY.getValue();
  }

  public final DoubleProperty crosshairYProperty() {
    return crosshairY;
  }

  @Override
  protected void seriesAdded(final Series<X, Y> series, int seriesIndex) {
    super.seriesAdded(series, seriesIndex);

    series.getNode().addEventHandler(MouseEvent.MOUSE_CLICKED, new EventHandler<MouseEvent>() {
      @Override
      public void handle(MouseEvent event) {
        setSelectedSeries(getSelectedSeries() == series ? null : series);
      }
    });
  }

  @Override
  protected void updateLegend() {
    super.updateLegend();

    for (final Series<X, Y> series : getData()) {
      series.getLegendItem().getLabel().addEventHandler(MouseEvent.MOUSE_CLICKED, new EventHandler<MouseEvent>() {
        @Override
        public void handle(MouseEvent event) {
          setSelectedSeries(getSelectedSeries() == series ? null : series);
        }
      });
    }
  }

  @Override
  protected void yAxisAdded(Axis<Y> yAxis) {
    super.yAxisAdded(yAxis);

    yAxis.addEventHandler(MouseEvent.MOUSE_CLICKED, new EventHandler<MouseEvent>() {
      @Override
      public void handle(MouseEvent event) {
        @SuppressWarnings("unchecked")
        final Axis<Y> yAxis = (Axis<Y>) event.getSource();
        setPrimaryYAxis(yAxis);
      }
    });
  }

  @Override
  protected void layoutPlotChildren() {
    super.layoutPlotChildren();
    
    selectRect.toFront();
    crosshairLineX.toFront();
    crosshairLineY.toFront();
  }
  
  protected boolean isPanEvent(MouseEvent mouseEvent) {
    return mouseEvent.getButton() == MouseButton.SECONDARY || (mouseEvent.getButton() == MouseButton.PRIMARY && mouseEvent.isShortcutDown());
  }

  protected boolean isZoomEvent(MouseEvent mouseEvent) {
    return mouseEvent.getButton() == MouseButton.PRIMARY;
  }

  protected void startPanning(final double fromX, final double fromY) {
    panningLastX = fromX;
    panningLastY = fromY;

    final Axis<X> xAxis = this.getXAxis();

    // TODO check or bind to chart animation?
    // wasXAnimated = xAxis.getAnimated();
    // wasYAnimated = chart.getPrimaryYAxis().getAnimated();

    xAxis.setAnimated(false);
    xAxis.setAutoRanging(false);

    for (Axis<Y> yAxis : getYAxes()) {
      yAxis.setAnimated(false);
      yAxis.setAutoRanging(false);
    }

    // TODO property?
    panning.set(true);
  }

  protected void doPanning(final double toX, final double toY) {
    // TODO animated?
    // TODO do we really need to check this? Filters?
    if (!panning.get()) {
      return;
    }

    final ValueAxis<?> xAxis = (ValueAxis<?>) getXAxis();

    final double dX = (toX - panningLastX) / -xAxis.getScale();
    xAxis.setAutoRanging(false);
    xAxis.setLowerBound(xAxis.getLowerBound() + dX);
    xAxis.setUpperBound(xAxis.getUpperBound() + dX);

    @SuppressWarnings("unchecked")
    final List<ValueAxis<?>> yAxes = ((List<ValueAxis<?>>) (List<?>) getYAxes());
    for (ValueAxis<?> yAxis : yAxes) {
      final double dY = (toY - panningLastY) / -yAxis.getScale();
      yAxis.setAutoRanging(false);
      yAxis.setLowerBound(yAxis.getLowerBound() + dY);
      yAxis.setUpperBound(yAxis.getUpperBound() + dY);
    }

    panningLastX = toX;
    panningLastY = toY;
  }

  protected void stopPanning() {
    if (!panning.get())
      return;

    panning.set(false);

    // TODO should we allow certain axis to not be animated?
    getXAxis().setAnimated(getAnimated());

    for (Axis<Y> yAxis : getYAxes()) {
      yAxis.setAnimated(getAnimated());
    }
  }

  protected void setupZooming(final double x, final double y) {
    final Axis<X> xAxis = getXAxis();
    final Axis<Y> yAxis = getPrimaryYAxis();
    
    final double xShifted = x - getXShift(xAxis);
    final double yShifted = y - getYShift(yAxis);

    final double width = xAxis.getWidth();
    final double height = yAxis.getHeight();
    
    if (xShifted >= 0 && xShifted <= width &&
        yShifted >= 0 && yShifted <= height) {
      selectRect.setTranslateX(xShifted);
      selectRect.setTranslateY(yShifted);
      rectX.set(xShifted);
      rectY.set(yShifted);
      zoomMode = ZoomMode.Both;

    } else if (getComponentArea(xAxis).contains(x, y)) {
      selectRect.setTranslateX(xShifted);
      selectRect.setTranslateY(0);
      rectX.set(xShifted);
      rectY.set(height);
      zoomMode = ZoomMode.Horizontal;

    } else if (getComponentArea(yAxis).contains(x, y)) {
      selectRect.setTranslateX(0);
      selectRect.setTranslateY(yShifted);
      rectX.set(width);
      rectY.set(yShifted);
      zoomMode = ZoomMode.Vertical;
    }
  }

  protected void startZooming() {
    // Don't actually start the selecting process until it's officially a drag
    // But, we saved the original coordinates from where we started.
    // TODO this??
    zooming.set(true);
  }

  protected void doZomming(double x, double y) {
    if (!zooming.get())
      return;

    final Axis<X> xAxis = getXAxis();
    final Axis<Y> yAxis = getPrimaryYAxis();
    
    x -= getXShift(xAxis);
    y -= getYShift(yAxis);
    
    if (zoomMode == ZoomMode.Both || zoomMode == ZoomMode.Horizontal) {
      // Clamp to the selection start
      x = Math.max(x, selectRect.getTranslateX());
      // Clamp to plot area
      x = Math.min(x, xAxis.getWidth());
      rectX.set(x);
    }

    if (zoomMode == ZoomMode.Both || zoomMode == ZoomMode.Vertical) {
      // Clamp to the selection start
      y = Math.max(y, selectRect.getTranslateY());
      // Clamp to plot area
      y = Math.min(y, yAxis.getHeight());
      rectY.set(y);
    }
  }

  protected void stopZooming() {
    if (!zooming.get())
      return;

    // Prevent a silly zoom... I'm still undecided about && vs ||
    if (selectRect.getWidth() == 0.0 || selectRect.getHeight() == 0.0) {
      zooming.set(false);
      return;
    }

    // Rectangle2D zoomWindow = chartInfo.getDataCoordinates(
    // selectRect.getTranslateX(), selectRect.getTranslateY(),
    // rectX.get(), rectY.get()
    // );

    double minX = selectRect.getTranslateX();
    double minY = selectRect.getTranslateY();
    double maxX = rectX.get();
    double maxY = rectY.get();

    // TODO allow zoom in any x/y direction

    if (minX > maxX || minY > maxY) {
      throw new IllegalArgumentException("min > max for X and/or Y");
    }

    final ValueAxis<X> xAxis = (ValueAxis<X>) getXAxis();

    // Axis xAxis = chart.getXAxis();
    // Axis yAxis = chart.getPrimaryYAxis();

    double minDataX = translatePositionToData(xAxis, minX);
    double maxDataX = translatePositionToData(xAxis, maxX);

    // The "low" Y data value is actually at the maxY graphical location as Y
    // graphical axis gets
    // larger as you go down on the screen.
    // double minDataY = yAxis.toNumericValue( yAxis.getValueForDisplay( maxY -
    // yStart ) );
    // double maxDataY = yAxis.toNumericValue( yAxis.getValueForDisplay( minY -
    // yStart ) );
    //
    // return new Rectangle2D( minDataX,
    // minDataY,
    // maxDataX - minDataX,
    // maxDataY - minDataY );

    if (zoomMode == ZoomMode.Both || zoomMode == ZoomMode.Horizontal) {
      xAxis.setAutoRanging(false);
    } else {
      // TODO this doesn't work, try to auto range x when only zooming y??
      ArrayList<X> d = new ArrayList<>();
      getData().forEach(s -> {s.getData().forEach(dy -> {d.add(dy.getXValue());});});
      xAxis.invalidateRange(d);
    }

    @SuppressWarnings("unchecked")
    final List<ValueAxis<Y>> yAxes = ((List<ValueAxis<Y>>) (List<?>) getYAxes());
    if (zoomMode == ZoomMode.Both || zoomMode == ZoomMode.Vertical) {
      for (ValueAxis<Y> yAxis : yAxes) {
        yAxis.setAutoRanging(false);
      }
    } else {
      // TODO this doesn't work, try to auto range y when only zooming x
      for (ValueAxis<Y> yAxis : yAxes) {
      ArrayList<Y> d = new ArrayList<>();
      getData().forEach(s -> {s.getData().forEach(dy -> {d.add(dy.getYValue());});});
      yAxis.invalidateRange(d);
      }
    }
    
    if (zoomAnimated.get()) {
      zoomAnimation.stop();

      final KeyValue[] startFrames = new KeyValue[2 + (yAxes.size() * 2)];
      final KeyValue[] stopFrames = new KeyValue[2 + (yAxes.size() * 2)];
      int startFramesIndex = 0;
      int stopFramesIndex = 0;
      if (zoomMode == ZoomMode.Both || zoomMode == ZoomMode.Horizontal) {
        startFrames[startFramesIndex++] = new KeyValue(xAxis.lowerBoundProperty(), xAxis.getLowerBound());
        startFrames[startFramesIndex++] = new KeyValue(xAxis.upperBoundProperty(), xAxis.getUpperBound());
        stopFrames[stopFramesIndex++] = new KeyValue(xAxis.lowerBoundProperty(), minDataX);
        stopFrames[stopFramesIndex++] = new KeyValue(xAxis.upperBoundProperty(), maxDataX);
      }
      
      if (zoomMode == ZoomMode.Both || zoomMode == ZoomMode.Vertical) {
        for (ValueAxis<?> yAxis : yAxes) {
          final double minDataY = translatePositionToData(yAxis, maxY);
          final double maxDataY = translatePositionToData(yAxis, minY);
          startFrames[startFramesIndex++] = new KeyValue(yAxis.lowerBoundProperty(), yAxis.getLowerBound());
          startFrames[startFramesIndex++] = new KeyValue(yAxis.upperBoundProperty(), yAxis.getUpperBound());
          stopFrames[stopFramesIndex++] = new KeyValue(yAxis.lowerBoundProperty(), minDataY);
          stopFrames[stopFramesIndex++] = new KeyValue(yAxis.upperBoundProperty(), maxDataY);
        }
      }
      
      zoomAnimation.getKeyFrames().setAll(new KeyFrame(Duration.ZERO, startFrames), new KeyFrame(Duration.millis(zoomDurationMillis.get()), stopFrames));
      zoomAnimation.play();
    } else {
      zoomAnimation.stop();
      if (zoomMode == ZoomMode.Both || zoomMode == ZoomMode.Horizontal) {
      xAxis.setLowerBound(minDataX);
      xAxis.setUpperBound(maxDataX);
      }
      if (zoomMode == ZoomMode.Both || zoomMode == ZoomMode.Vertical) {
        for (ValueAxis<?> yAxis : yAxes) {
          final double minDataY = translatePositionToData(yAxis, maxY);
          final double maxDataY = translatePositionToData(yAxis, minY);
          yAxis.setLowerBound(minDataY);
          yAxis.setUpperBound(maxDataY);
        }
      }
    }

    zooming.set(false);
  }

  protected Rectangle2D getComponentArea(Region childRegion) {
    double xStart = getXShift(childRegion);
    double yStart = getYShift(childRegion);

    return new Rectangle2D(xStart, yStart, childRegion.getWidth(), childRegion.getHeight());
  }

  protected static final double getXShift(Node node) {
    return node.getLocalToParentTransform().getTx();
  }

  protected static final double getYShift(Node node) {
    return node.getLocalToParentTransform().getTy();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected static final double translatePositionToData(ValueAxis axis, double position) {
    return axis.toNumericValue(axis.getValueForDisplay(position));
  }

  protected class MouseWheelZoomHandler implements EventHandler<ScrollEvent> {
    // TODO never getting start event
    private boolean ignoring = false;

    @Override
    public void handle(ScrollEvent event) {
      EventType<? extends Event> eventType = event.getEventType();
      if (eventType == ScrollEvent.SCROLL_STARTED) {
        // mouse wheel events never send SCROLL_STARTED, bullshit
        ignoring = false;
      } else if (eventType == ScrollEvent.SCROLL_FINISHED) {
        // end non-mouse wheel event, never comes either
        ignoring = false;

      } else if (eventType == ScrollEvent.SCROLL &&
      // If we are allowing mouse wheel zooming
          mouseWheelZoomAllowed.get() &&
          // If we aren't between SCROLL_STARTED and SCROLL_FINISHED
          !ignoring &&
          // inertia from non-wheel gestures might have touch count of 0
          // !event.isInertia() &&
          // Only care about vertical wheel events
          event.getDeltaY() != 0 &&
          // mouse wheel always has touch count of 0
          event.getTouchCount() == 0) {

        // If we are are doing a zoom animation, stop it. Also of note is that
        // we don't zoom the
        // mouse wheel zooming. Because the mouse wheel can "fly" and generate a
        // lot of events,
        // animation doesn't work well. Plus, as the mouse wheel changes the
        // view a small amount in
        // a predictable way, it "looks like" an animation when you roll it.
        // We might experiment with mouse wheel zoom animation in the future,
        // though.
        zoomAnimation.stop();

        final ValueAxis<?> xAxis = (ValueAxis<?>) getXAxis();

        // If we wheel zoom on either axis, we restrict zooming to that axis
        // only, else if anywhere
        // else, including the plot area, zoom both axes.
        ZoomMode zoomMode;
        final double eventX = event.getX();
        final double eventY = event.getY();
        if (getComponentArea(xAxis).contains(eventX, eventY)) {
          zoomMode = ZoomMode.Horizontal;
        } else if (getComponentArea(getPrimaryYAxis()).contains(eventX, eventY)) {
          // TODO other y axes
          zoomMode = ZoomMode.Vertical;
        } else {
          zoomMode = ZoomMode.Both;
        }

        // At this point we are a mouse wheel event, based on everything I've
        // read
        // Point2D dataCoords = chartInfo.getDataCoordinates( eventX, eventY );

        // TODO shift properties
        final double xStart = getXShift(xAxis);
        final double yStart = getYShift(getPrimaryYAxis());

        double distance = event.getDeltaY();

        // TODO accelerator property
        double zoomAmount = 0.0005 * distance;

        if (zoomMode == ZoomMode.Both || zoomMode == ZoomMode.Horizontal) {
          // Determine the proportion of change to the lower and upper bounds
          // based on how far the
          // cursor is along the axis.
          final double dataX = translatePositionToData(xAxis, eventX - xStart);
          final double xZoomBalance = getBalance(dataX, xAxis.getLowerBound(), xAxis.getUpperBound());
          final double xZoomDelta = (xAxis.getUpperBound() - xAxis.getLowerBound()) * zoomAmount;
          xAxis.setAutoRanging(false);
          xAxis.setLowerBound(xAxis.getLowerBound() - xZoomDelta * xZoomBalance);
          xAxis.setUpperBound(xAxis.getUpperBound() + xZoomDelta * (1 - xZoomBalance));
        }

        if (zoomMode == ZoomMode.Both || zoomMode == ZoomMode.Vertical) {
          @SuppressWarnings("unchecked")
          final List<ValueAxis<?>> yAxes = ((List<ValueAxis<?>>) (List<?>) getYAxes());
          for (final ValueAxis<?> yAxis : yAxes) {
            // Determine the proportion of change to the lower and upper bounds
            // based on how far the
            // cursor is along the axis.
            final double dataY = translatePositionToData(yAxis, eventY - yStart);
            final double yZoomBalance = getBalance(dataY, yAxis.getLowerBound(), yAxis.getUpperBound());
            final double yZoomDelta = (yAxis.getUpperBound() - yAxis.getLowerBound()) * zoomAmount;
            yAxis.setAutoRanging(false);
            yAxis.setLowerBound(yAxis.getLowerBound() - yZoomDelta * yZoomBalance);
            yAxis.setUpperBound(yAxis.getUpperBound() + yZoomDelta * (1 - yZoomBalance));
          }
        }
      }
    }
  }

  protected static final double getBalance(double val, double min, double max) {
    if (val <= min)
      return 0.0;
    else if (val >= max)
      return 1.0;

    return (val - min) / (max - min);
  }

}