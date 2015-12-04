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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javafx.animation.Interpolator;
import javafx.animation.KeyFrame;
import javafx.animation.KeyValue;
import javafx.animation.Timeline;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.ObjectPropertyBase;
import javafx.beans.property.ReadOnlyObjectProperty;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.collections.ListChangeListener.Change;
import javafx.collections.ObservableList;
import javafx.css.CssMetaData;
import javafx.css.Styleable;
import javafx.css.StyleableBooleanProperty;
import javafx.css.StyleableProperty;
import javafx.geometry.Side;
import javafx.scene.Group;
import javafx.scene.Node;
import javafx.scene.chart.Axis;
import javafx.scene.chart.Chart;
import javafx.scene.layout.Region;
import javafx.scene.shape.ClosePath;
import javafx.scene.shape.Line;
import javafx.scene.shape.LineTo;
import javafx.scene.shape.MoveTo;
import javafx.scene.shape.Path;
import javafx.scene.shape.Rectangle;
import javafx.util.Duration;

import com.sun.javafx.collections.NonIterableChange;
import com.sun.javafx.css.converters.BooleanConverter;

/**
 * Chart base class for all multiple Y axis charts. It is responsible for drawing the multiple
 * axes and the plot content. It contains a list of all content in the plot and
 * implementations of MultiAxisChart can add nodes to this list that need to be rendered.
 */
public abstract class MultiAxisChart<X,Y> extends Chart {

    // -------------- PRIVATE FIELDS -----------------------------------------------------------------------------------

    private int seriesDefaultColorIndex = 0;
    private boolean rangeValid = false;
    private final Line verticalZeroLine = new Line();
    private final Line horizontalZeroLine = new Line();
    private final Path verticalGridLines = new Path();
    private final Path horizontalGridLines = new Path();
    private final Path horizontalRowFill = new Path();
    private final Path verticalRowFill = new Path();
    private final Region plotBackground = new Region();
    private final Group plotArea = new Group(){
        @Override public void requestLayout() {} // suppress layout requests
    };
    private final Group plotContent = new Group();
    private final Rectangle plotAreaClip = new Rectangle();
    /* start pointer of a series linked list. */
    Series<X,Y> begin = null;
    /** This is called when a series is added or removed from the chart */
    private final ListChangeListener<Series<X,Y>> seriesChanged = new ListChangeListener<Series<X,Y>>() {
        @Override public void onChanged(Change<? extends Series<X,Y>> c) {
            while (c.next()) {
                if (c.getRemoved().size() > 0) updateLegend();
                for (Series<X,Y> series : c.getRemoved()) {
                    series.setChart(null);
                    seriesRemoved(series);
                    seriesDefaultColorIndex --;
                }
                for(int i=c.getFrom(); i<c.getTo() && !c.wasPermutated(); i++) {
                    final Series<X,Y> series = c.getList().get(i);
                    // add new listener to data
                    series.setChart(MultiAxisChart.this);
                    // update linkedList Pointers for series
                    if (MultiAxisChart.this.begin == null) {
                        MultiAxisChart.this.begin = getData().get(i);
                        MultiAxisChart.this.begin.setNext(null);
                    } else {
                        if (i == 0) {
                            getData().get(0).setNext(MultiAxisChart.this.begin);
                            begin = getData().get(0);
                        } else {
                            Series<X,Y> ptr = begin;
                            for (int j = 0; j < i -1 && ptr!=null ; j++) {
                                ptr = ptr.getNext();
                            }
                            if (ptr != null) {
                                getData().get(i).setNext(ptr.getNext());
                                ptr.setNext(getData().get(i));
                            }

                        }
                    }
                    // update default color style class
                    series.setDefaultColorStyleClass("default-color"+(seriesDefaultColorIndex % 8));
                    seriesDefaultColorIndex ++;
                    // set default axis
                    // TODO only set it unset
                    if (null == series.getYAxis()) {
                      series.setYAxis(getYAxes().get(0));
                    }
                    // inform sub-classes of series added
                    seriesAdded(series, i);
                }
                if (c.getFrom() < c.getTo()) updateLegend();
                seriesChanged(c);
                // RT-12069, linked list pointers should update when list is permutated.
                if (c.wasPermutated() && getData().size() > 0) {
                    MultiAxisChart.this.begin = getData().get(0);
                    Series<X,Y> ptr = begin;
                    for(int k = 1; k < getData().size() && ptr != null; k++) {
                        ptr.setNext(getData().get(k));
                        ptr = ptr.getNext();
                    }
                    ptr.setNext(null);
                }
            }
            // update axis ranges
            invalidateRange();
            // lay everything out
            requestChartLayout();
        }
    };

    // -------------- PUBLIC PROPERTIES --------------------------------------------------------------------------------

//    private final Axis<X> xAxis;
//    /** Get the X axis, by default it is along the bottom of the plot */
//    public Axis<X> getXAxis() { return xAxis; }

    /** X axis */
    private final ReadOnlyObjectWrapper<Axis<X>> xAxis = new ReadOnlyObjectWrapper<Axis<X>>(null, "xAxis") {
      protected void invalidated() {
        final Axis<X> axis = get();
        axis.autoRangingProperty().addListener(new ChangeListener<Boolean>() {
          @Override
          public void changed(ObservableValue<? extends Boolean> observable, Boolean oldValue, Boolean newValue) {
            if (newValue) {
              // TODO keep lists in series changed listener?
              // TODO find way to select the min/max values only?
//              axis.invalidateRange(getData().stream()
//                  .flatMap((Series<X, Y> s) -> s.getData().stream().map((Data<X, Y> d) -> d.getXValue()))
//                  .collect(Collectors.toList()));
              
              ArrayList<X> d = new ArrayList<>();
              getData().forEach(sx -> {sx.getData().forEach(dx -> {d.add(dx.getXValue());});});
              axis.invalidateRange(d);
            }
          }
        });
      };
    };
    public final Axis<X> getXAxis() { return xAxis.get(); }
    protected void setXAxis(Axis<X> value) { xAxis.set(value); }
    public final ReadOnlyObjectProperty<Axis<X>> xAxisProperty() { return xAxis.getReadOnlyProperty(); }
    
    
    /** Y axes */
    private final ObjectProperty<ObservableList<Axis<Y>>> yAxes = new ObjectPropertyBase<ObservableList<Axis<Y>>>() {
      protected void invalidated() {
        getValue().addListener(new ListChangeListener<Axis<Y>>(){
          @Override
          public void onChanged(javafx.collections.ListChangeListener.Change<? extends Axis<Y>> c) {
            while(c.next()) {
              getChartChildren().removeAll(c.getRemoved());
              getChartChildren().addAll(c.getAddedSubList());
              for (Axis<Y> yAxis : c.getAddedSubList()) {
                yAxisAdded(yAxis);
              }
            }
            requestChartLayout();
          }
        });
      };
      
      @Override
      public Object getBean() {
        return MultiAxisChart.this;
      }

      @Override
      public String getName() {
        return "yAxes";
      }};      
    public ObservableList<Axis<Y>> getYAxes() { return yAxes.getValue(); }
    public final void setYAxes(ObservableList<Axis<Y>> value) { yAxes.setValue(value); }
    public final ObjectProperty<ObservableList<Axis<Y>>> yAxesProperty() { return yAxes; }

    /** Primary Y axis */
    private final ObjectProperty<Axis<Y>> primaryYAxis = new ObjectPropertyBase<Axis<Y>>() {
      {
        addListener(new ChangeListener<Axis<Y>>() {
          @Override
          public void changed(ObservableValue<? extends Axis<Y>> observable, Axis<Y> oldValue, Axis<Y> newValue) {
            if (null != oldValue) {
              oldValue.getStyleClass().remove("chart-primary-y-axis");
            }
            if (null != newValue) {
              newValue.getStyleClass().add("chart-primary-y-axis");
            }
            requestChartLayout();
          }
        });
      }
      
      @Override
      public Object getBean() {
        return MultiAxisChart.this;
      }

      @Override
      public String getName() {
        return "primaryYAxis";
      }
    };
    public final Axis<Y> getPrimaryYAxis() { return primaryYAxis.get(); }
    public final void setPrimaryYAxis(Axis<Y> value) { primaryYAxis.set(value); }
    public final ObjectProperty<Axis<Y>> primaryYAxisProperty() { return primaryYAxis; }

    /** MultiAxisCharts data */
    private ObjectProperty<ObservableList<Series<X,Y>>> data = new ObjectPropertyBase<ObservableList<Series<X,Y>>>() {
        private ObservableList<Series<X,Y>> old;
        @Override protected void invalidated() {
            final ObservableList<Series<X,Y>> current = getValue();
            int saveAnimationState = -1;
            // add remove listeners
            if(old != null) {
                old.removeListener(seriesChanged);
                // Set animated to false so we don't animate both remove and add
                // at the same time. RT-14163 
                // RT-21295 - disable animated only when current is also not null. 
                if (current != null && old.size() > 0) {
                    saveAnimationState = (old.get(0).getChart().getAnimated()) ? 1 : 2;
                    old.get(0).getChart().setAnimated(false);
                }
            }
            if(current != null) current.addListener(seriesChanged);
            // fire series change event if series are added or removed
            if(old != null || current != null) {
                final List<Series<X,Y>> removed = (old != null) ? old : Collections.<Series<X, Y>>emptyList();
                final int toIndex = (current != null) ? current.size() : 0;
                // let series listener know all old series have been removed and new that have been added
                if (toIndex > 0 || !removed.isEmpty()) {
                    seriesChanged.onChanged(new NonIterableChange<Series<X,Y>>(0, toIndex, current){
                        @Override public List<Series<X,Y>> getRemoved() { return removed; }
                        @Override protected int[] getPermutation() {
                            return new int[0];
                        }
                    });
                }
            } else if (old != null && old.size() > 0) {
                // let series listener know all old series have been removed
                seriesChanged.onChanged(new NonIterableChange<Series<X,Y>>(0, 0, current){
                    @Override public List<Series<X,Y>> getRemoved() { return old; }
                    @Override protected int[] getPermutation() {
                        return new int[0];
                    }
                });
            }
            // restore animated on chart.
            if (current != null && current.size() > 0 && saveAnimationState != -1) {
                current.get(0).getChart().setAnimated((saveAnimationState == 1) ? true : false);
            }
            old = current;
        }

        public Object getBean() {
            return MultiAxisChart.this;
        }

        public String getName() {
            return "data";
        }
    };
    public final ObservableList<Series<X,Y>> getData() { return data.getValue(); }
    public final void setData(ObservableList<Series<X,Y>> value) { data.setValue(value); }
    public final ObjectProperty<ObservableList<Series<X,Y>>> dataProperty() { return data; }
    
    /** True if vertical grid lines should be drawn */ 
    private BooleanProperty verticalGridLinesVisible = new StyleableBooleanProperty(true) {
      @Override protected void invalidated() {
          requestChartLayout();
      }

      @Override
      public Object getBean() {
          return MultiAxisChart.this;
      }

      @Override
      public String getName() {
          return "verticalGridLinesVisible";
      }

      @Override
      public CssMetaData<MultiAxisChart<?,?>,Boolean> getCssMetaData() {
          return StyleableProperties.VERTICAL_GRID_LINE_VISIBLE;
      }
  };
  /**
   * Indicates whether vertical grid lines are visible or not.
   *
   * @return true if verticalGridLines are visible else false.
   * @see #verticalGridLinesVisible
   */
  public final boolean getVerticalGridLinesVisible() { return verticalGridLinesVisible.get(); }
  public final void setVerticalGridLinesVisible(boolean value) { verticalGridLinesVisible.set(value); }
  public final BooleanProperty verticalGridLinesVisibleProperty() { return verticalGridLinesVisible; }

  /** True if horizontal grid lines should be drawn */
  private BooleanProperty horizontalGridLinesVisible = new StyleableBooleanProperty(true) {
      @Override protected void invalidated() {
          requestChartLayout();
      }

      @Override
      public Object getBean() {
          return MultiAxisChart.this;
      }

      @Override
      public String getName() {
          return "horizontalGridLinesVisible";
      }

      @Override
      public CssMetaData<MultiAxisChart<?,?>,Boolean> getCssMetaData() {
          return StyleableProperties.HORIZONTAL_GRID_LINE_VISIBLE;
      }
  };
  public final boolean isHorizontalGridLinesVisible() { return horizontalGridLinesVisible.get(); }
  public final void setHorizontalGridLinesVisible(boolean value) { horizontalGridLinesVisible.set(value); }
  public final BooleanProperty horizontalGridLinesVisibleProperty() { return horizontalGridLinesVisible; }

  /** If true then alternative vertical columns will have fills */
  private BooleanProperty alternativeColumnFillVisible = new StyleableBooleanProperty(false) {
      @Override protected void invalidated() {
          requestChartLayout();
      }

      @Override
      public Object getBean() {
          return MultiAxisChart.this;
      }

      @Override
      public String getName() {
          return "alternativeColumnFillVisible";
      }

      @Override
      public CssMetaData<MultiAxisChart<?,?>,Boolean> getCssMetaData() {
          return StyleableProperties.ALTERNATIVE_COLUMN_FILL_VISIBLE;
      }
  };
  public final boolean isAlternativeColumnFillVisible() { return alternativeColumnFillVisible.getValue(); }
  public final void setAlternativeColumnFillVisible(boolean value) { alternativeColumnFillVisible.setValue(value); }
  public final BooleanProperty alternativeColumnFillVisibleProperty() { return alternativeColumnFillVisible; }

  /** If true then alternative horizontal rows will have fills */
  private BooleanProperty alternativeRowFillVisible = new StyleableBooleanProperty(true) {
      @Override protected void invalidated() {
          requestChartLayout();
      }

      @Override
      public Object getBean() {
          return MultiAxisChart.this;
      }

      @Override
      public String getName() {
          return "alternativeRowFillVisible";
      }

      @Override
      public CssMetaData<MultiAxisChart<?,?>,Boolean> getCssMetaData() {
          return StyleableProperties.ALTERNATIVE_ROW_FILL_VISIBLE;
      }
  };
  public final boolean isAlternativeRowFillVisible() { return alternativeRowFillVisible.getValue(); }
  public final void setAlternativeRowFillVisible(boolean value) { alternativeRowFillVisible.setValue(value); }
  public final BooleanProperty alternativeRowFillVisibleProperty() { return alternativeRowFillVisible; }

  /**
   * If this is true and the vertical axis has both positive and negative values then a additional axis line
   * will be drawn at the zero point
   *
   * @defaultValue true
   */
  private BooleanProperty verticalZeroLineVisible = new StyleableBooleanProperty(true) {
      @Override protected void invalidated() {
          requestChartLayout();
      }

      @Override
      public Object getBean() {
          return MultiAxisChart.this;
      }

      @Override
      public String getName() {
          return "verticalZeroLineVisible";
      }

      @Override
      public CssMetaData<MultiAxisChart<?,?>,Boolean> getCssMetaData() {
          return StyleableProperties.VERTICAL_ZERO_LINE_VISIBLE;
      }
  };
  public final boolean isVerticalZeroLineVisible() { return verticalZeroLineVisible.get(); }
  public final void setVerticalZeroLineVisible(boolean value) { verticalZeroLineVisible.set(value); }
  public final BooleanProperty verticalZeroLineVisibleProperty() { return verticalZeroLineVisible; }

  /**
   * If this is true and the horizontal axis has both positive and negative values then a additional axis line
   * will be drawn at the zero point
   *
   * @defaultValue true
   */
  private BooleanProperty horizontalZeroLineVisible = new StyleableBooleanProperty(true) {
      @Override protected void invalidated() {
          requestChartLayout();
      }

      @Override
      public Object getBean() {
          return MultiAxisChart.this;
      }

      @Override
      public String getName() {
          return "horizontalZeroLineVisible";
      }

      @Override
      public CssMetaData<MultiAxisChart<?,?>,Boolean> getCssMetaData() {
          return StyleableProperties.HORIZONTAL_ZERO_LINE_VISIBLE;
      }
  };
  public final boolean isHorizontalZeroLineVisible() { return horizontalZeroLineVisible.get(); }
  public final void setHorizontalZeroLineVisible(boolean value) { horizontalZeroLineVisible.set(value); }
  public final BooleanProperty horizontalZeroLineVisibleProperty() { return horizontalZeroLineVisible; }

    // -------------- PROTECTED PROPERTIES -----------------------------------------------------------------------------

    /**
     * Modifiable and observable list of all content in the plot. This is where implementations of MultiAxisChart should add
     * any nodes they use to draw their plot.
     *
     * @return Observable list of plot children
     */
    protected ObservableList<Node> getPlotChildren() {
        return plotContent.getChildren();
    }

    // -------------- CONSTRUCTOR --------------------------------------------------------------------------------------

    /**
     * Constructs a MultiAxisChart given the two axes. The initial content for the chart
     * plot background and plot area that includes vertical and horizontal grid
     * lines and fills, are added.
     *
     * @param xAxis X Axis for this XY chart
     * @param yAxis Y Axis for this XY chart
     */
    public MultiAxisChart(final Axis<X> xAxis, final Axis<Y> yAxis) {
        setXAxis(xAxis);
        setYAxes(FXCollections.<Axis<Y>>observableArrayList());
        
        if(xAxis.getSide() == null) xAxis.setSide(Side.BOTTOM);
        if(yAxis.getSide() == null) yAxis.setSide(Side.LEFT);
        
        // add initial content to chart content
        getChartChildren().addAll(plotBackground,plotArea, xAxis);
        setPrimaryYAxis(yAxis);
        getYAxes().add(yAxis);

        // We don't want plotArea or plotContent to autoSize or do layout
        plotArea.setAutoSizeChildren(false);
        plotContent.setAutoSizeChildren(false);
        // setup clipping on plot area
        plotAreaClip.setSmooth(false);
        plotArea.setClip(plotAreaClip);
        // add children to plot area
        plotArea.getChildren().addAll(
                verticalRowFill, horizontalRowFill,
                verticalGridLines, horizontalGridLines,
                verticalZeroLine, horizontalZeroLine,
                plotContent);
        // setup css style classes
        plotContent.getStyleClass().setAll("plot-content");
        plotBackground.getStyleClass().setAll("chart-plot-background");
        verticalRowFill.getStyleClass().setAll("chart-alternative-column-fill");
        horizontalRowFill.getStyleClass().setAll("chart-alternative-row-fill");
        verticalGridLines.getStyleClass().setAll("chart-vertical-grid-lines");
        horizontalGridLines.getStyleClass().setAll("chart-horizontal-grid-lines");
        verticalZeroLine.getStyleClass().setAll("chart-vertical-zero-line");
        horizontalZeroLine.getStyleClass().setAll("chart-horizontal-zero-line");
        // mark plotContent as unmanaged as its preferred size changes do not effect our layout
        plotContent.setManaged(false);
        plotArea.setManaged(false);
        // listen to animation on/off and sync to axis
        animatedProperty().addListener(new ChangeListener<Boolean>() {
            @Override public void changed(ObservableValue<? extends Boolean> valueModel, Boolean oldValue, Boolean newValue) {
                xAxis.setAnimated(newValue);
                for (Axis<Y> yAxis : getYAxes()) {
                    yAxis.setAnimated(newValue);
                }
            }
        });
    }

    // -------------- METHODS ------------------------------------------------------------------------------------------

    /**
     * Gets the size of the data returning 0 if the data is null
     *
     * @return The number of items in data, or null if data is null
     */
    final int getDataSize() {
        final ObservableList<Series<X,Y>> data = getData();
        return (data!=null) ? data.size() : 0;
    }

    /** Called when a series's name has changed */
    void seriesNameChanged() {
        updateLegend();
        requestChartLayout();
    }

    void dataItemsChanged(Series<X,Y> series, List<Data<X,Y>> removed, int addedFrom, int addedTo, boolean permutation) {
        for (Data<X,Y> item : removed) {
            dataItemRemoved(item, series);
        }
        for(int i=addedFrom; i<addedTo; i++) {
            Data<X,Y> item = series.getData().get(i);
            dataItemAdded(series, i, item);
        }
        invalidateRange();
        requestChartLayout();
    }

    void dataXValueChanged(Data<X,Y> item) {
        if(item.getCurrentX() != item.getXValue()) invalidateRange();
        dataItemChanged(item);
        if (shouldAnimate()) {
            // TODO look at animate method with KeyFrames for correct Timeline settings
            animate(new Timeline(
                    new KeyFrame(Duration.ZERO, new KeyValue(item.currentXProperty(), item.getCurrentX())),
                    new KeyFrame(Duration.millis(700), new KeyValue(item.currentXProperty(), item.getXValue(), Interpolator.EASE_BOTH)))
            );
        } else {
            item.setCurrentX(item.getXValue());
            requestChartLayout();
        }
    }

    void dataYValueChanged(Data<X,Y> item) {
        if(item.getCurrentY() != item.getYValue()) invalidateRange();
        dataItemChanged(item);
        if (shouldAnimate()) {
            animate(new Timeline(
                    new KeyFrame(Duration.ZERO, new KeyValue(item.currentYProperty(), item.getCurrentY())),
                    new KeyFrame(Duration.millis(700), new KeyValue(item.currentYProperty(), item.getYValue(), Interpolator.EASE_BOTH)))
            );
        } else {
            item.setCurrentY(item.getYValue());
            requestChartLayout();
        }
    }

    void dataExtraValueChanged(Data<X,Y> item) {
        if(item.getCurrentY() != item.getYValue()) invalidateRange();
        dataItemChanged(item);
        if (shouldAnimate()) {
            animate(new Timeline(
                    new KeyFrame(Duration.ZERO, new KeyValue(item.currentYProperty(), item.getCurrentY())),
                    new KeyFrame(Duration.millis(700), new KeyValue(item.currentYProperty(), item.getYValue(), Interpolator.EASE_BOTH)))
            );
        } else {
            item.setCurrentY(item.getYValue());
            requestChartLayout();
        }
    }

    /**
     * This is called whenever a series is added or removed and the legend needs to be updated
     */
    protected void updateLegend(){}

    /**
     * Called when a data item has been added to a series. This is where implementations of MultiAxisChart can create/add new
     * nodes to getPlotChildren to represent this data item. They also may animate that data add with a fade in or
     * similar if animated = true.
     *
     * @param series    The series the data item was added to
     * @param itemIndex The index of the new item within the series
     * @param item      The new data item that was added
     */
    protected abstract void dataItemAdded(Series<X,Y> series, int itemIndex, Data<X,Y> item);

    /**
     * Called when a data item has been removed from data model but it is still visible on the chart. Its still visible
     * so that you can handle animation for removing it in this method. After you are done animating the data item you
     * must call removeDataItemFromDisplay() to remove the items node from being displayed on the chart.
     *
     * @param item   The item that has been removed from the series
     * @param series The series the item was removed from
     */
    protected abstract void dataItemRemoved(Data<X, Y> item, Series<X, Y> series);

    /**
     * Called when a data item has changed, ie its xValue, yValue or extraValue has changed. 
     *
     * @param item    The data item who was changed
     */
    protected abstract void dataItemChanged(Data<X, Y> item);
    /**
     * A series has been added to the charts data model. This is where implementations of MultiAxisChart can create/add new
     * nodes to getPlotChildren to represent this series. Also you have to handle adding any data items that are
     * already in the series. You may simply call dataItemAdded() for each one or provide some different animation for
     * a whole series being added.
     *
     * @param series      The series that has been added
     * @param seriesIndex The index of the new series
     */
    protected abstract void seriesAdded(Series<X, Y> series, int seriesIndex);

    /**
     * A series has been removed from the data model but it is still visible on the chart. Its still visible
     * so that you can handle animation for removing it in this method. After you are done animating the data item you
     * must call removeSeriesFromDisplay() to remove the series from the display list.
     *
     * @param series The series that has been removed
     */
    protected abstract void seriesRemoved(Series<X,Y> series);

    /** Called when each atomic change is made to the list of series for this chart */
    protected void seriesChanged(Change<? extends Series<X, Y>> c) {}

    protected abstract void yAxisAdded(Axis<Y> yAxis);
    
    /**
     * This is called when a data change has happened that may cause the range to be invalid.
     */
    private void invalidateRange() {
        rangeValid = false;
    }

    /**
     * This is called when the range has been invalidated and we need to update it. If the axis are auto
     * ranging then we compile a list of all data that the given axis has to plot and call invalidateRange() on the
     * axis passing it that data.
     */
  protected void updateAxisRange() {
    final Axis<X> xa = getXAxis();
    List<X> xData = null;
    if (xa.isAutoRanging()) {
      xData = new ArrayList<X>();
    }
    
    for (final Axis<Y> ya : getYAxes()) {
      List<Y> yData = null;
      if (ya.isAutoRanging()) {
        yData = new ArrayList<Y>();
      }
      
      for (Series<X, Y> series : getData()) {
        // only consider series that are on this y axis
        if (!ya.equals(series.getYAxis()))
          continue;
        
        for (Data<X, Y> data : series.getData()) {
          if (xData != null)
            xData.add(data.getXValue());
          if (yData != null)
            yData.add(data.getYValue());
        }
      }
      
      if (yData != null) {
        ya.invalidateRange(yData);
      }
    }

    if (xData != null) {
      xa.invalidateRange(xData);
    }
  }

    /**
     * Called to update and layout the plot children. This should include all work to updates nodes representing
     * the plot on top of the axis and grid lines etc. The origin is the top left of the plot area, the plot area with
     * can be got by getting the width of the x axis and its height from the height of the y axis.
     */
    protected abstract void layoutPlotChildren();

    /** @inheritDoc */
    @Override protected final void layoutChartChildren(double top, double left, double width, double height) {
        if(getData() == null) return;
        if (!rangeValid) {
            rangeValid = true;
            if(getData() != null) updateAxisRange();
        }
        // snap top and left to pixels
        top = snapPosition(top);
        left = snapPosition(left);
        // get starting stuff
        final Axis<X> xa = getXAxis();
        final ObservableList<Axis.TickMark<X>> xaTickMarks = xa.getTickMarks();
        final ObservableList<Axis<Y>> yAxes = getYAxes();
        // TODO currently selected axis?
        final Axis<Y> yaPrimary = getPrimaryYAxis();
        final ObservableList<Axis.TickMark<Y>> yaTickMarks = yaPrimary.getTickMarks();
        // check we have 2 axises and know their sides
        // TODO check yar
        if (xa == null || yaPrimary == null || xa.getSide() == null || yaPrimary.getSide() == null) return;
        // try and work out width and height of axises
        double xAxisWidth = 0;
        double xAxisHeight = 30; // guess x axis height to start with
        double yAxisHeight = 0;
        double yAxisWidth[] = new double[yAxes.size()];
        for (int count=0; count<5; count ++) {
            double yAxisWidthSum = 0;
            yAxisHeight = height-xAxisHeight;
            for (int y = 0; y < yAxisWidth.length; y++) {
              yAxisWidthSum += yAxisWidth[y] = yAxes.get(y).prefWidth(yAxisHeight);
            }
            xAxisWidth = width - yAxisWidthSum;
            double newXAxisHeight = xa.prefHeight(xAxisWidth);
            if (newXAxisHeight == xAxisHeight) break;
            xAxisHeight = newXAxisHeight;
        }
        // round axis sizes up to whole integers to snap to pixel
        xAxisWidth = Math.ceil(xAxisWidth);
        xAxisHeight = Math.ceil(xAxisHeight);
        for (int y = 0; y < yAxisWidth.length; y++) {
          yAxisWidth[y] = Math.ceil(yAxisWidth[y]);
        }
        yAxisHeight = Math.ceil(yAxisHeight);
        // calc xAxis height
        double xAxisY = 0;
        if (xa.getSide().equals(Side.TOP)) {
            xa.setVisible(true);
            xAxisY = top+1;
            top += xAxisHeight;
        } else if (xa.getSide().equals(Side.BOTTOM)) {
            xa.setVisible(true);
            xAxisY = top + yAxisHeight;
        } else {
            // X axis should never be left or right so hide
            xa.setVisible(false);
            xAxisHeight = 0;
        }
        // calc yAxis width
        double yAxisX[] = new double[yAxisWidth.length];
        double right = 0;
        for (int y = 0; y < yAxisWidth.length; y++) {
          final Axis<Y> ya = yAxes.get(y);
          yAxisX[y] = 0;
          if (ya.getSide().equals(Side.LEFT)) {
              ya.setVisible(true);
              yAxisX[y] = left +1;
              left += yAxisWidth[y];
          } else if (ya.getSide().equals(Side.RIGHT)) {
              ya.setVisible(true);
              yAxisX[y] = left + xAxisWidth + right;
              right += yAxisWidth[y];
          } else {
              // Y axis should never be top or bottom so hide
              ya.setVisible(false);
              yAxisWidth[y] = 0;
          }
        }
        // resize axises
        xa.resizeRelocate(left, xAxisY, xAxisWidth, xAxisHeight);
        for (int y = 0; y < yAxisWidth.length; y++) {
          yAxes.get(y).resizeRelocate(yAxisX[y], top, yAxisWidth[y], yAxisHeight);
        }
        // When the chart is resized, need to specifically call out the axises
        // to lay out as they are unmanaged.
        xa.requestAxisLayout();
        xa.layout();
        for (int y = 0; y < yAxisWidth.length; y++) {
          final Axis<Y> ya = yAxes.get(y);
          ya.requestAxisLayout();        
          ya.layout();        
        }
        // layout plot content
        layoutPlotChildren();
        // get axis zero points
        final double xAxisZero = xa.getZeroPosition();
        final double yAxisZero = yaPrimary.getZeroPosition();
        // position vertical and horizontal zero lines
        if(Double.isNaN(xAxisZero) || !isVerticalZeroLineVisible()) {
            verticalZeroLine.setVisible(false);
        } else {
            verticalZeroLine.setStartX(left+xAxisZero+0.5);
            verticalZeroLine.setStartY(top);
            verticalZeroLine.setEndX(left+xAxisZero+0.5);
            verticalZeroLine.setEndY(top+yAxisHeight);
            verticalZeroLine.setVisible(true);
        }
        if(Double.isNaN(yAxisZero) || !isHorizontalZeroLineVisible()) {
            horizontalZeroLine.setVisible(false);
        } else {
            horizontalZeroLine.setStartX(left);
            horizontalZeroLine.setStartY(top+yAxisZero+0.5);
            horizontalZeroLine.setEndX(left+xAxisWidth);
            horizontalZeroLine.setEndY(top+yAxisZero+0.5);
            horizontalZeroLine.setVisible(true);
        }
        // layout plot background
        plotBackground.resizeRelocate(left, top, xAxisWidth, yAxisHeight);
        // update clip
        plotAreaClip.setX(left);
        plotAreaClip.setY(top);
        plotAreaClip.setWidth(xAxisWidth+1);
        plotAreaClip.setHeight(yAxisHeight+1);
//??me        plotArea.setClip(new Rectangle(left, top, xAxisWidth, yAxisHeight));
        // position plot group, its origin is the bottom left corner of the plot area
        plotContent.setLayoutX(left);
        plotContent.setLayoutY(top);
        plotContent.requestLayout(); // Note: not sure this is right, maybe plotContent should be resizeable
        // update vertical grid lines
        verticalGridLines.getElements().clear();
        if(getVerticalGridLinesVisible()) {
            for(int i=0; i < xaTickMarks.size(); i++) {
                Axis.TickMark<X> tick = xaTickMarks.get(i);
                double pixelOffset = (i==(xaTickMarks.size()-1)) ? -0.5 : 0.5; 
                final double x = xa.getDisplayPosition(tick.getValue());
                if ((x!=xAxisZero || !isVerticalZeroLineVisible()) && x > 0 && x <= xAxisWidth) {
                    verticalGridLines.getElements().add(new MoveTo(left+x+pixelOffset,top));
                    verticalGridLines.getElements().add(new LineTo(left+x+pixelOffset,top+yAxisHeight));
                }
            }
        }
        // update horizontal grid lines
        horizontalGridLines.getElements().clear();
        if(isHorizontalGridLinesVisible()) {
            for(int i=0; i < yaTickMarks.size(); i++) {
                Axis.TickMark<Y> tick = yaTickMarks.get(i);
                double pixelOffset = (i==(yaTickMarks.size()-1)) ? -0.5 : 0.5;
                final double y = yaPrimary.getDisplayPosition(tick.getValue());
                if ((y!=yAxisZero || !isHorizontalZeroLineVisible()) && y >= 0 && y < yAxisHeight) {
                    horizontalGridLines.getElements().add(new MoveTo(left,top+y+pixelOffset));
                    horizontalGridLines.getElements().add(new LineTo(left+xAxisWidth,top+y+pixelOffset));
                }
            }
        }
        // Note: is there a more efficient way to calculate horizontal and vertical row fills?
        // update vertical row fill
        verticalRowFill.getElements().clear();
        if (isAlternativeColumnFillVisible()) {
            // tick marks are not sorted so get all the positions and sort them
            final List<Double> tickPositionsPositive = new ArrayList<Double>();
            final List<Double> tickPositionsNegative = new ArrayList<Double>();
            for(int i=0; i < xaTickMarks.size(); i++) {
                double pos = xa.getDisplayPosition((X) xaTickMarks.get(i).getValue());
                if (pos == xAxisZero) {
                    tickPositionsPositive.add(pos);
                    tickPositionsNegative.add(pos);
                } else if (pos < xAxisZero) {
                    tickPositionsPositive.add(pos);
                } else {
                    tickPositionsNegative.add(pos);
                }
            }
            Collections.sort(tickPositionsPositive);
            Collections.sort(tickPositionsNegative);
            // iterate over every pair of positive tick marks and create fill
            for(int i=1; i < tickPositionsPositive.size(); i+=2) {
                if((i+1) < tickPositionsPositive.size()) {
                    final double x1 = tickPositionsPositive.get(i);
                    final double x2 = tickPositionsPositive.get(i+1);
                    verticalRowFill.getElements().addAll(
                            new MoveTo(left+x1,top),
                            new LineTo(left+x1,top+yAxisHeight),
                            new LineTo(left+x2,top+yAxisHeight),
                            new LineTo(left+x2,top),
                            new ClosePath());
                }
            }
            // iterate over every pair of positive tick marks and create fill
            for(int i=0; i < tickPositionsNegative.size(); i+=2) {
                if((i+1) < tickPositionsNegative.size()) {
                    final double x1 = tickPositionsNegative.get(i);
                    final double x2 = tickPositionsNegative.get(i+1);
                    verticalRowFill.getElements().addAll(
                            new MoveTo(left+x1,top),
                            new LineTo(left+x1,top+yAxisHeight),
                            new LineTo(left+x2,top+yAxisHeight),
                            new LineTo(left+x2,top),
                            new ClosePath());
                }
            }
        }
        // update horizontal row fill
        horizontalRowFill.getElements().clear();
        if (isAlternativeRowFillVisible()) {
            // tick marks are not sorted so get all the positions and sort them
            final List<Double> tickPositionsPositive = new ArrayList<Double>();
            final List<Double> tickPositionsNegative = new ArrayList<Double>();
            for(int i=0; i < yaTickMarks.size(); i++) {
                double pos = yaPrimary.getDisplayPosition((Y) yaTickMarks.get(i).getValue());
                if (pos == yAxisZero) {
                    tickPositionsPositive.add(pos);
                    tickPositionsNegative.add(pos);
                } else if (pos < yAxisZero) {
                    tickPositionsPositive.add(pos);
                } else {
                    tickPositionsNegative.add(pos);
                }
            }
            Collections.sort(tickPositionsPositive);
            Collections.sort(tickPositionsNegative);
            // iterate over every pair of positive tick marks and create fill
            for(int i=1; i < tickPositionsPositive.size(); i+=2) {
                if((i+1) < tickPositionsPositive.size()) {
                    final double y1 = tickPositionsPositive.get(i);
                    final double y2 = tickPositionsPositive.get(i+1);
                    horizontalRowFill.getElements().addAll(
                            new MoveTo(left, top + y1),
                            new LineTo(left + xAxisWidth, top + y1),
                            new LineTo(left + xAxisWidth, top + y2),
                            new LineTo(left, top + y2),
                            new ClosePath());
                }
            }
            // iterate over every pair of positive tick marks and create fill
            for(int i=0; i < tickPositionsNegative.size(); i+=2) {
                if((i+1) < tickPositionsNegative.size()) {
                    final double y1 = tickPositionsNegative.get(i);
                    final double y2 = tickPositionsNegative.get(i+1);
                    horizontalRowFill.getElements().addAll(
                            new MoveTo(left, top + y1),
                            new LineTo(left + xAxisWidth, top + y1),
                            new LineTo(left + xAxisWidth, top + y2),
                            new LineTo(left, top + y2),
                            new ClosePath());
                }
            }
        }
//
    }

    /**
     * Get the index of the series in the series linked list.
     *
     * @param series The series to find index for
     * @return index of the series in series list
     */
    int getSeriesIndex(Series<X, Y> series) {
        int itemIndex = 0;
        for (Series<X,Y> s = MultiAxisChart.this.begin; s != null; s = s.getNext()) {
            if (s == series) break;
            itemIndex++;
        }
        return itemIndex;
    }

    /**
     * Computes the size of series linked list
     * @return size of series linked list
     */
    int getSeriesSize() {
        int count = 0;
        for (Series<X,Y> d = MultiAxisChart.this.begin; d != null; d = d.getNext()) {
            count++;
        }
        return count;
    }
    
    /**
     * This should be called from seriesRemoved() when you are finished with any animation for deleting the series from
     * the chart. It will remove the series from showing up in the Iterator returned by getDisplayedSeriesIterator().
     *
     * @param series The series to remove
     */
    protected final void removeSeriesFromDisplay(Series<X, Y> series) {
        if (begin == series) {
            begin = series.getNext();
        } else {
            Series<X, Y> ptr = begin;
            while(ptr != null && ptr.getNext() != series) {
                ptr = ptr.getNext();
            }
            if (ptr != null)
            ptr.setNext(series.getNext());
        }
    }

    /**
     * MultiAxisChart maintains a list of all series currently displayed this includes all current series + any series that
     * have recently been deleted that are in the process of being faded(animated) out. This creates and returns a
     * iterator over that list. This is what implementations of MultiAxisChart should use when plotting data.
     *
     * @return iterator over currently displayed series
     */
    protected final Iterator<Series<X,Y>> getDisplayedSeriesIterator() {
        return new Iterator<Series<X, Y>>() {
            private boolean start = true;
            private Series<X,Y> current = begin;
            @Override public boolean hasNext() {
                if (start) {
                    return current != null;
                } else {
                    return current.getNext() != null;
                }
            }
            @Override public Series<X, Y> next() {
                if (start) {
                    start = false;
                } else if (current!=null) {
                    current = current.getNext();
                }
                return current;
            }
            @Override public void remove() {
                throw new UnsupportedOperationException("We don't support removing items from the displayed series list.");
            }
        };
    }

    /**
     * The current displayed data value plotted on the X axis. This may be the same as xValue or different. It is
     * used by MultiAxisChart to animate the xValue from the old value to the new value. This is what you should plot
     * in any custom MultiAxisChart implementations. Some MultiAxisChart chart implementations such as LineChart also use this
     * to animate when data is added or removed.
     */
    protected final X getCurrentDisplayedXValue(Data<X,Y> item) { return item.getCurrentX(); }

    /** Set the current displayed data value plotted on X axis.
     *
     * @param item The MultiAxisChart.Data item from which the current X axis data value is obtained.
     * @see #getCurrentDisplayedXValue
     */
    protected final void setCurrentDisplayedXValue(Data<X,Y> item, X value) { item.setCurrentX(value); }

    /** The current displayed data value property that is plotted on X axis.
     *
     * @param item The MultiAxisChart.Data item from which the current X axis data value property object is obtained.
     * @return The current displayed X data value ObjectProperty.
     * @see #getCurrentDisplayedXValue
     */
    protected final ObjectProperty<X> currentDisplayedXValueProperty(Data<X,Y> item) { return item.currentXProperty(); }

    /**
     * The current displayed data value plotted on the Y axis. This may be the same as yValue or different. It is
     * used by MultiAxisChart to animate the yValue from the old value to the new value. This is what you should plot
     * in any custom MultiAxisChart implementations. Some MultiAxisChart chart implementations such as LineChart also use this
     * to animate when data is added or removed.
     */
    protected final Y getCurrentDisplayedYValue(Data<X,Y> item) { return item.getCurrentY(); }
    
    /**
     * Set the current displayed data value plotted on Y axis.
     *
     * @param item The MultiAxisChart.Data item from which the current Y axis data value is obtained.
     * @see #getCurrentDisplayedYValue
     */
    protected final void setCurrentDisplayedYValue(Data<X,Y> item, Y value) { item.setCurrentY(value); }

    /** The current displayed data value property that is plotted on Y axis.
     *
     * @param item The MultiAxisChart.Data item from which the current Y axis data value property object is obtained.
     * @return The current displayed Y data value ObjectProperty.
     * @see #getCurrentDisplayedYValue
     */
    protected final ObjectProperty<Y> currentDisplayedYValueProperty(Data<X,Y> item) { return item.currentYProperty(); }

    /**
     * The current displayed data extra value. This may be the same as extraValue or different. It is
     * used by MultiAxisChart to animate the extraValue from the old value to the new value. This is what you should plot
     * in any custom MultiAxisChart implementations.
     */
    protected final Object getCurrentDisplayedExtraValue(Data<X,Y> item) { return item.getCurrentExtraValue(); }

    /**
     * Set the current displayed data extra value.
     *
     * @param item The MultiAxisChart.Data item from which the current extra value is obtained.
     * @see #getCurrentDisplayedExtraValue
     */
    protected final void setCurrentDisplayedExtraValue(Data<X,Y> item, Object value) { item.setCurrentExtraValue(value); }

    /**
     * The current displayed extra value property.
     *
     * @param item The MultiAxisChart.Data item from which the current extra value property object is obtained.
     * @return ObjectProperty<Object> The current extra value ObjectProperty
     * @see #getCurrentDisplayedExtraValue
     */
    protected final ObjectProperty<Object> currentDisplayedExtraValueProperty(Data<X,Y> item) { return item.currentExtraValueProperty(); }

    /**
     * MultiAxisChart maintains a list of all items currently displayed this includes all current data + any data items
     * recently deleted that are in the process of being faded out. This creates and returns a iterator over
     * that list. This is what implementations of MultiAxisChart should use when plotting data.
     *
     * @param series The series to get displayed data for
     * @return iterator over currently displayed items from this series
     */
    protected final Iterator<Data<X,Y>> getDisplayedDataIterator(final Series<X,Y> series) {
      return series.getData().iterator();
//        return new Iterator<Data<X, Y>>() {
//            private boolean start = true;
//            private Data<X,Y> current = series.getBegin();
//            @Override public boolean hasNext() {
//                if (start) {
//                    return current != null;
//                } else {
//                    return current.next != null;
//                }
//            }
//            @Override public Data<X, Y> next() {
//                if (start) {
//                    start = false;
//                } else if (current!=null) {
//                    current = current.next;
//                }
//                return current;
//            }
//            @Override public void remove() {
//                throw new UnsupportedOperationException("We don't support removing items from the displayed data list.");
//            }
//        };
    }

//    /**
//     * This should be called from dataItemRemoved() when you are finished with any animation for deleting the item from the
//     * chart. It will remove the data item from showing up in the Iterator returned by getDisplayedDataIterator().
//     *
//     * @param series The series to remove
//     * @param item   The item to remove from series's display list
//     */
//    protected final void removeDataItemFromDisplay(Series<X, Y> series, Data<X, Y> item) {
//        series.removeDataItemRef(item);
//    }

    // -------------- STYLESHEET HANDLING ------------------------------------------------------------------------------

    private static class StyleableProperties {
      private static final CssMetaData<MultiAxisChart<?,?>,Boolean> HORIZONTAL_GRID_LINE_VISIBLE =
          new CssMetaData<MultiAxisChart<?,?>,Boolean>("-fx-horizontal-grid-lines-visible",
              BooleanConverter.getInstance(), Boolean.TRUE) {

          @Override
          public boolean isSettable(MultiAxisChart<?,?> node) {
              return node.horizontalGridLinesVisible == null ||
                      !node.horizontalGridLinesVisible.isBound();
          }

          @SuppressWarnings("unchecked")
          @Override
          public StyleableProperty<Boolean> getStyleableProperty(MultiAxisChart<?,?> node) {
              return (StyleableProperty<Boolean>)node.horizontalGridLinesVisibleProperty();
          }
      };

      private static final CssMetaData<MultiAxisChart<?,?>,Boolean> HORIZONTAL_ZERO_LINE_VISIBLE =
          new CssMetaData<MultiAxisChart<?,?>,Boolean>("-fx-horizontal-zero-line-visible",
              BooleanConverter.getInstance(), Boolean.TRUE) {

          @Override
          public boolean isSettable(MultiAxisChart<?,?> node) {
              return node.horizontalZeroLineVisible == null ||
                      !node.horizontalZeroLineVisible.isBound();
          }

          @SuppressWarnings("unchecked")
          @Override
          public StyleableProperty<Boolean> getStyleableProperty(MultiAxisChart<?,?> node) {
              return (StyleableProperty<Boolean>)node.horizontalZeroLineVisibleProperty();
          }
      };

      private static final CssMetaData<MultiAxisChart<?,?>,Boolean> ALTERNATIVE_ROW_FILL_VISIBLE =
          new CssMetaData<MultiAxisChart<?,?>,Boolean>("-fx-alternative-row-fill-visible",
              BooleanConverter.getInstance(), Boolean.TRUE) {

          @Override
          public boolean isSettable(MultiAxisChart<?,?> node) {
              return node.alternativeRowFillVisible == null ||
                      !node.alternativeRowFillVisible.isBound();
          }

          @SuppressWarnings("unchecked")
          @Override
          public StyleableProperty<Boolean> getStyleableProperty(MultiAxisChart<?,?> node) {
              return (StyleableProperty<Boolean>)node.alternativeRowFillVisibleProperty();
          }
      };

      private static final CssMetaData<MultiAxisChart<?,?>,Boolean> VERTICAL_GRID_LINE_VISIBLE =
          new CssMetaData<MultiAxisChart<?,?>,Boolean>("-fx-vertical-grid-lines-visible",
              BooleanConverter.getInstance(), Boolean.TRUE) {

          @Override
          public boolean isSettable(MultiAxisChart<?,?> node) {
              return node.verticalGridLinesVisible == null ||
                      !node.verticalGridLinesVisible.isBound();
          }

          @SuppressWarnings("unchecked")
          @Override
          public StyleableProperty<Boolean> getStyleableProperty(MultiAxisChart<?,?> node) {
              return (StyleableProperty<Boolean>)node.verticalGridLinesVisibleProperty();
          }
      };

      private static final CssMetaData<MultiAxisChart<?,?>,Boolean> VERTICAL_ZERO_LINE_VISIBLE =
          new CssMetaData<MultiAxisChart<?,?>,Boolean>("-fx-vertical-zero-line-visible",
              BooleanConverter.getInstance(), Boolean.TRUE) {

          @Override
          public boolean isSettable(MultiAxisChart<?,?> node) {
              return node.verticalZeroLineVisible == null ||
                      !node.verticalZeroLineVisible.isBound();
          }

          @SuppressWarnings("unchecked")
          @Override
          public StyleableProperty<Boolean> getStyleableProperty(MultiAxisChart<?,?> node) {
              return (StyleableProperty<Boolean>)node.verticalZeroLineVisibleProperty();
          }
      };

      private static final CssMetaData<MultiAxisChart<?,?>,Boolean> ALTERNATIVE_COLUMN_FILL_VISIBLE =
          new CssMetaData<MultiAxisChart<?,?>,Boolean>("-fx-alternative-column-fill-visible",
              BooleanConverter.getInstance(), Boolean.TRUE) {

          @Override
          public boolean isSettable(MultiAxisChart<?,?> node) {
              return node.alternativeColumnFillVisible == null ||
                      !node.alternativeColumnFillVisible.isBound();
          }

          @SuppressWarnings("unchecked")
          @Override
          public StyleableProperty<Boolean> getStyleableProperty(MultiAxisChart<?,?> node) {
              return (StyleableProperty<Boolean>)node.alternativeColumnFillVisibleProperty();
          }
      };

      private static final List<CssMetaData<? extends Styleable, ?>> STYLEABLES;
      static {
          final List<CssMetaData<? extends Styleable, ?>> styleables =
              new ArrayList<CssMetaData<? extends Styleable, ?>>(Chart.getClassCssMetaData());
          styleables.add(HORIZONTAL_GRID_LINE_VISIBLE);
          styleables.add(HORIZONTAL_ZERO_LINE_VISIBLE);
          styleables.add(ALTERNATIVE_ROW_FILL_VISIBLE);
          styleables.add(VERTICAL_GRID_LINE_VISIBLE);
          styleables.add(VERTICAL_ZERO_LINE_VISIBLE);
          styleables.add(ALTERNATIVE_COLUMN_FILL_VISIBLE);
          STYLEABLES = Collections.unmodifiableList(styleables);
      }
  }
 
    /**
     * @return The CssMetaData associated with this class, which may include the
     * CssMetaData of its super classes.
     * @since JavaFX 8.0
     */
    public static List<CssMetaData<? extends Styleable, ?>> getClassCssMetaData() {
        return StyleableProperties.STYLEABLES;
    }

    /**
     * {@inheritDoc}
     * @since JavaFX 8.0
     */
    @Override
    public List<CssMetaData<? extends Styleable, ?>> getCssMetaData() {
        return getClassCssMetaData();
    }

    // -------------- INNER CLASSES ------------------------------------------------------------------------------------


}
