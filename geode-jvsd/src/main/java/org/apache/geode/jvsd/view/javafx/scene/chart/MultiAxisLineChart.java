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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javafx.animation.FadeTransition;
import javafx.animation.Interpolator;
import javafx.animation.KeyFrame;
import javafx.animation.KeyValue;
import javafx.animation.Timeline;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.DoubleProperty;
import javafx.beans.property.SimpleDoubleProperty;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import javafx.css.CssMetaData;
import javafx.css.Styleable;
import javafx.css.StyleableBooleanProperty;
import javafx.css.StyleableProperty;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.Node;
import javafx.scene.chart.Axis;
import javafx.scene.layout.StackPane;
import javafx.scene.shape.LineTo;
import javafx.scene.shape.MoveTo;
import javafx.scene.shape.Path;
import javafx.scene.shape.StrokeLineJoin;
import javafx.util.Duration;

import com.sun.javafx.charts.Legend;
import com.sun.javafx.css.converters.BooleanConverter;

/**
 * Line Chart plots a line connecting the data points in a series. The data points
 * themselves can be represented by symbols optionally. Line charts are usually used
 * to view data trends over time or category. 
 */
public class MultiAxisLineChart<X,Y> extends MultiAxisChart<X,Y> {

    // -------------- PRIVATE FIELDS ------------------------------------------

    /** A multiplier for the Y values that we store for each series, it is used to animate in a new series */
    private Map<Series<X,Y>, DoubleProperty> seriesYMultiplierMap = new HashMap<Series<X,Y>, DoubleProperty>();
    private Legend legend = new Legend();
    // TODO fix animation
//    private Timeline dataRemoveTimeline;
//    private Series<X,Y> seriesOfDataRemoved = null;
//    private Data<X,Y> dataItemBeingRemoved = null;
    private Set<Node> symbols = new HashSet<>();

    // -------------- PUBLIC PROPERTIES ----------------------------------------

    /** When true, CSS styleable symbols are created for any data items that don't have a symbol node specified. */
    private BooleanProperty createSymbols = new StyleableBooleanProperty(true) {
        @Override protected void invalidated() {
          // TODO remove all from symbols list;
          for (Node symbol : symbols) {
            getPlotChildren().remove(symbol);
          }
          symbols.clear();
          // TODO double check that requestChartLayout will rebuild symbols.
//            for (int seriesIndex=0; seriesIndex < getData().size(); seriesIndex ++) {
//                Series<X,Y> series = getData().get(seriesIndex);
//                for (int itemIndex=0; itemIndex < series.getData().size(); itemIndex ++) {
//                    Data<X,Y> item = series.getData().get(itemIndex);
//                    Node symbol = item.getNode();
//                    if(get() && symbol == null) { // create any symbols
//                        symbol = createSymbol(series, getData().indexOf(series), item, itemIndex);
//                        getPlotChildren().add(symbol);
//                    } else if (!get() && symbol != null) { // remove symbols
//                        getPlotChildren().remove(symbol);
//                        symbol = null;
//                        item.setNode(null);
//                    }
//                }
//            }
            requestChartLayout();
        }

        public Object getBean() {
            return MultiAxisLineChart.this;
        }

        public String getName() {
            return "createSymbols";
        }

        public CssMetaData<MultiAxisLineChart<?,?>,Boolean> getCssMetaData() {
            return StyleableProperties.CREATE_SYMBOLS;
        }
    };

    /**
     * Indicates whether symbols for data points will be created or not.
     *
     * @return true if symbols for data points will be created and false otherwise.
     */
    public final boolean getCreateSymbols() { return createSymbols.getValue(); }
    public final void setCreateSymbols(boolean value) { createSymbols.setValue(value); }
    public final BooleanProperty createSymbolsProperty() { return createSymbols; }

    // -------------- CONSTRUCTORS ----------------------------------------------

    /**
     * Construct a new LineChart with the given axis.
     *
     * @param xAxis The x axis to use
     * @param yAxis The y axis to use
     */
    public MultiAxisLineChart(Axis<X> xAxis, Axis<Y> yAxis) {
        this(xAxis, yAxis, FXCollections.<Series<X, Y>>observableArrayList());
    }

    /**
     * Construct a new LineChart with the given axis and data.
     *
     * @param xAxis The x axis to use
     * @param yAxes The y axis to use
     * @param data The data to use, this is the actual list used so any changes to it will be reflected in the chart
     */
    public MultiAxisLineChart(Axis<X> xAxis, Axis<Y> yAxis, ObservableList<Series<X,Y>> data) {
        super(xAxis,yAxis);
        setLegend(legend);
        setData(data);
    }

    // -------------- METHODS ------------------------------------------------------------------------------------------

    @Override protected void dataItemAdded(final Series<X,Y> series, int itemIndex, final Data<X,Y> item) {
        final Node symbol = createSymbol(series, getData().indexOf(series), item, itemIndex);
        if (shouldAnimate()) {
//            if (dataRemoveTimeline != null && dataRemoveTimeline.getStatus().equals(Animation.Status.RUNNING)) {
//                if (seriesOfDataRemoved == series) {
//                    dataRemoveTimeline.stop();
//                    dataRemoveTimeline = null;
//                    getPlotChildren().remove(dataItemBeingRemoved.getNode());
//                    removeDataItemFromDisplay(seriesOfDataRemoved, dataItemBeingRemoved);
//                    seriesOfDataRemoved = null;
//                    dataItemBeingRemoved = null;
//                }
//            }
            boolean animate = false;
            if (itemIndex > 0 && itemIndex < (series.getData().size()-1)) {
                animate = true;
                Data<X,Y> p1 = series.getData().get(itemIndex - 1);
                Data<X,Y> p2 = series.getData().get(itemIndex + 1);
                if (p1 != null && p2 != null) {
                    final Axis<X> xAxis = getXAxis();
                    final Axis<Y> yAxis = series.getYAxis();
                    double x1 = xAxis.toNumericValue(p1.getXValue());
                    double y1 = yAxis.toNumericValue(p1.getYValue());
                    double x3 = xAxis.toNumericValue(p2.getXValue());
                    double y3 = yAxis.toNumericValue(p2.getYValue());

                    double x2 = xAxis.toNumericValue(item.getXValue());
                    //double y2 = getYAxis().toNumericValue(item.getYValue());
                    if (x2 > x1 && x2 < x3) {
                         //1. y intercept of the line : y = ((y3-y1)/(x3-x1)) * x2 + (x3y1 - y3x1)/(x3 -x1)
                        double y = ((y3-y1)/(x3-x1)) * x2 + (x3*y1 - y3*x1)/(x3-x1);
                        item.setCurrentY(yAxis.toRealValue(y));
                        item.setCurrentX(xAxis.toRealValue(x2));
                    } else {
                        //2. we can simply use the midpoint on the line as well..
                        double x = (x3 + x1)/2;
                        double y = (y3 + y1)/2;
                        item.setCurrentX(xAxis.toRealValue(x));
                        item.setCurrentY(yAxis.toRealValue(y));
                    }
                }
            } else if (itemIndex == 0 && series.getData().size() > 1) {
                animate = true;
                item.setCurrentX(series.getData().get(1).getXValue());
                item.setCurrentY(series.getData().get(1).getYValue());
            } else if (itemIndex == (series.getData().size() - 1) && series.getData().size() > 1) {
                animate = true;
                int last = series.getData().size() - 2;
                item.setCurrentX(series.getData().get(last).getXValue());
                item.setCurrentY(series.getData().get(last).getYValue());
            } else if(symbol != null) {
                // fade in new symbol
                FadeTransition ft = new FadeTransition(Duration.millis(500),symbol);
                ft.setToValue(1);
                ft.play();
            }
            if(symbol != null) {
                    getPlotChildren().add(symbol);
                    symbols.add(symbol);
            }
            if (animate) {
                animate(new Timeline(
                    new KeyFrame(Duration.ZERO, new KeyValue(item.currentYProperty(),
                                        item.getCurrentY()),
                                        new KeyValue(item.currentXProperty(),
                                        item.getCurrentX())),
                    new KeyFrame(Duration.millis(700), new KeyValue(item.currentYProperty(),
                                        item.getYValue(), Interpolator.EASE_BOTH),
                                        new KeyValue(item.currentXProperty(),
                                        item.getXValue(), Interpolator.EASE_BOTH)))
                );
            }
            
        } else {
            if (symbol != null) {
              getPlotChildren().add(symbol);
              symbols.add(symbol);
            }
        }
    }

    @Override protected  void dataItemRemoved(final Data<X,Y> item, final Series<X,Y> series) {
        final Node symbol = item.getNode();
        // remove item from sorted list
        // TODO fix animation
//        int itemIndex = series.getItemIndex(item);
//        if (shouldAnimate()) {
//            boolean animate = false;
//            if (itemIndex > 0 && itemIndex < series.getDataSize()) {
//                animate = true;
//                int index=0; Data<X,Y> d;
//                for (d = series.getBegin(); d != null && index != itemIndex - 1; d=d.next) index++;
//                Data<X,Y> p1 = d;
//                Data<X,Y> p2 = (d.next).next;
//                if (p1 != null && p2 != null) {
//                    final Axis<X> xAxis = getXAxis();
//                    final Axis<Y> yAxis = series.getYAxis();
//                    double x1 = xAxis.toNumericValue(p1.getXValue());
//                    double y1 = yAxis.toNumericValue(p1.getYValue());
//                    double x3 = xAxis.toNumericValue(p2.getXValue());
//                    double y3 = yAxis.toNumericValue(p2.getYValue());
//
//                    double x2 = xAxis.toNumericValue(item.getXValue());
//                    double y2 = yAxis.toNumericValue(item.getYValue());
//                    if (x2 > x1 && x2 < x3) {
//    //                //1.  y intercept of the line : y = ((y3-y1)/(x3-x1)) * x2 + (x3y1 - y3x1)/(x3 -x1)
//                        double y = ((y3-y1)/(x3-x1)) * x2 + (x3*y1 - y3*x1)/(x3-x1);
//                        item.setCurrentX(xAxis.toRealValue(x2));
//                        item.setCurrentY(yAxis.toRealValue(y2));
//                        item.setXValue(xAxis.toRealValue(x2));
//                        item.setYValue(yAxis.toRealValue(y));
//                    } else {
//                    //2.  we can simply use the midpoint on the line as well..
//                        double x = (x3 + x1)/2;
//                        double y = (y3 + y1)/2;
//                        item.setCurrentX(xAxis.toRealValue(x));
//                        item.setCurrentY(yAxis.toRealValue(y));
//                    }
//                }
//            } else if (itemIndex == 0 && series.getDataSize() > 1) {
//                animate = true;
//                Iterator<Data<X,Y>> iter = getDisplayedDataIterator(series);
//                if (iter.hasNext()) { // get first data value
//                    Data<X,Y> d = iter.next();
//                    item.setXValue(d.getXValue());
//                    item.setYValue(d.getYValue());
//                }
//            } else if (itemIndex == (series.getDataSize() - 1) && series.getDataSize() > 1) {
//                animate = true;
//                int last = series.getData().size() - 1;
//                item.setXValue(series.getData().get(last).getXValue());
//                item.setYValue(series.getData().get(last).getYValue());
//            } else {
//                // fade out symbol
//                symbol.setOpacity(0);
//                FadeTransition ft = new FadeTransition(Duration.millis(500),symbol);
//                ft.setToValue(0);
//                ft.setOnFinished(new EventHandler<ActionEvent>() {
//                    @Override public void handle(ActionEvent actionEvent) {
//                        getPlotChildren().remove(symbol);
//                        symbols.remove(symbol);
//                        removeDataItemFromDisplay(series, item);
//                    }
//                });
//                ft.play();
//            }
//            if (animate) {
//                dataRemoveTimeline = createDataRemoveTimeline(item, symbol, series);
//                seriesOfDataRemoved = series;
//                dataItemBeingRemoved = item;
//                dataRemoveTimeline.play();
//            }
//        } else {
            getPlotChildren().remove(symbol);
            symbols.remove(symbol);
//            removeDataItemFromDisplay(series, item);
//        }
        //Note: better animation here, point should move from old position to new position at center point between prev and next symbols
    }

    /** @inheritDoc */
    @Override protected void dataItemChanged(Data<X, Y> item) {
    }
    
    @Override protected void seriesChanged(ListChangeListener.Change<? extends Series<X,Y>> c) {
        // Update style classes for all series lines and symbols
        // Note: is there a more efficient way of doing this?
        final Axis<Y> primaryYAxis = getPrimaryYAxis();
        for (int i = 0; i < getDataSize(); i++) {
            final Series<X,Y> s = getData().get(i);
            Node seriesNode = s.getNode();
            if(seriesNode != null) {
              seriesNode.getStyleClass().setAll("chart-series-line", "series" + i, s.getDefaultColorStyleClass());
              if (s.getYAxis() == primaryYAxis) {
                seriesNode.getStyleClass().add("chart-primary-y-axis");
              } else {
                seriesNode.getStyleClass().remove("chart-primary-y-axis");
              }
            }
            // TODO remove decorations from here
            for (int j=0; j < s.getData().size(); j++) {
                final Data<X,Y> item = s.getData().get(j);
                final Node node = item.getNode();
                if(node!=null) {
                  node.getStyleClass().setAll("chart-line-symbol", "series" + i, "data" + j, s.getDefaultColorStyleClass());
                  if (s.getYAxis() == primaryYAxis) {
                    node.getStyleClass().add("chart-primary-y-axis");
                  } else {
                    node.getStyleClass().remove("chart-primary-y-axis");
                  }
                }
            }
        }
    }
    
    @Override protected  void seriesAdded(final Series<X,Y> series, int seriesIndex) {
        // create new path for series
        Path seriesLine = new Path();
        seriesLine.setStrokeLineJoin(StrokeLineJoin.BEVEL);
        series.setNode(seriesLine);

        primaryYAxisProperty().addListener(new ChangeListener<Axis<Y>>() {
          @Override
          public void changed(ObservableValue<? extends Axis<Y>> observable, Axis<Y> oldValue, Axis<Y> newValue) {
            final Axis<Y> yAxis = series.getYAxis();
            if (yAxis == newValue) {
              series.getNode().getStyleClass().add("chart-primary-y-axis");
              for (Data<X, Y> data : series.getData()) {
                final Node node = data.getNode();
                if (node != null) {
                  node.getStyleClass().remove("chart-primary-y-axis");
                }
              }
            } else if (yAxis == oldValue) {
              series.getNode().getStyleClass().remove("chart-primary-y-axis");
              for (Data<X, Y> data : series.getData()) {
                final Node node = data.getNode();
                if (node != null) {
                  node.getStyleClass().add("chart-primary-y-axis");
                }
              }
            }
          }
        });
                
        // create series Y multiplier
        DoubleProperty seriesYAnimMultiplier = new SimpleDoubleProperty(this, "seriesYMultiplier");
        seriesYMultiplierMap.put(series, seriesYAnimMultiplier);
        // handle any data already in series
        if (shouldAnimate()) {
            seriesLine.setOpacity(0);
            seriesYAnimMultiplier.setValue(0d);
        } else {
            seriesYAnimMultiplier.setValue(1d);
        }
        getPlotChildren().add(seriesLine);

        List<KeyFrame> keyFrames = new ArrayList<KeyFrame>();
        if (shouldAnimate()) {
            // animate in new series
            keyFrames.add(new KeyFrame(Duration.ZERO,
                new KeyValue(seriesLine.opacityProperty(), 0),
                new KeyValue(seriesYAnimMultiplier, 0)
            ));
            keyFrames.add(new KeyFrame(Duration.millis(200),
                new KeyValue(seriesLine.opacityProperty(), 1)
            ));
            keyFrames.add(new KeyFrame(Duration.millis(500),
                new KeyValue(seriesYAnimMultiplier, 1)
            ));
        }
        // TOOD symbols
//        for (int j=0; j<series.getData().size(); j++) {
//            Data<X,Y> item = series.getData().get(j);
//            // TODO create a region to hold symbols and just while all children?
//            // setup symbols but don't plot or animate.
//            createSymbol(series, seriesIndex, item, j);
//            if(symbol != null) {
//                if (shouldAnimate()) symbol.setOpacity(0);
//                getPlotChildren().add(symbol);
//                if (shouldAnimate()) {
//                    // fade in new symbol
//                    keyFrames.add(new KeyFrame(Duration.ZERO, new KeyValue(symbol.opacityProperty(), 0)));
//                    keyFrames.add(new KeyFrame(Duration.millis(200), new KeyValue(symbol.opacityProperty(), 1)));
//                }
//            }
//        }
        if (shouldAnimate()) animate(new Timeline(keyFrames.toArray(new KeyFrame[keyFrames.size()])));
    }

    @Override protected  void seriesRemoved(final Series<X,Y> series) {
        // remove all symbol nodes
        seriesYMultiplierMap.remove(series);
        if (shouldAnimate()) {
            // create list of all nodes we need to fade out
            final List<Node> nodes = new ArrayList<Node>();
            nodes.add(series.getNode());
            // TODO visible data, symbols, etc.?
            for (Data<X,Y> d: series.getData()) nodes.add(d.getNode());
            // fade out old and symbols
            KeyValue[] startValues = new KeyValue[nodes.size()];
            KeyValue[] endValues = new KeyValue[nodes.size()];
            for (int j=0; j < nodes.size(); j++) {
                startValues[j]   = new KeyValue(nodes.get(j).opacityProperty(),0);
                endValues[j]       = new KeyValue(nodes.get(j).opacityProperty(),1);
            }
            Timeline tl = new Timeline();
            tl.getKeyFrames().addAll(
                new KeyFrame(Duration.ZERO,startValues),
                new KeyFrame(Duration.millis(900), new EventHandler<ActionEvent>() {
                    @Override public void handle(ActionEvent actionEvent) {
                        getPlotChildren().removeAll(nodes);
                        removeSeriesFromDisplay(series);
                    }
                },endValues)
            );
            tl.play();
        } else {
            getPlotChildren().remove(series.getNode());
            for (Data<X,Y> d:series.getData()) getPlotChildren().remove(d.getNode());
            removeSeriesFromDisplay(series);
        }
    }
    
    @Override
    protected void yAxisAdded(Axis<Y> yAxis) {
    }

    /** @inheritDoc */
    @Override protected void layoutPlotChildren() {
        getPlotChildren().removeAll(symbols);
        symbols.clear();
        for (int seriesIndex=0; seriesIndex < getDataSize(); seriesIndex++) {
            final Series<X, Y> series = getData().get(seriesIndex);
            if(series.getNode() instanceof  Path) {
                // TODO do some dirty conditional re-layout
                final Path seriesLine = (Path)series.getNode();
                seriesLine.getElements().clear();
                final Axis<X> xAxis = getXAxis();
                final Axis<Y> yAxis = series.getYAxis();
                
                final double seriesYAnimMultiplier = seriesYMultiplierMap.get(series).getValue();
                boolean isFirst = true;
                for (Data<X, Y> item : series.getVisibleData()) {
//                for (Data<X, Y> item : series.getData()) {
                  isFirst = layoutItem(seriesLine, xAxis, yAxis, seriesYAnimMultiplier, isFirst, item);
                }
            }
        }
    }
        
    protected boolean layoutItem(final Path seriesLine, final Axis<X> xAxis, final Axis<Y> yAxis, final double seriesYAnimMultiplier, boolean isFirst,
        Data<X, Y> item) {
      final double x = getDisplayPosition(xAxis, item);
      final double y = getDisplayPosition(yAxis, seriesYAnimMultiplier, item);
      if (isFirst) {
        isFirst = false;
        seriesLine.getElements().add(new MoveTo(x, y));
      } else {
        seriesLine.getElements().add(new LineTo(x, y));
      }
      Node symbol = item.getNode();
      if (symbol != null) {
        getPlotChildren().add(symbol);
        symbols.add(symbol);
        final double w = symbol.prefWidth(-1);
        final double h = symbol.prefHeight(-1);
        symbol.resizeRelocate(x - (w / 2), y - (h / 2), w, h);
      }
      return isFirst;
    }

    protected double getDisplayPosition(final Axis<X> xAxis, final Data<X, Y> item) {
      return xAxis.getDisplayPosition(item.getCurrentX());
    }

    protected double getDisplayPosition(final Axis<Y> yAxis, final double seriesYAnimMultiplier, Data<X, Y> item) {
      return yAxis.getDisplayPosition(yAxis.toRealValue(yAxis.toNumericValue(item.getCurrentY()) * seriesYAnimMultiplier));
    }

//    private Timeline createDataRemoveTimeline(final Data<X,Y> item, final Node symbol, final Series<X,Y> series) {
//        Timeline t = new Timeline();
//
//        t.getKeyFrames().addAll(new KeyFrame(Duration.ZERO, new KeyValue(item.currentYProperty(),
//                item.getCurrentY()), new KeyValue(item.currentXProperty(),
//                item.getCurrentX())),
//                new KeyFrame(Duration.millis(500), new EventHandler<ActionEvent>() {
//                    @Override public void handle(ActionEvent actionEvent) {
//                        getPlotChildren().remove(symbol);
//                        removeDataItemFromDisplay(series, item);
//                    }
//                },
//                new KeyValue(item.currentYProperty(),
//                item.getYValue(), Interpolator.EASE_BOTH),
//                new KeyValue(item.currentXProperty(),
//                item.getXValue(), Interpolator.EASE_BOTH))
//        );
//        return t;
//    }

    private Node createSymbol(Series<X, Y> series, int seriesIndex, final Data<X,Y> item, int itemIndex) {
        Node symbol = item.getNode();
        // check if symbol has already been created
        if (symbol == null && getCreateSymbols()) {
            symbol = new StackPane();
            item.setNode(symbol);
        }
        // set symbol styles
        if (symbol != null) symbol.getStyleClass().setAll("chart-line-symbol", "series" + seriesIndex,
                "data" + itemIndex, series.getDefaultColorStyleClass());
        return symbol;
    }

    /**
     * This is called whenever a series is added or removed and the legend needs to be updated
     */
     @Override protected void updateLegend() {
        legend.getItems().clear();
        if (getData() != null) {
            for (int seriesIndex=0; seriesIndex < getData().size(); seriesIndex++) {
                Series<X,Y> series = getData().get(seriesIndex);
                LegendItem legenditem = new LegendItem(series.getName());
                legenditem.getSymbol().getStyleClass().addAll("chart-line-symbol", "series"+seriesIndex, series.getDefaultColorStyleClass());
                legend.getItems().add(legenditem);
                series.setLegendItem(legenditem);
            }
        }
        if (legend.getItems().size() > 0) {
            if (getLegend() == null) {
                setLegend(legend);
            }
        } else {
            setLegend(null);
        }
    }

  
     
     // -------------- STYLESHEET HANDLING --------------------------------------

     private static class StyleableProperties {
         private static final CssMetaData<MultiAxisLineChart<?,?>,Boolean> CREATE_SYMBOLS = 
             new CssMetaData<MultiAxisLineChart<?,?>,Boolean>("-fx-create-symbols",
                 BooleanConverter.getInstance(), Boolean.TRUE) {

             @Override
             public boolean isSettable(MultiAxisLineChart<?,?> node) {
                 return node.createSymbols == null || !node.createSymbols.isBound();
             }

             @SuppressWarnings("unchecked")
             @Override
             public StyleableProperty<Boolean> getStyleableProperty(MultiAxisLineChart<?,?> node) {
                 return (StyleableProperty<Boolean>)node.createSymbolsProperty();
             }
         };

         private static final List<CssMetaData<? extends Styleable, ?>> STYLEABLES;
         static {
             final List<CssMetaData<? extends Styleable, ?>> styleables =
                 new ArrayList<CssMetaData<? extends Styleable, ?>>(MultiAxisChart.getClassCssMetaData());
             styleables.add(CREATE_SYMBOLS);
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


}
