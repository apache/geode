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

import java.util.stream.Collectors;

import javafx.beans.property.ObjectProperty;
import javafx.beans.property.ReadOnlyObjectProperty;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.StringProperty;
import javafx.beans.property.StringPropertyBase;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.ObservableList;
import javafx.scene.Node;
import javafx.scene.chart.Axis;

/**
 * A named series of data items
 */
public abstract class AbstractSeries<X extends Number,Y extends Number> implements Series<X, Y> {

    // -------------- PRIVATE PROPERTIES ----------------------------------------

    /** the style class for default color for this series */
    protected String defaultColorStyleClass;

    // TODO make property
    @Override
    public String getDefaultColorStyleClass() {
      return defaultColorStyleClass;
    }
    
    @Override
    public void setDefaultColorStyleClass(String defaultColorStyleClass) {
      this.defaultColorStyleClass = defaultColorStyleClass;
    }

//    // TODO remove
//    protected Data<X,Y> begin = null; // start pointer of a data linked list.
//    
//    // TODO remove
//    @Override
//    public Data<X, Y> getBegin() {
//      return begin;
//    }
//
//    // TODO remove
//    @Override
//    public void setBegin(Data<X, Y> begin) {
//      this.begin = begin;
//    }

    /*
     * Next pointer for the next series. We maintain a linkedlist of the
     * serieses  so even after the series is deleted from the list,
     * we have a reference to it - needed by BarChart e.g.
     */
    // TODO remove
    protected Series<X,Y> next = null;

    // TODO remove
    @Override
    public Series<X, Y> getNext() {
      return next;
    }

    // TODO remove
    @Override
    public void setNext(Series<X, Y> next) {
      this.next = next;
    }


    // -------------- PUBLIC PROPERTIES ----------------------------------------

    /** Reference to the chart this series belongs to */
    private final ReadOnlyObjectWrapper<MultiAxisChart<X, Y>> chart = new ReadOnlyObjectWrapper<MultiAxisChart<X, Y>>(this, "chart");

    /* (non-Javadoc)
     * @see com.pivotal.javafx.scene.chart.Series#getChart()
     */
    @Override
    public final MultiAxisChart<X,Y> getChart() { return chart.get(); }
    @Override
    public void setChart(MultiAxisChart<X,Y> value) { chart.set(value); }
    /* (non-Javadoc)
     * @see com.pivotal.javafx.scene.chart.Series#chartProperty()
     */
    @Override
    public final ReadOnlyObjectProperty<MultiAxisChart<X,Y>> chartProperty() { return chart.getReadOnlyProperty(); }

    /** Reference to the Y Axis this chart belongs to */
    private final ReadOnlyObjectWrapper<Axis<Y>> yAxis = new ReadOnlyObjectWrapper<Axis<Y>>(null, "yAxis") {
      protected void invalidated() {
        final Axis<Y> axis = get();
        axis.autoRangingProperty().addListener(new ChangeListener<Boolean>() {
          @Override
          public void changed(ObservableValue<? extends Boolean> observable, Boolean oldValue, Boolean newValue) {
            if (newValue) {
              // TODO keep lists in series changed listener?
              axis.invalidateRange(getData().stream().map((Data<X,Y> d) -> d.getYValue()).collect(Collectors.toList()));
            }
          }
        });
      };
    };
    
    /* (non-Javadoc)
     * @see com.pivotal.javafx.scene.chart.Series#getYAxis()
     */
    @Override
    public final Axis<Y> getYAxis() { return yAxis.get(); }
    // TODO change axis event
    /* (non-Javadoc)
     * @see com.pivotal.javafx.scene.chart.Series#setYAxis(javafx.scene.chart.Axis)
     */
    @Override
    public void setYAxis(Axis<Y> value) { yAxis.set(value); }
    /* (non-Javadoc)
     * @see com.pivotal.javafx.scene.chart.Series#yAxisProperty()
     */
    @Override
    public final ReadOnlyObjectProperty<Axis<Y>> yAxisProperty() { return yAxis.getReadOnlyProperty(); }

    /** Reference to the legend for this series */
    private final ReadOnlyObjectWrapper<LegendItem> legendItem = new ReadOnlyObjectWrapper<>(null, "legendItem");
    /* (non-Javadoc)
     * @see com.pivotal.javafx.scene.chart.Series#getLegendItem()
     */
    @Override
    public final LegendItem getLegendItem() { return legendItem.get(); }
    
    @Override
    public  void setLegendItem(LegendItem value) { legendItem.set(value); }
    /* (non-Javadoc)
     * @see com.pivotal.javafx.scene.chart.Series#legendItemProperty()
     */
    @Override
    public final ReadOnlyObjectProperty<LegendItem> legendItemProperty() { return legendItem.getReadOnlyProperty(); }

    
    /** The user displayable name for this series */
    private final StringProperty name = new StringPropertyBase() {
        @Override protected void invalidated() {
            get(); // make non-lazy
            if(getChart() != null) getChart().seriesNameChanged();
        }

        @Override
        public Object getBean() {
            return AbstractSeries.this;
        }

        @Override
        public String getName() {
            return "name";
        }
    };
    /* (non-Javadoc)
     * @see com.pivotal.javafx.scene.chart.Series#getName()
     */
    @Override
    public final String getName() { return name.get(); }
    /* (non-Javadoc)
     * @see com.pivotal.javafx.scene.chart.Series#setName(java.lang.String)
     */
    @Override
    public final void setName(String value) { name.set(value); }
    /* (non-Javadoc)
     * @see com.pivotal.javafx.scene.chart.Series#nameProperty()
     */
    @Override
    public final StringProperty nameProperty() { return name; }

    /**
     * The node to display for this series. This is created by the chart if it uses nodes to represent the whole
     * series. For example line chart uses this for the line but scatter chart does not use it. This node will be
     * set as soon as the series is added to the chart. You can then get it to add mouse listeners etc.
     */
    private ObjectProperty<Node> node = new SimpleObjectProperty<Node>(this, "node");
    /* (non-Javadoc)
     * @see com.pivotal.javafx.scene.chart.Series#getNode()
     */
    @Override
    public final Node getNode() { return node.get(); }
    /* (non-Javadoc)
     * @see com.pivotal.javafx.scene.chart.Series#setNode(javafx.scene.Node)
     */
    @Override
    public final void setNode(Node value) { node.set(value); }
    /* (non-Javadoc)
     * @see com.pivotal.javafx.scene.chart.Series#nodeProperty()
     */
    @Override
    public final ObjectProperty<Node> nodeProperty() { return node; }


    // -------------- CONSTRUCTORS ----------------------------------------------

    /**
     * Construct a empty series
     */
    public AbstractSeries() {
    }

    /**
     * Constructs a named Series and populates it with the given {@link ObservableList} data.
     *
     * @param name a name for the series
     * @param data ObservableList of MultiAxisChart.Data
     */
    public AbstractSeries(String name, ObservableList<Data<X,Y>> data) {
        setName(name);
    }

    // -------------- PUBLIC METHODS ----------------------------------------------

    /* (non-Javadoc)
     * @see com.pivotal.javafx.scene.chart.Series#toString()
     */ 
    @Override public String toString() {
        return "Series["+getName()+"]";
    }

    // -------------- PRIVATE/PROTECTED METHODS -----------------------------------

    /*
     * The following methods are for manipulating the pointers in the linked list
     * when data is deleted. 
     */
//    @Override
//    public void removeDataItemRef(Data<X,Y> item) {
//        if (begin == item) {
//            begin = item.next;
//        } else {
//            Data<X,Y> ptr = begin;
//            while(ptr != null && ptr.next != item) {
//                ptr = ptr.next;
//            }
//            if(ptr != null) ptr.next = item.next;
//        }
//    }

    @Override
    public int getItemIndex(Data<X,Y> item) {
//        int itemIndex = 0;
//        for (Data<X,Y> d = begin; d != null; d = d.next) {
//            if (d == item) break;
//            itemIndex++;
//        }
//        return itemIndex;
      return getData().indexOf(item);
    }

    @Override
    public int getDataSize() {
//        int count = 0;
//        for (Data<X,Y> d = begin; d != null; d = d.next) {
//            count++;
//        }
//        return count;
      return getData().size();
    }
}