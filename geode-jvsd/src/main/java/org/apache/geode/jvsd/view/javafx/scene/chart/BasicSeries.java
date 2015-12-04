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

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import javafx.beans.property.ObjectProperty;
import javafx.beans.property.ObjectPropertyBase;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import javafx.scene.chart.Axis;

import com.sun.javafx.collections.NonIterableChange;

/**
 * A named series of data items
 */
public class BasicSeries<X extends Number, Y extends Number> extends AbstractSeries<X, Y> {

    // -------------- PRIVATE PROPERTIES ----------------------------------------

    private final ListChangeListener<Data<X,Y>> dataChangeListener = new ListChangeListener<Data<X, Y>>() {
        @SuppressWarnings("unchecked")
        @Override public void onChanged(Change<? extends Data<X, Y>> c) {
            while (c.next()) {
                // update data items reference to series
                for (Data<X,Y> item : c.getRemoved()) {
                    item.setSeries(null);
                    item.setToRemove = true;
                }
//                if (c.getAddedSize() > 0) {
//                    for (Data<X,Y> itemPtr = begin; itemPtr != null; itemPtr = itemPtr.next) {
//                        if (itemPtr.setToRemove) {
//                            removeDataItemRef(itemPtr);
//                        }
//                    }
//                }
//                final ObservableList<Data<X, Y>> items = getData();
//                for(int i=c.getFrom(); i<c.getTo(); i++) {
//                    final Data<X, Y> item = items.get(i);
//                    item.setSeries(BasicSeries.this);
//                    // update linkedList Pointers for data in this series
//                    if (begin == null) {
//                        begin = item;
//                        begin.next = null;
//                    } else {
//                        if (i == 0) {
//                            items.get(0).next = begin;
//                            begin = items.get(0);
//                        } else {
//                            // Cant't we assume that the list up to this point is already in order?
//                            // Data<X,Y> ptr = begin;
//                            // for (int j = 0; j < i -1 ; j++) {
//                            // ptr = ptr.next;
//                            // }
//                            // So just grab the item at i - 1 rather than walking to it.
//                            Data<X,Y> ptr = items.get(i - 1);
//                            item.next = ptr.next;
//                            ptr.next = item;
//                        }
//                    }
//                }
                // inform chart
                MultiAxisChart<X,Y> chart = getChart();
                if(chart!=null) chart.dataItemsChanged(BasicSeries.this,
                        (List<Data<X,Y>>)c.getRemoved(), c.getFrom(), c.getTo(), c.wasPermutated());
            }
        }
    };

    // -------------- PUBLIC PROPERTIES ----------------------------------------

    /** ObservableList of data items that make up this series */
    private final ObjectProperty<ObservableList<Data<X,Y>>> data = new ObjectPropertyBase<ObservableList<Data<X,Y>>>() {
        private ObservableList<Data<X,Y>> old;
        @Override protected void invalidated() {
            final ObservableList<Data<X,Y>> current = getValue();
            // add remove listeners
            if(old != null) old.removeListener(dataChangeListener);
            if(current != null) current.addListener(dataChangeListener);
            // fire data change event if series are added or removed
            if(old != null || current != null) {
                final List<Data<X,Y>> removed = (old != null) ? old : Collections.<Data<X,Y>>emptyList();
                final int toIndex = (current != null) ? current.size() : 0;
                // let data listener know all old data have been removed and new data that has been added
                if (toIndex > 0 || !removed.isEmpty()) {
                    dataChangeListener.onChanged(new NonIterableChange<Data<X,Y>>(0, toIndex, current){
                        @Override public List<Data<X,Y>> getRemoved() { return removed; }

                        @Override protected int[] getPermutation() {
                            return new int[0];
                        }
                    });
                }
            } else if (old != null && old.size() > 0) {
                // let series listener know all old series have been removed
                dataChangeListener.onChanged(new NonIterableChange<Data<X,Y>>(0, 0, current){
                    @Override public List<Data<X,Y>> getRemoved() { return old; }
                    @Override protected int[] getPermutation() {
                        return new int[0];
                    }
                });
            }
            old = current;
        }

        @Override
        public Object getBean() {
            return BasicSeries.this;
        }

        @Override
        public String getName() {
            return "data";
        }
    };
    /* (non-Javadoc)
     * @see com.pivotal.javafx.scene.chart.Series#getData()
     */
    @Override
    public final ObservableList<Data<X,Y>> getData() { return data.getValue(); }
    /* (non-Javadoc)
     * @see com.pivotal.javafx.scene.chart.Series#setData(javafx.collections.ObservableList)
     */
    @Override
    public final void setData(ObservableList<Data<X,Y>> value) { data.setValue(value); }
    /* (non-Javadoc)
     * @see com.pivotal.javafx.scene.chart.Series#dataProperty()
     */
    @Override
    public final ObjectProperty<ObservableList<Data<X,Y>>> dataProperty() { return data; }

    // -------------- CONSTRUCTORS ----------------------------------------------

    /**
     * Construct a empty series
     */
    public BasicSeries() {
        this(FXCollections.<Data<X,Y>>observableArrayList());
    }

    /**
     * Constructs a Series and populates it with the given {@link ObservableList} data.
     *
     * @param data ObservableList of MultiAxisChart.Data
     */
    public BasicSeries(ObservableList<Data<X,Y>> data) {
        setData(data);
        for(Data<X,Y> item:data) item.setSeries(this);
    }

    /**
     * Constructs a named Series and populates it with the given {@link ObservableList} data.
     *
     * @param name a name for the series
     * @param data ObservableList of MultiAxisChart.Data
     */
    public BasicSeries(String name, ObservableList<Data<X,Y>> data) {
        this(data);
        setName(name);
    }
    
    @Override
    public Iterable<Data<X, Y>> getVisibleData() {
      final Axis<X> xAxis = getChart().getXAxis();
      final double width = xAxis.getWidth();
      
      List<Data<X, Y>> visible = getData().filtered(new Predicate<Data<X,Y>>() {
        @Override
        public boolean test(Data<X, Y> item) {
          final double p = xAxis.getDisplayPosition(item.getXValue());
          return p >= 0 && p <= width;
        }
      });
      
      return visible;
    }
  
}