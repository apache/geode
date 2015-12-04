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

import javafx.beans.property.ObjectProperty;
import javafx.beans.property.ReadOnlyObjectProperty;
import javafx.beans.property.StringProperty;
import javafx.collections.ObservableList;
import javafx.scene.Node;
import javafx.scene.chart.Axis;

public interface Series<X, Y> {

  public abstract void setChart(MultiAxisChart<X, Y> value);

  public abstract MultiAxisChart<X, Y> getChart();

  public abstract ReadOnlyObjectProperty<MultiAxisChart<X, Y>> chartProperty();

  public abstract Axis<Y> getYAxis();
  
  // TODO change axis event
  public abstract void setYAxis(Axis<Y> value);

  public abstract ReadOnlyObjectProperty<Axis<Y>> yAxisProperty();

  public abstract void setLegendItem(LegendItem value);

  public abstract LegendItem getLegendItem();

  public abstract ReadOnlyObjectProperty<LegendItem> legendItemProperty();

  public abstract String getName();

  public abstract void setName(String value);

  public abstract StringProperty nameProperty();

  public abstract Node getNode();

  public abstract void setNode(Node value);

  public abstract ObjectProperty<Node> nodeProperty();

  public abstract ObservableList<Data<X, Y>> getData();

  public abstract void setData(ObservableList<Data<X, Y>> value);

  public abstract ObjectProperty<ObservableList<Data<X, Y>>> dataProperty();

  /**
   * Returns a string representation of this {@code Series} object.
   * 
   * @return a string representation of this {@code Series} object.
   */
  public abstract String toString();

//  public abstract void removeDataItemRef(Data<X, Y> item);

  public abstract int getItemIndex(Data<X, Y> item);

  public abstract int getDataSize();

  public abstract String getDefaultColorStyleClass();

  public abstract void setDefaultColorStyleClass(String defaultColorStyleClass);

//  public abstract Data<X, Y> getBegin();
//
//  public abstract void setBegin(Data<X, Y> begin);

  public abstract Series<X, Y> getNext();

  public abstract void setNext(Series<X, Y> next);

  public abstract Iterable<Data<X, Y>> getVisibleData();

//  public abstract Iterable<Data<X, Y>> getData(int from, int to, int limit);


}