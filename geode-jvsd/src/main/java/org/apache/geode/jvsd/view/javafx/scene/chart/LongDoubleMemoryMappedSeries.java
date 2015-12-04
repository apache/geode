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

import java.nio.ByteBuffer;

import javafx.collections.ObservableList;

/**
 * A named series of data items
 */
public class LongDoubleMemoryMappedSeries extends ByteBufferNumberSeries<Number, Number> {

  // -------------- CONSTRUCTORS ----------------------------------------------

  /**
   * Construct a empty series
   */
  public LongDoubleMemoryMappedSeries() {
    super();
  }

  /**
   * Constructs a Series and populates it with the given {@link ObservableList}
   * data.
   *
   * @param data
   *          ObservableList of MultiAxisChart.Data
   */
  public LongDoubleMemoryMappedSeries(ByteBuffer buffer) {
    super(buffer);
  }

  /**
   * Constructs a named Series and populates it with the given
   * {@link ObservableList} data.
   *
   * @param name
   *          a name for the series
   * @param data
   *          ObservableList of MultiAxisChart.Data
   */
  public LongDoubleMemoryMappedSeries(String name, ByteBuffer buffer) {
    super(name, buffer);
  }

  // BEGIN FORMAT SPECIFIC
  private static final int DATA_X_OFFSET = 0;
  private static final int DATA_X_LENGTH = 8;
  private static final int DATA_Y_OFFSET = DATA_X_OFFSET + DATA_X_LENGTH;
  private static final int DATA_Y_LENGTH = 8;
  private static final int DATA_WIDTH = DATA_Y_OFFSET + DATA_Y_LENGTH;
  
  // TODO Make these Abstract
  @Override
  public int getDataSize() {
    return getBuffer().limit() / DATA_WIDTH;
  }
  
  // TODO Make these Abstract
  private long getXRaw(final int index) {
    return buffer.getLong(index * DATA_WIDTH + DATA_X_OFFSET);
  }

  // TODO Make these Abstract
  private double getYRaw(final int index) {
    return buffer.getDouble(index * DATA_WIDTH + DATA_Y_OFFSET);
  }

  // END FORMAT SPECIFIC
 
  @Override
  final protected Data<Number,Number> createData(int index) {
    return new Data<>(getXRaw(index), getYRaw(index));
  };
    
  @Override
  final protected Long convertX(final double x) {
    return (long) x;
  }
  
  @Override
  final protected Double convertY(final double y) {
    return y;
  }

  @Override
  final protected double getX(final int index) {
    return (double) getXRaw(index);
  };

  @Override
  final protected double getY(final int index) {
    return (double) getYRaw(index);
  }

}