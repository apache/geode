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
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.Node;
import javafx.scene.chart.Axis;

import org.apache.geode.jvsd.view.javafx.scene.layout.HoveredThresholdNode;

/**
 * A named series of data items
 */
public abstract class ByteBufferNumberSeries<X extends Number, Y extends Number> extends BasicSeries<X, Y> {

  // -------------- PUBLIC PROPERTIES ----------------------------------------

  // TODO remove
  /** ObservableList of data items that make up this series */
  private final ObjectProperty<ByteBuffer> bufferProperty = new SimpleObjectProperty<ByteBuffer>() {
    protected void invalidated() {
      buffer = get();
    }
  };

  // here for speed over getBuffer();
  protected ByteBuffer buffer;

  public final ByteBuffer getBuffer() {
    return bufferProperty.get();
  }

  public final void setBuffer(ByteBuffer value) {
    bufferProperty.setValue(value);
  }

  public final ObjectProperty<ByteBuffer> bufferProperty() {
    return bufferProperty;
  }

//  // TODO remove
//  /** ObservableList of data items that make up this series */
//  private final ObjectProperty<ObservableList<Data<X, Y>>> data = new ObjectPropertyBase<ObservableList<Data<X, Y>>>() {
//    @Override
//    protected void invalidated() {
//      // TODO update buffer, minX, maxX, etc.
//    }
//
//    @Override
//    public Object getBean() {
//      return ByteBufferNumberSeries.this;
//    }
//
//    @Override
//    public String getName() {
//      return "data";
//    }
//  };
//
//  /*
//   * (non-Javadoc)
//   * 
//   * @see com.pivotal.javafx.scene.chart.Series#getData()
//   */
//  // TODO remove
//  @Override
//  public final ObservableList<Data<X, Y>> getData() {
//    return data.getValue();
//    // System.out.println("getData:");
//    // return ;
//  }
//
//  /*
//   * (non-Javadoc)
//   * 
//   * @see
//   * com.pivotal.javafx.scene.chart.Series#setData(javafx.collections.ObservableList
//   * )
//   */
//  // TODO remove
//  @Override
//  public final void setData(ObservableList<Data<X, Y>> value) {
//    data.setValue(value);
//  }
//
//  /*
//   * (non-Javadoc)
//   * 
//   * @see com.pivotal.javafx.scene.chart.Series#dataProperty()
//   */
//  // TODO remove
//  @Override
//  public final ObjectProperty<ObservableList<Data<X, Y>>> dataProperty() {
//    return data;
//  }

  // -------------- CONSTRUCTORS ----------------------------------------------

  /**
   * Construct a empty series
   */
  public ByteBufferNumberSeries() {
    super();
    buffer = null;
  }

  /**
   * Constructs a Series and populates it with the given {@link ObservableList}
   * data.
   *
   * @param data
   *          ObservableList of MultiAxisChart.Data
   */
  public ByteBufferNumberSeries(ByteBuffer buffer) {
    super();
    setBuffer(buffer);
    //setData(FXCollections.observableArrayList(new Data(0, 1000000), new Data(10000000000000l, -100000)));
    setData(FXCollections.observableArrayList(new ThresholdDataCollection(0, getDataSize(), 1000)));
    
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
  public ByteBufferNumberSeries(String name, ByteBuffer buffer) {
    this(buffer);
    setName(name);
  }

  @Override
  public Collection<Data<X, Y>> getVisibleData() {

    final Axis<X> xAxis = getChart().getXAxis();
    final double width = xAxis.getWidth();

    final int end = getDataSize() - 1;

    final double min = getX(0);
    final double max = getX(end);

    final double left = xAxis.getValueForDisplay(0).doubleValue();
    final double right = xAxis.getValueForDisplay(width).doubleValue();

    // find file position for first visible point
    int first;
    if (left <= min) {
      first = 0;
    } else if (left >= max) {
      first = end;
    } else {
      // time series should be linear on x, so guess file position and scan
      first = (int) (((left - min) / (max - min)) * end);
      if (getX(first) < left) {
        first = findFirst(first + 1, left) - 1;
      } else {
        first = findLast(first - 1, left);
      }
    }

    // find file position for last visible point
    int last;
    if (right <= min) {
      last = 0;
    } else if (right >= max) {
      last = end;
    } else {
      // time series should be linear on x, so guess file position and scan
      last = (int) (((right - min) / (max - min)) * end);
      if (getX(last) < right) {
        last = findFirst(last + 1, right);
      } else {
        last = findLast(last - 1, right) + 1;
      }
    }

    // fit threshold to series width
    int threshold = (int) width;
    if (left < min || right > max) {
      // there is space on either/both ends so adjust the threshold
      threshold = Math.max((int) (xAxis.getDisplayPosition(convertX(Math.min(right, max))) - xAxis.getDisplayPosition(convertX(Math.max(left, min)))), 3);
    }
    
    // using 2 times the width makes it look a little smoother. Consider dynamic bucket size algorithm.
    // you can still seem some points blink in and out on zoom and pan
    // maybe used fixed points for bucket boundaries, first/last buckets will shrink by first/last data position
    final Collection<Data<X, Y>> v = new ThresholdDataCollection(first, last - first + 1, threshold);

    return v;
  }



  protected Data<X, Y> getData(final int index) {
    Data<X, Y> data = createData(index);
    data.setNode(createSymbol(data));
    return data;
  }
  
  
  private Node createSymbol(Data<X, Y> data) {
    return new HoveredThresholdNode(NumberFormat.getNumberInstance().format(data.getYValue()));
  }


  // TODO Make these Abstract
  @Override
  public abstract int getDataSize();

  protected abstract Data<X,Y> createData(final int index);

  protected abstract double getX(final int index);

  protected abstract X convertX(double x);

  protected abstract double getY(final int index);

  protected abstract Y convertY(double y);

  protected int findFirst(final int start, final double left) {
    // TODO improve over scan? since likely linear chances are we hit on first try.
    for (int i = start; i < getDataSize(); i++) {
      if (getX(i) > left) {
//        System.out.println("f: " + (i - start));
        return i;
      }
    }

    return -1;
  }

  protected int findLast(final int start, final double right) {
    // TODO improve over scan? since likely linear chances are we hit on first try.
    for (int i = start; i >= 0; i--) {
      if (getX(i) < right) {
//        System.out.println("l: " + (start - i));
        return i;
      }
    }

    return getDataSize() - 1;
  }

  protected class ThresholdDataCollection extends AbstractCollection<Data<X, Y>> {
    final int offset;
    final int length;
    final int threshold;
    final int size;

    public ThresholdDataCollection(final int offset, final int length, final int threshold) {
      this.offset = offset;
      this.length = length;
      this.threshold = threshold;
      this.size = Math.min(length, (int) threshold);

      System.out.println(MessageFormat.format("ThresholdDataCollection: offset={0}, length={1}, threshold={2}, end={3}", offset, length, threshold, offset + length));
    }

    @Override
    public Iterator<Data<X, Y>> iterator() {
      if (length > threshold) {
        return createDownsampleVisibleIterator();
      } else {
        return createVisibleIterator();
      }
    }

    @Override
    public int size() {
      return size;
    }

    protected Iterator<Data<X, Y>> createVisibleIterator() {
      return new VisibleIterator();
    }

    protected Iterator<Data<X, Y>> createDownsampleVisibleIterator() {
      return new DownsampleVisibleIterator();
    }

    protected abstract class AbstractDataIterator implements Iterator<Data<X, Y>> {
      int index = offset;
      Data<X, Y> next = null;
      
      protected abstract void advance();

      protected Data<X, Y> consume() {
        final Data<X, Y> data = next;
        next = null;
        return data;
      }

      @Override
      public boolean hasNext() {
        advance();
        return (null != next);
      }

      @Override
      public Data<X, Y> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        return consume();
      }

    }
    
    protected class VisibleIterator extends AbstractDataIterator {
      
       
      @Override
      protected void advance() {
        if (null != next) {
          return;
        }

        if (index >= (offset + length)) {
          return;
        }

        next = getData(index);
        index++;
      }
    }

    protected class DownsampleVisibleIterator extends AbstractDataIterator {
      // Bucket size. Leave room for start and end data points
      final double bucketSize = (double) (length - 2) / (threshold - 2);

      int bucket = 0;
      int a = index; // Initially a is the first point in the triangle

      public DownsampleVisibleIterator() {
        super();

        System.out.println(MessageFormat.format("DownsampleVisibleIterator: bucketSize={0}", bucketSize));
      }

      @Override
      protected void advance() {
        // TODO when zooming out bound to visible width not chart width
        if (null != next) {
          return;
        }

        if (index >= (offset + length)) {
          return;
        }

//         final long start = System.nanoTime();
//         try {

        if (offset == index) {
          // Always add the first point
          next = getData(index);
          index++;
          return;
        }

        if (bucket < threshold - 2) {
          // final long start2 = System.nanoTime();
          // Calculate point average for next bucket (containing c)
          double pointCX = 0;
          double pointCY = 0;
          int pointCStart = (int) Math.floor((bucket + 1) * bucketSize) + offset + 1;
          int pointCEnd = (int) Math.floor((bucket + 2) * bucketSize) + offset + 1;
          pointCEnd = Math.min(pointCEnd, (offset + length));
          final int pointCSize = pointCEnd - pointCStart;
          for (; pointCStart < pointCEnd; pointCStart++) {
            pointCX += getX(pointCStart);
            pointCY += getY(pointCStart);
          }
          pointCX /= pointCSize;
          pointCY /= pointCSize;
          // System.out.println("time2: " + (System.nanoTime() - start2));

          // Point a
          // TODO cache?
          final double pointAX = getX(a);
          final double pointAY = getY(a);

          // Get the range for bucket b
          int pointBStart = (int) Math.floor((bucket + 0) * bucketSize) + offset + 1;
          final int pointBEnd = (int) Math.floor((bucket + 1) * bucketSize) + offset + 1;
          double maxArea = -1;
          for (; pointBStart < pointBEnd; pointBStart++) {
            index++;
            // Calculate triangle area over three buckets
            final double area = Math.abs((pointAX - pointCX) * (getY(pointBStart) - pointAY) - (pointAX - getX(pointBStart))
                * (pointCY - pointAY)) * 0.5;
            if (area > maxArea) {
              maxArea = area;
              a = pointBStart; // Next a is this b
            }
          }

          // Pick this point from the bucket
          next = getData(a);
          bucket++;
          return;
        }

        // Always add last
        assert ((offset + length - 1) == index);
        next = getData(index);
        index++;
//         } finally {
//         System.out.println(MessageFormat.format("DownsampleVisibleIterator: bucket={0}, time={1}ms",
//         bucket, ((double) System.nanoTime() - start) / 1000000));
//         }
      }
    }

  }
}