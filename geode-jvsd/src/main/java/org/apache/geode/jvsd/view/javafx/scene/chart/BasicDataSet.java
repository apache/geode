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
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javafx.beans.InvalidationListener;

public class BasicDataSet<X extends Number & Comparable<X>, Y extends Number & Comparable<Y>> implements DataSet<X, Y> {

  protected final List<Data<X,Y>> list = new ArrayList<>();
  
  public void addData(final Data<X,Y> data) {
    list.add(data);

    // TODO fire change;
  }
  
  @Override
  public List<Data<X, Y>> getData(final X from, final X to, final int limit) {
    
    List<Data<X, Y>> visible = list.stream().filter(new Predicate<Data<X,Y>>() {
      @Override
      public boolean test(Data<X, Y> item) {
        final X x = item.getXValue();
        return between(from, to, x);
      }

    }).collect(Collectors.toList());
    
    //System.out.println(getData().size() + " - " + visible.size());
    
    if (visible.size() > limit) {
      visible = getVisibleDataSubsampled(visible, limit);
    }
    
    return visible;
  }
  
  private List<Data<X, Y>> getVisibleDataSubsampled(List<Data<X, Y>> data, int threshold) {
    // Bucket size. Leave room for start and end data points
    final int dataLength = data.size();
    final double bucketSize = (double) (dataLength - 2) / (threshold - 2);
    final ArrayList<Data<X,Y>> sampled = new ArrayList<>(threshold);

    int a = 0; // Initially a is the first point in the triangle
    int nextA = 0;

    sampled.add(data.get(a)); // Always add the first point

    for (int i = 0; i < threshold - 2; i++) {
      // Calculate point average for next bucket (containing c)
      double pointCX = 0;
      double pointCY = 0;
      int pointCStart = (int) Math.floor((i + 1) * bucketSize) + 1;
      int pointCEnd = (int) Math.floor((i + 2) * bucketSize) + 1;
      pointCEnd = pointCEnd < dataLength ? pointCEnd : dataLength;
      final int pointCSize = pointCEnd - pointCStart;
      for (; pointCStart < pointCEnd; pointCStart++) {
        final Data<X, Y> item = data.get(pointCStart);
        // TODO use the axis position? Don't have to assume number type then
        pointCX += item.getXValue().doubleValue();
        pointCY +=item.getYValue().doubleValue();
      }
      pointCX /= pointCSize;
      pointCY /= pointCSize;

      // Point a
      final double pointAX;
      final double pointAY;
      {
        final Data<X, Y> item = data.get(a);
        pointAX = item.getXValue().doubleValue();
        pointAY = item.getYValue().doubleValue();
      }

      // Get the range for bucket b
      int pointBStart = (int) Math.floor((i + 0) * bucketSize) + 1;
      final int pointBEnd = (int) Math.floor((i + 1) * bucketSize) + 1;
      double maxArea = -1;
      for (; pointBStart < pointBEnd; pointBStart++) {
        // Calculate triangle area over three buckets
        final Data<X, Y> item = data.get(pointBStart);
        final double pointBX = item.getXValue().doubleValue();
        final double pointBY = item.getYValue().doubleValue();
        final double area = Math.abs((pointAX - pointCX) * (pointBY - pointAY) - (pointAX - pointBX)
            * (pointCY - pointAY)) * 0.5;
        if (area > maxArea) {
          maxArea = area;
          nextA = pointBStart; // Next a is this b
        }
      }

     // Pick this point from the bucket
      sampled.add(data.get(nextA)); 
      a = nextA; // This a is the next a (chosen b)
    }
    
    // Always add last
    sampled.add(data.get(dataLength - 1));
    
    return sampled;
  }

  @Override
  public void addListener(InvalidationListener listener) {
    // TODO Auto-generated method stub

  }

  @Override
  public void removeListener(InvalidationListener listener) {
    // TODO Auto-generated method stub

  }

  @Override
  public void addListener(DataChangedListener listener) {
    // TODO Auto-generated method stub

  }

  @Override
  public void removeListener(DataChangedListener listener) {
    // TODO Auto-generated method stub

  }

  protected static <T extends Comparable<T>> boolean between(final T from, final T to, final T value) {
    return from.compareTo(value) >= 0 && to.compareTo(value) <= 0;
  }

}
