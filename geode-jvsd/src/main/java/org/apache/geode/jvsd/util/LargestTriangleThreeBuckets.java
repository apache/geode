/* 
 * Copied and ported from https://raw.githubusercontent.com/sveinn-steinarsson/flot-downsample/master/jquery.flot.downsample.js
 * Portions of this file are under the following license:
 * 
 * The MIT License
 * 
 * Copyright (c) 2013 by Sveinn Steinarsson
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.apache.geode.jvsd.util;

/**
 * Java implementation of Largest Triangle Three Bucket downsampling algorithm
 * based on http://skemman.is/stream/get/1946/15343/37285/3/SS_MSthesis.pdf.
 * 
 * @author jbarrett
 * 
 */
public class LargestTriangleThreeBuckets {

  public static final Number[][] downsample(final Number[][] data, final int threshold) {

    final int dataLength = data.length;
    if (threshold >= dataLength || 0 == threshold) {
      return data; // Nothing to do
    }

    final Number[][] sampled = new Number[threshold][];
    int sampledIndex = 0;

    // Bucket size. Leave room for start and end data points
    final double bucketSize = (double) (dataLength - 2) / (threshold - 2);

    int a = 0; // Initially a is the first point in the triangle
    int nextA = 0;

    sampled[sampledIndex++] = data[a]; // Always add the first point

    for (int i = 0; i < threshold - 2; i++) {

      // Calculate point average for next bucket (containing c)
      double pointCX = 0;
      double pointCY = 0;
      int pointCStart = (int) Math.floor((i + 1) * bucketSize) + 1;
      int pointCEnd = (int) Math.floor((i + 2) * bucketSize) + 1;
      pointCEnd = pointCEnd < dataLength ? pointCEnd : dataLength;
      final int pointCSize = pointCEnd - pointCStart;
      for (; pointCStart < pointCEnd; pointCStart++) {
        pointCX += data[pointCStart][0].doubleValue(); // TODO DATE??
        pointCY += data[pointCStart][1].doubleValue();
      }
      pointCX /= pointCSize;
      pointCY /= pointCSize;

      // Point a
      double pointAX = data[a][0].doubleValue(); // TODO Date
      double pointAY = data[a][1].doubleValue();

      // Get the range for bucket b
      int pointBStart = (int) Math.floor((i + 0) * bucketSize) + 1;
      final int pointBEnd = (int) Math.floor((i + 1) * bucketSize) + 1;
      double maxArea = -1;
      Number[] maxAreaPoint = null;
      for (; pointBStart < pointBEnd; pointBStart++) {
        // Calculate triangle area over three buckets
        final double area = Math.abs((pointAX - pointCX) * (data[pointBStart][1].doubleValue() - pointAY) - (pointAX - data[pointBStart][0].doubleValue())
            * (pointCY - pointAY)) * 0.5;
        if (area > maxArea) {
          maxArea = area;
          maxAreaPoint = data[pointBStart];
          nextA = pointBStart; // Next a is this b
        }
      }

      sampled[sampledIndex++] = maxAreaPoint; // Pick this point from the bucket
      a = nextA; // This a is the next a (chosen b)
    }

    sampled[sampledIndex++] = data[dataLength - 1]; // Always add last

    return sampled;
  }

}