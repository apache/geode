/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package org.apache.geode.gradle.testing.isolation;

public class PortRange {
  private final int lowerBound;
  private final int upperBound;

  public PortRange(int lowerBound, int upperBound) {
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
  }

  public int lowerBound() {
    return lowerBound;
  }

  public int upperBound() {
    return upperBound;
  }

  public int size() {
    return upperBound - lowerBound + 1;
  }

  public PortRange partition(int partitionIndex, int numberOfPartitions) {
    int partitionLowerBound = partitionLowerBound(partitionIndex, numberOfPartitions);
    int partitionUpperBound = partitionLowerBound(partitionIndex + 1, numberOfPartitions) - 1;
    return new PortRange(partitionLowerBound, partitionUpperBound);
  }

  @Override
  public String toString() {
    return "[" + +lowerBound + "," + upperBound + ']';
  }

  private int partitionLowerBound(int partitionIndex, int numberOfPartitions) {
    return lowerBound() + size() * partitionIndex / numberOfPartitions;
  }
}
