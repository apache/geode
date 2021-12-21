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
 */
package org.apache.geode.internal.cache.persistence.query.mock;

import java.util.Comparator;

/**
 * A comparator for Pair objects that uses two passed in comparators.
 *
 */
public class PairComparator implements Comparator<Pair> {
  private final Comparator xComparator;
  private final Comparator yComparator;

  public PairComparator(Comparator xComparator, Comparator yComparator) {
    this.xComparator = xComparator;
    this.yComparator = yComparator;

  }

  @Override
  public int compare(Pair o1, Pair o2) {
    int result = xComparator.compare(o1.getX(), o2.getX());
    if (result == 0) {
      result = yComparator.compare(o1.getY(), o2.getY());
    }
    return result;
  }

}
