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

/*
 * CollectionHolder.java
 *
 * Created on May 18, 2005, 3:46 PM
 */

package org.apache.geode.cache.query.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;

public class CollectionHolder implements Serializable, DataSerializable {

  public String[] arr;
  public static String secIds[] = {"SUN", "IBM", "YHOO", "GOOG", "MSFT", "AOL", "APPL", "ORCL",
      "SAP", "DELL", "RHAT", "NOVL", "HP"};

  /** Creates a new instance of CollectionHolder */
  public CollectionHolder() {
    this.arr = new String[10];
    for (int i = 0; i < 5; i++) {
      arr[i] = "" + i;
    }
    for (int i = 5; i < 10; i++) {
      arr[i] = secIds[i - 5];
    }

  }

  public String[] getArr() {
    return this.arr;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.arr = DataSerializer.readStringArray(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeStringArray(this.arr, out);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(arr);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof CollectionHolder)) {
      return false;
    }
    CollectionHolder other = (CollectionHolder) obj;
    if (!Arrays.equals(arr, other.arr)) {
      return false;
    }
    return true;
  }

}
