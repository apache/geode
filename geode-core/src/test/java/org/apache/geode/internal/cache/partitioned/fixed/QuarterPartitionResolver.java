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
package org.apache.geode.internal.cache.partitioned.fixed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.Set;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.FixedPartitionResolver;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.internal.cache.xmlcache.Declarable2;

public class QuarterPartitionResolver
    implements FixedPartitionResolver, Declarable2, DataSerializable {
  private Properties resolveProps;

  public QuarterPartitionResolver() {
    this.resolveProps = new Properties();
    this.resolveProps.setProperty("routingType", "key");
  }

  int numBuckets;

  public String getPartitionName(EntryOperation opDetails, Set allAvailablePartitions) {
    Date date = (Date) opDetails.getKey();
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    int month = cal.get(Calendar.MONTH);
    if (month == 0 || month == 1 || month == 2) {
      return "Q1";
    } else if (month == 3 || month == 4 || month == 5) {
      return "Q2";
    } else if (month == 6 || month == 7 || month == 8) {
      return "Q3";
    } else if (month == 9 || month == 10 || month == 11) {
      return "Q4";
    } else {
      return "Invalid Quarter";
    }
  }

  public String getName() {
    return "QuarterPartitionResolver";
  }

  public Serializable getRoutingObject(EntryOperation opDetails) {
    Date date = (Date) opDetails.getKey();
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    int month = cal.get(Calendar.MONTH);
    return month;
  }

  public void close() {
    // TODO Auto-generated method stub

  }

  public void setnumBuckets(int numBukcets) {
    this.numBuckets = numBukcets;
  }

  public int getNumBuckets(String partitionName, String regionName,
      PartitionAttributes partitionAttributes) {
    return this.numBuckets;
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!obj.getClass().equals(this.getClass())) {
      return false;
    }
    QuarterPartitionResolver other = (QuarterPartitionResolver) obj;
    if (!this.resolveProps.equals(other.getConfig())) {
      return false;
    }

    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.internal.cache.xmlcache.Declarable2#getConfig()
   */
  public Properties getConfig() {
    return this.resolveProps;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.cache.Declarable#init(java.util.Properties)
   */
  public void init(Properties props) {
    this.resolveProps.putAll(props);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.resolveProps = DataSerializer.readProperties(in);
    this.numBuckets = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeProperties(this.resolveProps, out);
    out.writeInt(this.numBuckets);

  }

}
