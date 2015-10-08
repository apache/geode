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
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.SerializedComparator;

public abstract class ComparisonTestCase extends TestCase {
  public enum Comparison {
    EQ(0.0),
    LT(-1.0),
    GT(1.0);
    
    private final double sgn;
    
    private Comparison(double sgn) {
      this.sgn = sgn;
    }
    
    public double signum() {
      return sgn;
    }
    
    public Comparison opposite() {
      switch (this) {
      case LT: return GT;
      case GT : return LT;
      default : return EQ;
      }
    }
  }
  
  public void compare(SerializedComparator sc, Object o1, Object o2, Comparison result) throws IOException {
    double diff = sc.compare(convert(o1), convert(o2));
    assertEquals(String.format("%s ? %s", o1, o2), result.signum(), Math.signum(diff));
  }
  
  public void compareAsIs(SerializedComparator sc, byte[] b1, byte[] b2, Comparison result) throws IOException {
    double diff = sc.compare(b1, b2);
    assertEquals(result.signum(), Math.signum(diff));
  }
  
  public static byte[] convert(Object o) throws IOException {
    ByteArrayOutputStream b = new ByteArrayOutputStream();
    DataOutputStream d = new DataOutputStream(b);
    DataSerializer.writeObject(o, d);
    
    return b.toByteArray();
  }
  
  public static Object recover(byte[] obj, int off, int len) throws ClassNotFoundException, IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(obj, off, len));
    return DataSerializer.readObject(in);
  }
}
