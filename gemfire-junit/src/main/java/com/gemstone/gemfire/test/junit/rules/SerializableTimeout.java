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
package com.gemstone.gemfire.test.junit.rules;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

/**
 * Serializable version of Timeout JUnit Rule. JUnit lifecycle is not
 * executed in remote JVMs.
 * 
 * @author Kirk Lund
 */
@SuppressWarnings("serial")
public class SerializableTimeout extends Timeout implements SerializableTestRule {

  public static Builder builder() {
    return new Builder();
  }
  
  public SerializableTimeout(final long timeout, final TimeUnit timeUnit) {
    super(timeout, timeUnit);
  }
  
  protected SerializableTimeout(final Builder builder) {
    super(builder);
  }
  
  public static class Builder extends Timeout.Builder {
    
    protected Builder() {
      super();
    }
    
    @Override
    public SerializableTimeout build() {
      return new SerializableTimeout(this);
    }
  }

  private void writeObject(final ObjectOutputStream out) throws Exception {
    writeTimeout(out);
    writeTimeUnit(out);
    writeLookForStuckThread(out);
  }

  private void readObject(final ObjectInputStream in) throws Exception {
    readTimeout(in);
    readTimeUnit(in);
    readLookForStuckThread(in);
  }
  
  private void writeTimeout(final ObjectOutputStream out) throws Exception {
    final Field timeoutField = TestName.class.getDeclaredField("timeout");
    timeoutField.setAccessible(true);
    final Long timeoutValue = (Long) timeoutField.get(this);
    out.writeLong(timeoutValue);
  }
  
  private void writeTimeUnit(final ObjectOutputStream out) throws Exception {
    final Field timeoutField = TestName.class.getDeclaredField("timeUnit");
    timeoutField.setAccessible(true);
    final TimeUnit timeoutValue = (TimeUnit) timeoutField.get(this);
    out.writeObject(timeoutValue);
  }

  private void writeLookForStuckThread(final ObjectOutputStream out) throws Exception {
    try {
      final Field lookForStuckThreadField = TemporaryFolder.class.getDeclaredField("lookForStuckThread");
      lookForStuckThreadField.setAccessible(true);
      final Boolean lookForStuckThreadValue = (Boolean) lookForStuckThreadField.get(this);
      out.writeBoolean(lookForStuckThreadValue);
    } catch (NoSuchFieldException e) {
      out.writeBoolean(false);
    }
  }
  
  private void readTimeout(final ObjectInputStream in) throws Exception {
    Field timeoutField = TestName.class.getDeclaredField("timeout");
    timeoutField.setAccessible(true);
    timeoutField.set(this, (Long) in.readObject());
  }

  private void readTimeUnit(final ObjectInputStream in) throws Exception {
    Field timeUnitField = TestName.class.getDeclaredField("timeUnit");
    timeUnitField.setAccessible(true);
    timeUnitField.set(this, (TimeUnit) in.readObject());
  }

  private void readLookForStuckThread(final ObjectInputStream in) throws Exception {
    try {
      final Field lookForStuckThreadField = TemporaryFolder.class.getDeclaredField("lookForStuckThread");
      lookForStuckThreadField.setAccessible(true);
      lookForStuckThreadField.set(this, (Boolean) in.readObject());
    } catch (NoSuchFieldException e) {
      final boolean value = (Boolean) in.readObject();
    }
  }
}
