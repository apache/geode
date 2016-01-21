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

import org.junit.rules.TestName;

/**
 * Serializable version of TestName JUnit Rule. JUnit lifecycle is not
 * executed in remote JVMs.
 * 
 * @author Kirk Lund
 */
@SuppressWarnings("serial")
public class SerializableTestName extends TestName implements SerializableTestRule {

  private void writeObject(final ObjectOutputStream out) throws Exception {
    writeName(out);
  }

  private void readObject(final ObjectInputStream in) throws Exception {
    readName(in);
  }
  
  private void writeName(final ObjectOutputStream out) throws Exception {
    final Field nameField = TestName.class.getDeclaredField("name");
    nameField.setAccessible(true);
    final String nameValue = (String) nameField.get(this);
    out.writeObject(nameValue);
  }
  
  private void readName(final ObjectInputStream in) throws Exception {
    Field nameField = TestName.class.getDeclaredField("name");
    nameField.setAccessible(true);
    nameField.set(this, (String) in.readObject());
  }
}
