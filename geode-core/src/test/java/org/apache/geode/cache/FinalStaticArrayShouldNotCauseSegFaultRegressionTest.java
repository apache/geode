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
package org.apache.geode.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;


/**
 * Asserts fixes for bug JDK-8076152 in JDK 1.8.0u20 to 1.8.0.u45.
 * http://bugs.java.com/bugdatabase/view_bug.do?bug_id=8076152
 *
 * The JVM crashes when hotspot compiling a method that uses an array consisting of objects of a
 * base class when different child classes is used as actual instance objects AND when the array is
 * constant (declared final). The crash occurs during process of the aaload byte code.
 *
 * This test and its corrections can be removed after the release of JDK 1.8.0u60 if we choose to
 * not support 1.8.0u20 - 1.8.0u45 inclusive.
 *
 * <p>
 * TRAC #52289: HotSpot SIGSEGV in C2 CompilerThread1 (LoadNode::Value(PhaseTransform*) const+0x202)
 * with JDK 1.8.0_45
 *
 * @since GemFire 8.2
 */
public class FinalStaticArrayShouldNotCauseSegFaultRegressionTest {

  @Test
  public void finalStaticArrayShouldNotCauseSegFault() throws Exception {
    // Iterate enough to cause JIT to compile javax.print.attribute.EnumSyntax::readResolve
    for (int i = 0; i < 100_000; i++) {
      // Must execute two or more subclasses with static final arrays of different types.
      doEvictionAlgorithm();
      doEvictionAction();
    }
  }

  private void doEvictionAlgorithm() throws IOException, ClassNotFoundException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(EvictionAlgorithm.NONE);
    }

    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
      ois.readObject();
    }
  }

  private void doEvictionAction() throws IOException, ClassNotFoundException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(EvictionAction.NONE);
    }

    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
      ois.readObject();
    }
  }
}
