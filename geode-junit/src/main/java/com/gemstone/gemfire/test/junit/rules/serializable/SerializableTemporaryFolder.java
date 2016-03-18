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
package com.gemstone.gemfire.test.junit.rules.serializable;

import static com.gemstone.gemfire.test.junit.rules.serializable.FieldSerializationUtils.*;
import static com.gemstone.gemfire.test.junit.rules.serializable.FieldsOfTemporaryFolder.*;

import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;

/**
 * Serializable subclass of {@link org.junit.rules.TemporaryFolder TemporaryFolder}.
 * Instance variables of TemporaryFolder are serialized by reflection.
 */
public class SerializableTemporaryFolder extends TemporaryFolder implements SerializableTestRule {

  public SerializableTemporaryFolder() {
    super();
  }

  public SerializableTemporaryFolder(final File parentFolder) {
    super(parentFolder);
  }

  private void readObject(final ObjectInputStream stream) throws InvalidObjectException {
    throw new InvalidObjectException("SerializationProxy required");
  }

  private Object writeReplace() {
    return new SerializationProxy(this);
  }

  /**
   * Serialization proxy for {@code SerializableTemporaryFolder}.
   */
  private static class SerializationProxy implements Serializable {

    private final File parentFolder;
    private final File folder;

    SerializationProxy(final SerializableTemporaryFolder instance) {
      this.parentFolder = (File) readField(TemporaryFolder.class, instance, FIELD_PARENT_FOLDER);
      this.folder =(File) readField(TemporaryFolder.class, instance, FIELD_FOLDER);
    }

    private Object readResolve() {
      SerializableTemporaryFolder instance = new SerializableTemporaryFolder(this.parentFolder);
      writeField(TemporaryFolder.class, instance, FIELD_FOLDER, this.folder);
      return instance;
    }
  }
}
