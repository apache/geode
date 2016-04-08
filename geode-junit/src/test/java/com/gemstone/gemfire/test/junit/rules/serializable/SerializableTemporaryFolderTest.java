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
import static org.assertj.core.api.Assertions.*;

import com.gemstone.gemfire.test.junit.categories.UnitTest;
import org.apache.commons.lang.SerializationUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;

/**
 * Unit tests for {@link SerializableTemporaryFolder}.
 */
@Category(UnitTest.class)
public class SerializableTemporaryFolderTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void hasTwoFields() throws Exception {
    Field[] fields = TemporaryFolder.class.getDeclaredFields();
    assertThat(fields.length).as("Fields: " + Arrays.asList(fields)).isEqualTo(2);
  }

  @Test
  public void fieldParentFolderShouldExist() throws Exception {
    Field field = TemporaryFolder.class.getDeclaredField(FIELD_PARENT_FOLDER);
    assertThat(field.getType()).isEqualTo(File.class);
  }

  @Test
  public void fieldFolderShouldExist() throws Exception {
    Field field = TemporaryFolder.class.getDeclaredField(FIELD_FOLDER);
    assertThat(field.getType()).isEqualTo(File.class);
  }

  @Test
  public void fieldsCanBeRead() throws Exception {
    File parentFolder = this.temporaryFolder.getRoot();

    SerializableTemporaryFolder instance = new SerializableTemporaryFolder(parentFolder);
    instance.create();

    assertThat(readField(TemporaryFolder.class, instance, FIELD_PARENT_FOLDER)).isEqualTo(parentFolder);
    assertThat(readField(TemporaryFolder.class, instance, FIELD_FOLDER)).isEqualTo(instance.getRoot());
  }

  @Test
  public void isSerializable() throws Exception {
    assertThat(SerializableTemporaryFolder.class).isInstanceOf(Serializable.class);
  }

  @Test
  public void canBeSerialized() throws Exception {
    File parentFolder = this.temporaryFolder.getRoot();

    SerializableTemporaryFolder instance = new SerializableTemporaryFolder(parentFolder);
    instance.create();

    SerializableTemporaryFolder cloned = (SerializableTemporaryFolder)SerializationUtils.clone(instance);

    assertThat(readField(TemporaryFolder.class, cloned, FIELD_PARENT_FOLDER)).isEqualTo(parentFolder);
    assertThat(readField(TemporaryFolder.class, cloned, FIELD_FOLDER)).isEqualTo(cloned.getRoot());
  }
}
