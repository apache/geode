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
package org.apache.geode.test.junit.rules.serializable;

import static org.apache.geode.test.junit.rules.serializable.FieldSerializationUtils.readField;
import static org.apache.geode.test.junit.rules.serializable.FieldsOfTemporaryFolder.FIELD_ASSURE_DELETION;
import static org.apache.geode.test.junit.rules.serializable.FieldsOfTemporaryFolder.FIELD_FOLDER;
import static org.apache.geode.test.junit.rules.serializable.FieldsOfTemporaryFolder.FIELD_PARENT_FOLDER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.rules.TemporaryFolder.Builder;
import static org.junit.rules.TemporaryFolder.builder;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link SerializableTemporaryFolder}.
 */
public class SerializableTemporaryFolderTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void hasThreeFields() {
    Field[] fields = TemporaryFolder.class.getDeclaredFields();
    List<Field> nonStaticFields =
        Arrays.stream(fields).filter(f -> !Modifier.isStatic(f.getModifiers())).collect(
            Collectors.toList());
    assertThat(nonStaticFields).hasSize(3);
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
  public void fieldAssureDeletionShouldExist() throws Exception {
    Field field = TemporaryFolder.class.getDeclaredField(FIELD_ASSURE_DELETION);
    assertThat(field.getType()).isEqualTo(boolean.class);
  }

  @Test
  public void fieldsCanBeRead() throws Exception {
    File parentFolder = temporaryFolder.getRoot();

    SerializableTemporaryFolder instance = new SerializableTemporaryFolder(parentFolder);
    instance.create();

    assertThat(readField(TemporaryFolder.class, instance, FIELD_PARENT_FOLDER))
        .isEqualTo(parentFolder);
    assertThat(readField(TemporaryFolder.class, instance, FIELD_FOLDER))
        .isEqualTo(instance.getRoot());
    assertThat(readField(TemporaryFolder.class, instance, FIELD_ASSURE_DELETION)).isEqualTo(false);
  }

  @Test
  public void isSerializable() {
    assertThat(SerializableTemporaryFolder.class).isInstanceOf(Serializable.class);
  }

  @Test
  public void canBeSerialized() throws Exception {
    Builder instanceBuilder = builder();
    File parentFolder = temporaryFolder.getRoot();
    instanceBuilder.parentFolder(parentFolder);
    instanceBuilder.assureDeletion();

    SerializableTemporaryFolder instance = new SerializableTemporaryFolder(instanceBuilder);
    instance.create();

    SerializableTemporaryFolder cloned = SerializationUtils.clone(instance);

    assertThat(readField(TemporaryFolder.class, cloned, FIELD_PARENT_FOLDER))
        .isEqualTo(parentFolder);
    assertThat(readField(TemporaryFolder.class, cloned, FIELD_FOLDER)).isEqualTo(cloned.getRoot());
    assertThat(readField(TemporaryFolder.class, cloned, FIELD_ASSURE_DELETION)).isEqualTo(true);
  }
}
