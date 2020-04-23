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
import static org.apache.geode.test.junit.rules.serializable.FieldSerializationUtils.writeField;
import static org.apache.geode.test.junit.rules.serializable.FieldsOfTemporaryFolder.FIELD_FOLDER;
import static org.apache.geode.test.junit.rules.serializable.FieldsOfTemporaryFolder.FIELD_PARENT_FOLDER;

import java.io.File;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

/**
 * Serializable subclass of {@link TemporaryFolder TemporaryFolder}. Instance
 * variables of TemporaryFolder are serialized by reflection.
 */
@SuppressWarnings("WeakerAccess")
public class SerializableTemporaryFolder extends TemporaryFolder implements SerializableTestRule {
  private static final Logger logger = LogManager.getLogger();

  private final AtomicBoolean passed = new AtomicBoolean(true);
  private final AtomicBoolean delete = new AtomicBoolean(true);
  private final AtomicReference<File> copyTo = new AtomicReference<>();
  private final AtomicReference<When> when = new AtomicReference<>(When.FAILS);
  private final AtomicReference<String> methodName = new AtomicReference<>();

  /**
   * Specifies conditions under which copyTo is performed
   */
  public enum When {
    /** Perform {@code copyTo} only if test fails */
    FAILS(passed -> !passed),
    /** Perform {@code copyTo} only if test passes */
    PASSES(passed -> passed),
    /** Perform {@code copyTo} regardless if test fails or passes */
    ALWAYS(passed -> true);

    private final Predicate<Boolean> performCopyTo;

    When(Predicate<Boolean> performCopyTo) {
      this.performCopyTo = performCopyTo;
    }

    boolean test(boolean passed) {
      return performCopyTo.test(passed);
    }
  }

  public SerializableTemporaryFolder() {
    this(null);
  }

  public SerializableTemporaryFolder(File parentFolder) {
    super(parentFolder);
  }

  /**
   * Specifying false will prevent deletion of the temporary folder and its contents. Default is
   * true.
   */
  public SerializableTemporaryFolder delete(boolean value) {
    delete.set(value);
    return this;
  }

  /**
   * Specifies directory to copy artifacts to before deleting temporary folder. Default is null.
   */
  public SerializableTemporaryFolder copyTo(File directory) {
    copyTo.set(directory);
    return this;
  }

  /**
   * Specifies conditions under which {@code copyTo} is performed. Default is {@code FAIL}.
   */
  public SerializableTemporaryFolder when(When when) {
    this.when.set(when);
    return this;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return statement(base, description);
  }

  protected Statement statement(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        List<Throwable> errors = new ArrayList<>();

        before(description);
        try {
          base.evaluate();
        } catch (Throwable e) {
          errors.add(e);
          passed.set(false);
        } finally {
          after();
        }

        MultipleFailureException.assertEmpty(errors);
      }
    };
  }

  protected void before(Description description) throws Throwable {
    methodName.set(description.getMethodName());
    passed.set(true);
    before();
    logger.info("SerializableTemporaryFolder root: {}", getRoot().getAbsolutePath());
  }

  @Override
  protected void after() {
    File directory = copyTo.get();
    if (directory != null && when.get().test(passed.get())) {
      File timestamp = new File(directory, String.valueOf(System.currentTimeMillis()));
      File destination = new File(timestamp, methodName.get());
      destination.mkdirs();

      copyTo(getRoot(), destination);
    }
    if (delete.get()) {
      super.after();
    }
  }

  private void copyTo(File source, File destination) {
    if (destination == null) {
      return;
    }
    try {
      FileUtils.copyDirectory(source, destination, true);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void readObject(ObjectInputStream stream) throws InvalidObjectException {
    throw new InvalidObjectException("SerializationProxy required");
  }

  protected Object writeReplace() {
    return new SerializationProxy(this);
  }

  /**
   * Serialization proxy for {@code SerializableTemporaryFolder}.
   */
  @SuppressWarnings("serial")
  private static class SerializationProxy implements Serializable {

    private final File parentFolder;
    private final File folder;

    private SerializationProxy(SerializableTemporaryFolder instance) {
      parentFolder = (File) readField(TemporaryFolder.class, instance, FIELD_PARENT_FOLDER);
      folder = (File) readField(TemporaryFolder.class, instance, FIELD_FOLDER);
    }

    protected Object readResolve() {
      SerializableTemporaryFolder instance = new SerializableTemporaryFolder(parentFolder);
      writeField(TemporaryFolder.class, instance, FIELD_FOLDER, folder);
      return instance;
    }
  }
}
