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
package org.apache.geode.annotations;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.AnnotatedElement;

import org.junit.Test;

import org.apache.geode.annotations.experimentalpackage.ClassInExperimentalPackage;
import org.apache.geode.experimental.nonexperimentalpackage.ClassInNonExperimentalPackage;

/**
 * Unit tests for the <tt>Experimental</tt> annotation. Verifies that the annotation can be applied
 * to Interfaces, Classes, Public and Protected Fields, Enums, Enum Constants, Public and Protected
 * Methods, Packages, and Constructors.
 */
public class ExperimentalJUnitTest {

  private static final String FIELD_NAME = "field";
  private static final String METHOD_NAME = "method";

  @Test
  public void shouldIdentifyExperimentalInterface() throws Exception {
    assertThat(isExperimental(RegularInterface.class)).isFalse();
    assertThat(isExperimental(ExperimentalInterface.class)).isTrue();
  }

  @Test
  public void shouldIdentifyExperimentalClass() throws Exception {
    assertThat(isExperimental(RegularClass.class)).isFalse();
    assertThat(isExperimental(ExperimentalClass.class)).isTrue();
  }

  @Test
  public void shouldIdentifyExperimentalPublicField() throws Exception {
    assertThat(isExperimental(RegularPublicField.class.getField(FIELD_NAME))).isFalse();
    assertThat(isExperimental(ExperimentalPublicField.class.getField(FIELD_NAME))).isTrue();
  }

  @Test
  public void shouldIdentifyExperimentalProtectedField() throws Exception {
    assertThat(isExperimental(RegularProtectedField.class.getDeclaredField(FIELD_NAME))).isFalse();
    assertThat(isExperimental(ExperimentalProtectedField.class.getDeclaredField(FIELD_NAME)))
        .isTrue();
  }

  @Test
  public void shouldIdentifyExperimentalEnum() throws Exception {
    assertThat(isExperimental(RegularEnum.class)).isFalse();
    assertThat(isExperimental(ExperimentalEnum.class)).isTrue();
  }

  @Test
  public void shouldIdentifyExperimentalEnumConstant() throws Exception {
    assertThat(isExperimental(RegularEnumInstance.class.getField(RegularEnumInstance.THREE.name())))
        .isFalse();
    assertThat(isExperimental(
        ExperimentalEnumInstance.class.getField(ExperimentalEnumInstance.THREE.name()))).isTrue();
  }

  @Test
  public void shouldIdentifyExperimentalPublicMethod() throws Exception {
    assertThat(isExperimental(RegularPublicMethod.class.getMethod(METHOD_NAME))).isFalse();
    assertThat(isExperimental(ExperimentalPublicMethod.class.getMethod(METHOD_NAME))).isTrue();
  }

  @Test
  public void shouldIdentifyExperimentalProtectedMethod() throws Exception {
    assertThat(isExperimental(RegularProtectedMethod.class.getDeclaredMethod(METHOD_NAME)))
        .isFalse();
    assertThat(isExperimental(ExperimentalProtectedMethod.class.getDeclaredMethod(METHOD_NAME)))
        .isTrue();
  }

  @Test
  public void shouldIdentifyExperimentalPackage() throws Exception {
    assertThat(isExperimental(ClassInNonExperimentalPackage.class.getPackage())).isFalse();
    assertThat(isExperimental(ClassInExperimentalPackage.class.getPackage())).isTrue();
  }

  @Test
  public void shouldIdentifyExperimentalPublicConstructor() throws Exception {
    assertThat(isExperimental(RegularPublicConstructor.class.getConstructor())).isFalse();
    assertThat(isExperimental(ExperimentalPublicConstructor.class.getConstructor())).isTrue();
  }

  @Test
  public void shouldIdentifyExperimentalProtectedConstructor() throws Exception {
    assertThat(isExperimental(RegularProtectedConstructor.class.getConstructor())).isFalse();
    assertThat(isExperimental(ExperimentalProtectedConstructor.class.getConstructor())).isTrue();
  }

  private static boolean isExperimental(final AnnotatedElement element) {
    return element.getAnnotation(Experimental.class) != null;
  }

  public interface RegularInterface {
  }
  @Experimental("This is an experimental interface")
  public interface ExperimentalInterface {
  }

  public static class RegularClass {
  }
  @Experimental("This is an experimental class")
  public static class ExperimentalClass {
  }

  public static class RegularPublicField {
    public final boolean field = false;
  }
  public static class ExperimentalPublicField {
    @Experimental("This is an experimental public field")
    public final boolean field = false;
  }

  public static class RegularProtectedField {
    protected final boolean field = false;
  }
  public static class ExperimentalProtectedField {
    @Experimental("This is an experimental protected field")
    protected final boolean field = false;
  }

  public enum RegularEnum {
    ONE, TWO, THREE
  }
  @Experimental("This is an experimental enum")
  public enum ExperimentalEnum {
    ONE, TWO, THREE
  }

  public enum RegularEnumInstance {
    ONE, TWO, THREE
  }
  public enum ExperimentalEnumInstance {
    ONE, TWO, @Experimental("This is an experimental enum constant")
    THREE
  }

  public static class RegularPublicMethod {
    public void method() {}
  }
  public static class ExperimentalPublicMethod {
    @Experimental("This is an experimental public method")
    public void method() {}
  }

  public static class RegularProtectedMethod {
    public void method() {}
  }
  public static class ExperimentalProtectedMethod {
    @Experimental("This is an experimental protected method")
    protected void method() {}
  }

  public static class RegularPublicConstructor {
    public RegularPublicConstructor() {}
  }
  public static class ExperimentalPublicConstructor {
    @Experimental("This is an experimental public constructor")
    public ExperimentalPublicConstructor() {}
  }

  public static class RegularProtectedConstructor {
    public RegularProtectedConstructor() {}
  }
  public static class ExperimentalProtectedConstructor {
    @Experimental("This is an experimental protected constructor")
    public ExperimentalProtectedConstructor() {}
  }
}
