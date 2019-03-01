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

package org.apache.geode.javac;

import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.MirroredTypeException;
import javax.tools.Diagnostic;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@SupportedAnnotationTypes("org.junit.runner.RunWith")
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class EnsureCorrectRunsWithProcessor extends AbstractProcessor {

  private static final String FACTORY_CANONICAL_NAME = "org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory";
  private static final String FACTORY_SIMPLE_NAME = "CategoryWithParameterizedRunnerFactory";

  private static final String RUNWITH = RunWith.class.getCanonicalName();

  private Messager messager;

  @Override
  public synchronized void init(ProcessingEnvironment env) {
    super.init(env);
    messager = env.getMessager();
  }

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
    boolean hasErrors = false;

    for (Element annotatedElement : roundEnv.getElementsAnnotatedWith(RunWith.class)) {
      if (annotatedElement.getKind() != ElementKind.CLASS) {
        continue;
      }

      TypeElement typeElement = (TypeElement) annotatedElement;

      boolean hasUseParameterizedRunnerFactory = false;
      boolean hasRunWithParameterized = false;

      for (AnnotationMirror am : typeElement.getAnnotationMirrors()) {
        String clazz = am.getAnnotationType().toString();
        if (clazz.equals(RunWith.class.getCanonicalName())) {
          hasRunWithParameterized = isRunWithParameterized(typeElement);
        }

        if (clazz.equals(Parameterized.UseParametersRunnerFactory.class.getCanonicalName())) {
          hasUseParameterizedRunnerFactory = isUseParameterizedRunnerFactory(typeElement);
        }
      }

      if (hasRunWithParameterized && !hasUseParameterizedRunnerFactory) {
        error(typeElement, "class is annotated with @RunWith(Parameterized.class) but is missing the annotation @Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)");
        hasErrors = true;
      }
    }

    return hasErrors;
  }

  private boolean isRunWithParameterized(TypeElement typeElement) {
    RunWith runWith = typeElement.getAnnotation(RunWith.class);

    String runWithValue;
    try {
      runWithValue = runWith.value().getSimpleName();
    } catch (MirroredTypeException mex) {
      DeclaredType classTypeMirror = (DeclaredType) mex.getTypeMirror();
      TypeElement classTypeElement = (TypeElement) classTypeMirror.asElement();
      runWithValue = classTypeElement.getSimpleName().toString();
    }

    if (runWithValue == null) {
      return false;
    }

    return runWithValue.equals(Parameterized.class.getCanonicalName())
        || runWithValue.equals(Parameterized.class.getSimpleName());
  }

  private boolean isUseParameterizedRunnerFactory(TypeElement typeElement) {
    Parameterized.UseParametersRunnerFactory
        runnerFactory = typeElement.getAnnotation(Parameterized.UseParametersRunnerFactory.class);

    String runnerFactoryValue;
    try {
      runnerFactoryValue = runnerFactory.value().getSimpleName();
    } catch (MirroredTypeException mex) {
      DeclaredType classTypeMirror = (DeclaredType) mex.getTypeMirror();
      TypeElement classTypeElement = (TypeElement) classTypeMirror.asElement();
      runnerFactoryValue = classTypeElement.getSimpleName().toString();
    }

    if (runnerFactoryValue == null) {
      return false;
    }

    return runnerFactoryValue.equals(FACTORY_CANONICAL_NAME)
        || runnerFactoryValue.equals(FACTORY_SIMPLE_NAME);
  }

  private void error(Element e, String msg, Object... args) {
    messager.printMessage(
        Diagnostic.Kind.ERROR,
        String.format(msg, args),
        e);
  }
}
