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
package com.gemstone.gemfire.management.cli;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.gemstone.gemfire.management.internal.cli.CliAroundInterceptor;

/**
 * An annotation to define additional meta-data for commands.
 *
 *
 * @since GemFire 7.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.PARAMETER })
public @interface CliMetaData {

  /** Represents a default value to an option of a command. **/
  public static final String ANNOTATION_DEFAULT_VALUE = "__DEFAULT__";

  /** Represents a null value to an option of a command. **/
  public static final String ANNOTATION_NULL_VALUE = "__NULL__";

  /**
   * Indicates that the command will only run in the gfsh shell and will not
   * need the management service
   **/
  boolean shellOnly() default false;

  /** Indicates that the effect of the command is persisted or the commands affects the persistent configuration */
  boolean isPersisted() default false;
  
  boolean readsSharedConfiguration() default false;
  
  boolean writesToSharedConfiguration() default false;
  
  /** In help, topics that are related to this command **/
  String[] relatedTopic() default com.gemstone.gemfire.management.internal.cli.i18n.CliStrings.DEFAULT_TOPIC_GEODE;

  /**
   * The fully qualified name of a class which implements the
   * {@link CliAroundInterceptor} interface in order to provide additional pre-
   * and post-execution functionality for a command.
   */
  String interceptor() default com.gemstone.gemfire.management.cli.CliMetaData.ANNOTATION_NULL_VALUE;

  /**
   * String used as a separator when multiple values for a command are specified
   */
  String valueSeparator() default com.gemstone.gemfire.management.cli.CliMetaData.ANNOTATION_NULL_VALUE;


  // TODO - Abhishek - refactor to group this
//  /**
//   *
//   * @since GemFire 8.0
//   */
//  @Retention(RetentionPolicy.RUNTIME)
//  @Target({ ElementType.PARAMETER })
//  public @interface ParameterMetadata {
//    /**
//     * String used as a separator when multiple values for a command are specified
//     */
//    String valueSeparator() default CliMetaData.ANNOTATION_NULL_VALUE;
//  }

  /**
   * An annotation to define additional meta-data for availability of commands.
   * 
   *
   * @since GemFire 8.0
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.METHOD })
  public @interface AvailabilityMetadata {
    /**
     * String describing the availability condition.
     */
    String availabilityDescription() default com.gemstone.gemfire.management.cli.CliMetaData.ANNOTATION_NULL_VALUE;
  }

}
