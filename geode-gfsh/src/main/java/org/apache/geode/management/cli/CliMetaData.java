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
package org.apache.geode.management.cli;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.geode.management.internal.cli.CliAroundInterceptor;
import org.apache.geode.management.internal.i18n.CliStrings;

/**
 * An annotation to define additional meta-data for commands.
 *
 *
 * @since GemFire 7.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.PARAMETER})
public @interface CliMetaData {

  /** Represents a default value to an option of a command. **/
  String ANNOTATION_DEFAULT_VALUE = "__DEFAULT__";

  /** Represents a null value to an option of a command. **/
  String ANNOTATION_NULL_VALUE = "__NULL__";

  /**
   * Indicates that the command will only run in the gfsh shell Gfsh ExecutionStrategy will use this
   * flag to determine whether to invoke remote call or not.
   **/
  boolean shellOnly() default false;

  /**
   * Indicates when executed over http, is this command downloading files from the member. When this
   * is set to true, the RestHttpOperationInvoker will use an extractor to extract the inputstream
   * in the response to a temporary file and it's up to your command's interceptor's postExecution
   * to use that temp file to fit your need.
   **/
  boolean isFileDownloadOverHttp() default false;

  /**
   * Indicates whether this command would require fileData to be sent from the client. If this is
   * true, the preExecution of the interceptor needs to return a FileResult
   */
  boolean isFileUploaded() default false;

  /**
   * Indicates that the effect of the command is persisted or the commands affects the persistent
   * configuration
   */
  boolean isPersisted() default false;

  /** In help, topics that are related to this command **/
  String[] relatedTopic() default CliStrings.DEFAULT_TOPIC_GEODE;

  /**
   * The fully qualified name of a class which implements the {@link CliAroundInterceptor} interface
   * in order to provide additional pre- and post-execution functionality for a command.
   */
  String interceptor() default org.apache.geode.management.cli.CliMetaData.ANNOTATION_NULL_VALUE;

  /**
   * String used as a separator when multiple values for a command are specified
   *
   * @deprecated since 1.2, Command methods may override both the delimiter and the escape through
   *             spring shell's {@code splittingRegex} option context
   */
  String valueSeparator() default org.apache.geode.management.cli.CliMetaData.ANNOTATION_NULL_VALUE;

  /**
   * An annotation to define additional meta-data for availability of commands.
   *
   *
   * @since GemFire 8.0
   *
   * @deprecated since Geode1.2, not used at all
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD})
  @interface AvailabilityMetadata {
    /**
     * String describing the availability condition.
     */
    String availabilityDescription() default org.apache.geode.management.cli.CliMetaData.ANNOTATION_NULL_VALUE;
  }

}
