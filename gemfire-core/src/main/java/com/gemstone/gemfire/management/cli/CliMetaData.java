/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.cli;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.gemstone.gemfire.management.internal.cli.CliAroundInterceptor;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;

/**
 * An annotation to define additional meta-data for commands.
 *
 * @author Abhishek Chaudhari
 *
 * @since 7.0
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
  String[] relatedTopic() default com.gemstone.gemfire.management.internal.cli.i18n.CliStrings.DEFAULT_TOPIC_GEMFIRE;

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
//   * @author Abhishek Chaudhari
//   *
//   * @since 8.0
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
   * @author Abhishek Chaudhari
   *
   * @since 8.0
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
