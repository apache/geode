/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.cli;

import org.springframework.shell.core.annotation.CliOption;

/**
 * Used in {@link CliOption} annotations to indicate which converter(s) should
 * or should not be used.
 *
 * @author David Hoots
 * @since 8.0
 */
public interface ConverterHint {
  public static final String DIRS                  = "converter.hint.dirs";
  public static final String DIR_PATHSTRING        = "converter.hint.dir.path.string";
  public static final String DISKSTORE_ALL         = "converter.hint.cluster.diskstore";
  public static final String FILE                  = "converter.hint.file";
  public static final String FILE_PATHSTRING       = "converter.hint.file.path.string";
  public static final String HINTTOPIC             = "converter.hint.gfsh.hint.topic";
  public static final String MEMBERGROUP           = "converter.hint.member.groups";
  /** Hint to be used for all types of GemFire cluster members  */
  public static final String ALL_MEMBER_IDNAME     = "converter.hint.all.member.idOrName";
  /** Hint to be used for all non locator GemFire cluster members  */
  public static final String MEMBERIDNAME          = "converter.hint.member.idOrName";
  /** Hint to be used for GemFire stand-alone locator members  */
  public static final String LOCATOR_MEMBER_IDNAME = "converter.hint.locatormember.idOrName";
  /** Hint to be used for configured locators for discovery */
  public static final String LOCATOR_DISCOVERY_CONFIG = "converter.hint.locators.discovery.config";
  public static final String REGIONPATH            = "converter.hint.region.path";
  public static final String INDEX_TYPE            = "converter.hint.index.type";
  public static final String STRING_LIST           = "converter.hint.list.string";
  public static final String GATEWAY_SENDER_ID     = "converter.hint.gateway.senderid";
  public static final String GATEWAY_RECEIVER_ID   = "converter.hint.gateway.receiverid";
  public static final String LOG_LEVEL             = "converter.hint.log.levels";

  public static final String STRING_DISABLER       = "converter.hint.disable-string-converter";
}
