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
package org.apache.geode.management.cli;

import org.springframework.shell.core.annotation.CliOption;

/**
 * Used in {@link CliOption} annotations to indicate which converter(s) should
 * or should not be used.
 *
 * @since GemFire 8.0
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
