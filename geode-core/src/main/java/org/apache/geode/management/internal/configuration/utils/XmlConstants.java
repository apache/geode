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

package org.apache.geode.management.internal.configuration.utils;

import javax.xml.XMLConstants;

/**
 *
 * @since GemFire 8.1
 */
public final class XmlConstants {

  /**
   * Standard prefix for {@link XMLConstants#W3C_XML_SCHEMA_INSTANCE_NS_URI}
   * (http://www.w3.org/2001/XMLSchema-instance) namespace.
   * 
   * @since GemFire 8.1
   */
  public static final String W3C_XML_SCHEMA_INSTANCE_PREFIX = "xsi";

  /**
   * Schema location attribute local name, "schemaLocation", in
   * {@link XMLConstants#W3C_XML_SCHEMA_INSTANCE_NS_URI}
   * (http://www.w3.org/2001/XMLSchema-instance) namespace.
   * 
   * @since GemFire 8.1
   */
  public static final String W3C_XML_SCHEMA_INSTANCE_ATTRIBUTE_SCHEMA_LOCATION = "schemaLocation";

  /**
   * Default prefix. Effectively no prefix.
   * 
   * @since GemFire 8.1
   */
  public static final String DEFAULT_PREFIX = "";

  private XmlConstants() {
    // statics only
  }
}
