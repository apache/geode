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

package org.apache.geode.management.internal.configuration.converters;

import static org.apache.geode.cache.DiskStoreFactory.DEFAULT_DISK_STORE_NAME;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.PdxType;
import org.apache.geode.management.configuration.AutoSerializer;
import org.apache.geode.management.configuration.ClassName;
import org.apache.geode.management.configuration.Pdx;

public class PdxConverter extends ConfigurationConverter<Pdx, PdxType> {
  private final ClassNameConverter converter = new ClassNameConverter();
  private final AutoSerializerConverter autoSerializerConverter = new AutoSerializerConverter();

  @Override
  protected Pdx fromNonNullXmlObject(PdxType xmlObject) {
    Pdx pdx = new Pdx();

    pdx.setReadSerialized(xmlObject.isReadSerialized());
    if (xmlObject.isPersistent()) {
      String diskStoreName = xmlObject.getDiskStoreName();
      if (StringUtils.isBlank(diskStoreName)) {
        diskStoreName = DEFAULT_DISK_STORE_NAME;
      }
      pdx.setDiskStoreName(diskStoreName);
    }
    pdx.setIgnoreUnreadFields(xmlObject.isIgnoreUnreadFields());
    AutoSerializer autoSerializer =
        autoSerializerConverter.fromXmlObject(xmlObject.getPdxSerializer());
    if (autoSerializer == null) {
      ClassName className = converter.fromXmlObject(xmlObject.getPdxSerializer());
      pdx.setPdxSerializer(className);
    } else {
      pdx.setAutoSerializer(autoSerializer);
    }
    return pdx;
  }

  @Override
  protected PdxType fromNonNullConfigObject(Pdx configObject) {
    PdxType xmlType = new PdxType();
    xmlType.setReadSerialized(configObject.isReadSerialized());
    xmlType.setDiskStoreName(configObject.getDiskStoreName());
    xmlType.setIgnoreUnreadFields(configObject.isIgnoreUnreadFields());
    xmlType.setPersistent(configObject.getDiskStoreName() != null);
    DeclarableType pdxSerializer =
        autoSerializerConverter.fromConfigObject(configObject.getAutoSerializer());
    if (pdxSerializer == null) {
      pdxSerializer = converter.fromConfigObject(configObject.getPdxSerializer());
    }
    xmlType.setPdxSerializer(pdxSerializer);
    return xmlType;
  }
}
