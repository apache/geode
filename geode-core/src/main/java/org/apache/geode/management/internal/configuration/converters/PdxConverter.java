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

import org.apache.geode.cache.configuration.PdxType;
import org.apache.geode.management.configuration.Pdx;

public class PdxConverter extends ConfigurationConverter<Pdx, PdxType> {
  private final ClassNameConverter converter = new ClassNameConverter();

  @Override
  protected Pdx fromNonNullXmlObject(PdxType xmlObject) {
    Pdx pdx = new Pdx();

    pdx.setReadSerialized(xmlObject.isReadSerialized());
    pdx.setDiskStoreName(xmlObject.getDiskStoreName());
    pdx.setIgnoreUnreadFields(xmlObject.isIgnoreUnreadFields());
    pdx.setPdxSerializer(converter.fromXmlObject(xmlObject.getPdxSerializer()));
    return pdx;
  }

  @Override
  protected PdxType fromNonNullConfigObject(Pdx configObject) {
    PdxType xmlType = new PdxType();
    xmlType.setReadSerialized(configObject.isReadSerialized());
    xmlType.setDiskStoreName(configObject.getDiskStoreName());
    xmlType.setIgnoreUnreadFields(configObject.isIgnoreUnreadFields());
    xmlType.setPersistent(configObject.getDiskStoreName()!=null);
    xmlType.setPdxSerializer(converter.fromConfigObject(configObject.getPdxSerializer()));
    return xmlType;
  }
}
