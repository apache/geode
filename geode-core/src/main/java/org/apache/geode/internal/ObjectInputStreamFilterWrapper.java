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
package org.apache.geode.internal;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URL;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import sun.misc.ObjectInputFilter;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.distributed.internal.DistributedSystemService;
import org.apache.geode.internal.logging.LogService;


public class ObjectInputStreamFilterWrapper implements InputStreamFilter {
  private static final Logger logger = LogService.getLogger();
  private final ObjectInputFilter serializationFilter;

  public ObjectInputStreamFilterWrapper(String serializationFilterSpec,
      Collection<DistributedSystemService> services) {

    Set<String> sanctionedClasses = new HashSet<>(500);
    for (DistributedSystemService service : services) {
      try {
        Collection<String> classNames = service.getSerializationWhitelist();
        logger.info("loaded {} sanctioned serializables from {}", classNames.size(),
            service.getClass().getSimpleName());
        sanctionedClasses.addAll(classNames);
      } catch (IOException e) {
        throw new InternalGemFireException("error initializing serialization filter for " + service,
            e);
      }
    }

    try {
      URL sanctionedSerializables = ClassPathLoader.getLatest()
          .getResource(InternalDataSerializer.class, "sanctioned-geode-core-serializables.txt");
      Collection<String> coreClassNames =
          InternalDataSerializer.loadClassNames(sanctionedSerializables);
      sanctionedClasses.addAll(coreClassNames);
    } catch (IOException e) {
      throw new InternalGemFireException(
          "unable to read sanctionedSerializables.txt to form a serialization white-list", e);
    }

    logger.info("setting a serialization filter containing {}", serializationFilterSpec);

    final ObjectInputFilter userFilter =
        ObjectInputFilter.Config.createFilter(serializationFilterSpec);
    serializationFilter = filterInfo -> {
      if (filterInfo.serialClass() == null) {
        return userFilter.checkInput(filterInfo);
      }

      String className = filterInfo.serialClass().getName();
      if (filterInfo.serialClass().isArray()) {
        className = filterInfo.serialClass().getComponentType().getName();
      }
      if (sanctionedClasses.contains(className)) {
        return ObjectInputFilter.Status.ALLOWED;
        // return ObjectInputFilter.Status.UNDECIDED;
      } else {
        ObjectInputFilter.Status status = userFilter.checkInput(filterInfo);
        if (status == ObjectInputFilter.Status.REJECTED) {
          logger.fatal("Serialization filter is rejecting class {}", className, new Exception(""));
        }
        return status;
      }
    };

  }

  @Override
  public void setFilterOn(ObjectInputStream objectInputStream) {
    ObjectInputFilter.Config.setObjectInputFilter(objectInputStream, serializationFilter);
  }
}
