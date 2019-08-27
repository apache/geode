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

package org.apache.geode.pdx.internal;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;

class TypeRegistrationStatistics {
  static final String TYPE_DEFINED = "typeDefined";
  static final String TYPE_CREATED = "typeCreated";
  static final String ENUM_DEFINED = "enumDefined";
  static final String ENUM_CREATED = "enumCreated";
  static final String SIZE = "size";

  private final TypeRegistration typeRegistration;
  private final int typeDefinedId;
  private final int typeCreatedId;
  private final int enumDefinedId;
  private final int enumCreatedId;
  private final Statistics statistics;

  TypeRegistrationStatistics(final StatisticsFactory statisticsFactory,
      final TypeRegistration typeRegistration) {
    this.typeRegistration = typeRegistration;

    final StatisticsType statisticsType =
        statisticsFactory.createType("PdxTypeRegistration", "PDX type registration statistics.",
            new StatisticDescriptor[] {
                statisticsFactory.createLongCounter(TYPE_DEFINED, "Number of PDX types defined.",
                    "ops"),
                statisticsFactory.createLongCounter(TYPE_CREATED, "Number of PDX types created.",
                    "ops"),
                statisticsFactory.createLongCounter(ENUM_DEFINED, "Number of PDX enums defined.",
                    "ops"),
                statisticsFactory.createLongCounter(ENUM_CREATED, "Number of PDX enums created.",
                    "ops"),
                statisticsFactory.createLongGauge(SIZE, "Size of PDX type and enum registry.",
                    "entries")
            });

    typeDefinedId = statisticsType.nameToId(TYPE_DEFINED);
    typeCreatedId = statisticsType.nameToId(TYPE_CREATED);
    enumDefinedId = statisticsType.nameToId(ENUM_DEFINED);
    enumCreatedId = statisticsType.nameToId(ENUM_CREATED);

    statistics = statisticsFactory.createAtomicStatistics(statisticsType,
        typeRegistration.getClass().getSimpleName());
  }

  public void initialize() {
    statistics.setLongSupplier(SIZE, typeRegistration::getLocalSize);
  }

  void typeDefined() {
    statistics.incLong(typeDefinedId, 1);
  }

  void typeCreated() {
    statistics.incLong(typeCreatedId, 1);
  }

  void enumDefined() {
    statistics.incLong(enumDefinedId, 1);
  }

  void enumCreated() {
    statistics.incLong(enumCreatedId, 1);
  }

}
