/*
 *
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
 *
 */
package org.apache.geode.tools.pulse.tests;

/**
 * Region on members mbean
 *
 *
 */
public interface RegionOnMemberMBean {

  String getName();

  String getRegionType(); // Ideally should be an Enum

  String getFullPath();

  String getMember();

  long getEntrySize();

  long getEntryCount();

  double getGetsRate();

  double getPutsRate();

  double getDiskReadsRate();

  double getDiskWritesRate();

  int getLocalMaxMemory();
}
