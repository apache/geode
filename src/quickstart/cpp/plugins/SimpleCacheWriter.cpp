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

#include "SimpleCacheWriter.hpp"

bool SimpleCacheWriter::beforeUpdate(const EntryEvent& event) {
  LOGINFO("SimpleCacheWriter: Got a beforeUpdate event.");
  return true;
}

bool SimpleCacheWriter::beforeCreate(const EntryEvent& event) {
  LOGINFO("SimpleCacheWriter: Got a beforeCreate event.");
  return true;
}

void SimpleCacheWriter::beforeInvalidate(const EntryEvent& event) {
  LOGINFO("SimpleCacheWriter: Got a beforeInvalidate event.");
}

bool SimpleCacheWriter::beforeDestroy(const EntryEvent& event) {
  LOGINFO("SimpleCacheWriter: Got a beforeDestroy event.");
  return true;
}

void SimpleCacheWriter::beforeRegionInvalidate(const RegionEvent& event) {
  LOGINFO("SimpleCacheWriter: Got a beforeRegionInvalidate event.");
}

bool SimpleCacheWriter::beforeRegionDestroy(const RegionEvent& event) {
  LOGINFO("SimpleCacheWriter: Got a beforeRegionDestroy event.");
  return true;
}

void SimpleCacheWriter::close(const RegionPtr& region) {
  LOGINFO("SimpleCacheWriter: Got a close event.");
}
