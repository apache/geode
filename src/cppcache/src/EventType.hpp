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
#ifndef _EVENTTYPE_H__
#define _EVENTTYPE_H__

namespace gemfire {
enum EntryEventType {
  BEFORE_CREATE = 0,
  BEFORE_UPDATE,
  BEFORE_INVALIDATE,
  BEFORE_DESTROY,
  AFTER_CREATE,
  AFTER_UPDATE,
  AFTER_INVALIDATE,
  AFTER_DESTROY
};

enum RegionEventType {
  BEFORE_REGION_INVALIDATE = 0,
  BEFORE_REGION_DESTROY,
  AFTER_REGION_INVALIDATE,
  AFTER_REGION_DESTROY,
  BEFORE_REGION_CLEAR,
  AFTER_REGION_CLEAR
};
}

#endif  // _EVENTTYPE_H__
