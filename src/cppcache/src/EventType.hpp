/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
