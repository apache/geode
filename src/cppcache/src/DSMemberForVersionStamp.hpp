/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __DSMemberForVersionStamp_HPP__
#define __DSMemberForVersionStamp_HPP__

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/CacheableKey.hpp>
#include <string>

namespace gemfire {
class DSMemberForVersionStamp;
typedef SharedPtr<DSMemberForVersionStamp> DSMemberForVersionStampPtr;

class DSMemberForVersionStamp : public CacheableKey {
 public:
  virtual int16_t compareTo(DSMemberForVersionStampPtr tagID) = 0;

  virtual std::string getHashKey() = 0;

  /** return true if this key matches other. */
  virtual bool operator==(const CacheableKey& other) const = 0;

  /** return the hashcode for this key. */
  virtual uint32_t hashcode() const = 0;
};
}

#endif  // __DSMemberForVersionStamp_HPP__
