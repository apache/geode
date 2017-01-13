/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _LOCATOR_LIST_RESPONSE_
#define _LOCATOR_LIST_RESPONSE_
#include <vector>
#include "GemfireTypeIdsImpl.hpp"
#include "ServerLocationResponse.hpp"
#include "ServerLocation.hpp"
namespace gemfire {
class DataInput;
class LocatorListResponse : public ServerLocationResponse {
 private:
  std::vector<ServerLocation> m_locators;
  bool m_isBalanced;
  void readList(DataInput& input);

 public:
  LocatorListResponse()
      :                         /* adonre
                                 * CID 28938: Uninitialized scalar field (UNINIT_CTOR) *
                                */
        m_isBalanced(false) {}  // Default constru needed for de-serialization
  virtual LocatorListResponse* fromData(DataInput& input);
  virtual int8_t typeId() const;
  virtual uint32_t objectSize() const;
  const std::vector<ServerLocation>& getLocators() const;
  bool isBalanced() const;
  virtual ~LocatorListResponse() {}  // Virtual destructor
  static Serializable* create();
};
typedef SharedPtr<LocatorListResponse> LocatorListResponsePtr;
}
#endif
