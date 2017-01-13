/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/gfcpp_globals.hpp>

#include "CacheableToken.hpp"

#include <gfcpp/DataInput.hpp>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/CacheableString.hpp>
#include "GemfireTypeIdsImpl.hpp"

using namespace gemfire;

//---- statics

CacheableTokenPtr* CacheableToken::invalidToken = 0;
CacheableTokenPtr* CacheableToken::destroyedToken = 0;
CacheableTokenPtr* CacheableToken::overflowedToken = 0;
CacheableTokenPtr* CacheableToken::tombstoneToken = 0;

void CacheableToken::init() {
  if (CacheableToken::invalidToken == 0) {
    CacheableToken::invalidToken = new CacheableTokenPtr();
    *CacheableToken::invalidToken = new CacheableToken(CacheableToken::INVALID);
    CacheableToken::destroyedToken = new CacheableTokenPtr();
    *CacheableToken::destroyedToken =
        new CacheableToken(CacheableToken::DESTROYED);
    CacheableToken::overflowedToken = new CacheableTokenPtr();
    *CacheableToken::overflowedToken =
        new CacheableToken(CacheableToken::OVERFLOWED);
    CacheableToken::tombstoneToken = new CacheableTokenPtr();
    *CacheableToken::tombstoneToken =
        new CacheableToken(CacheableToken::TOMBSTONE);
  }
}

//----- serialization

Serializable* CacheableToken::createDeserializable() {
  return new CacheableToken();
}

void CacheableToken::toData(DataOutput& output) const {
  output.writeInt(static_cast<int32_t>(m_value));
}

Serializable* CacheableToken::fromData(DataInput& input) {
  input.readInt(reinterpret_cast<int32_t*>(&m_value));
  switch (m_value) {
    case INVALID:
      return invalidToken->ptr();
    case DESTROYED:
      return destroyedToken->ptr();
    case OVERFLOWED:
      return overflowedToken->ptr();
    case TOMBSTONE:
      return tombstoneToken->ptr();

    default:
      GF_D_ASSERT(false);
      // we really can't be returning new instances all the time..
      // because we wish to test tokens with pointer identity.
      return this;
  }
}

int32_t CacheableToken::classId() const { return 0; }

int8_t CacheableToken::typeId() const {
  return static_cast<int8_t>(GemfireTypeIdsImpl::CacheableToken);
}

//------ ctor

CacheableToken::CacheableToken() : m_value(CacheableToken::NOT_USED) {}

CacheableToken::CacheableToken(TokenType value) : m_value(value) {}

//------- dtor

CacheableToken::~CacheableToken() {}

/**
 * Display this object as 'string', which depend on the implementation in
 * the subclasses
 * The default implementation renders the classname.
 */
CacheableStringPtr CacheableToken::toString() const {
  static const char* ctstrings[] = {
      "CacheableToken::NOT_USED", "CacheableToken::INVALID",
      "CacheableToken::DESTROYED", "CacheableToken::OVERFLOWED",
      "CacheableToken::TOMBSTONE"};

  return CacheableString::create(ctstrings[m_value]);
}

uint32_t CacheableToken::objectSize() const { return sizeof(m_value); }
