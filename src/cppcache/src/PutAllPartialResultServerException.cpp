/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "PutAllPartialResultServerException.hpp"

namespace gemfire {

PutAllPartialResultServerException::PutAllPartialResultServerException(
    PutAllPartialResultPtr result) {
  LOGDEBUG("Partial keys are processed in putAll");
  m_result = result;
}

PutAllPartialResultServerException::PutAllPartialResultServerException() {
  LOGDEBUG("Partial keys are processed in putAll");
  ACE_Recursive_Thread_Mutex responseLock;
  m_result = new PutAllPartialResult(-1, responseLock);
}

void PutAllPartialResultServerException::consolidate(
    PutAllPartialResultServerExceptionPtr pre) {
  m_result->consolidate(pre->getResult());
}

void PutAllPartialResultServerException::consolidate(
    PutAllPartialResultPtr otherResult) {
  m_result->consolidate(otherResult);
}

PutAllPartialResultPtr PutAllPartialResultServerException::getResult() {
  return m_result;
}

VersionedCacheableObjectPartListPtr
PutAllPartialResultServerException::getSucceededKeysAndVersions() {
  return m_result->getSucceededKeysAndVersions();
}

ExceptionPtr PutAllPartialResultServerException::getFailure() {
  return m_result->getFailure();
}

bool PutAllPartialResultServerException::hasFailure() {
  return m_result->hasFailure();
}

CacheableKeyPtr PutAllPartialResultServerException::getFirstFailedKey() {
  return m_result->getFirstFailedKey();
}

CacheableStringPtr PutAllPartialResultServerException::getMessage() {
  return m_result->toString();
}

void PutAllPartialResultServerException::toData(DataOutput& output) const {
  throw IllegalStateException(
      "PutAllPartialResultServerException::toData is not intended for use.");
}

Serializable* PutAllPartialResultServerException::fromData(DataInput& input) {
  throw IllegalStateException(
      "PutAllPartialResultServerException::fromData is not intended for use.");
  return NULL;
}

int32_t PutAllPartialResultServerException::classId() const {
  throw IllegalStateException(
      "PutAllPartialResultServerException::classId is not intended for use.");
  return 0;
}

uint32_t PutAllPartialResultServerException::objectSize() const {
  throw IllegalStateException(
      "PutAllPartialResultServerException::objectSize is not intended for "
      "use.");
  return 0;
}

int8_t PutAllPartialResultServerException::typeId() const {
  return static_cast<int8_t>(0);
}

PutAllPartialResultServerException::PutAllPartialResultServerException(
    CacheableStringPtr msg)
    : m_message(msg) {}
}  // namespace gemfire
