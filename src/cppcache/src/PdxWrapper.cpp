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
/*
 * PdxWrapper.cpp
 *
 *  Created on: Apr 17, 2012
 *      Author: vrao
 */

#include <gfcpp/PdxWrapper.hpp>
#include <Utils.hpp>
#include <PdxHelper.hpp>
#include <SerializationRegistry.hpp>

namespace apache {
namespace geode {
namespace client {

PdxWrapper::PdxWrapper(void *userObject, const char *className) {
  m_userObject = userObject;

  if (className != NULL) {
    m_className = Utils::copyString(className);
  } else {
    LOGERROR("Class name not provided to PdxWrapper constructor");
    throw IllegalArgumentException(
        "Class name not provided to PdxWrapper constructor");
  }

  m_serializer = SerializationRegistry::getPdxSerializer();

  if (m_serializer == NULLPTR) {
    LOGERROR("No registered PDX serializer found for PdxWrapper");
    throw IllegalArgumentException(
        "No registered PDX serializer found for PdxWrapper");
  }

  m_deallocator = m_serializer->getDeallocator(className);

  if (m_deallocator == NULL) {
    LOGERROR(
        "No deallocator function found from PDX serializer for PdxWrapper for "
        "%s",
        className);
    throw IllegalArgumentException(
        "No deallocator function found from PDX serializer for PdxWrapper");
  }

  /* m_sizer can be NULL - required only if heap LRU is enabled */
  m_sizer = m_serializer->getObjectSizer(className);
}

PdxWrapper::PdxWrapper(const char *className) {
  if (className != NULL) {
    m_className = Utils::copyString(className);
  } else {
    LOGERROR("Class name not provided to PdxWrapper for deserialization");
    throw IllegalArgumentException(
        "Class name not provided to PdxWrapper for deserialization");
  }

  m_serializer = SerializationRegistry::getPdxSerializer();

  if (m_serializer == NULLPTR) {
    LOGERROR(
        "No registered PDX serializer found for PdxWrapper deserialization");
    throw IllegalArgumentException(
        "No registered PDX serializer found for PdxWrapper deserialization");
  }

  m_deallocator = m_serializer->getDeallocator(className);

  if (m_deallocator == NULL) {
    LOGERROR(
        "No deallocator function found from PDX serializer for PdxWrapper "
        "deserialization for %s",
        className);
    throw IllegalArgumentException(
        "No deallocator function found from PDX serializer for PdxWrapper "
        "deserialization");
  }

  /* m_sizer can be NULL - required only if heap LRU is enabled */
  m_sizer = m_serializer->getObjectSizer(className);

  /* adongre   - Coverity II
   * CID 29277: Uninitialized pointer field (UNINIT_CTOR)
   */
  m_userObject = (void *)0;
}

void *PdxWrapper::getObject(bool detach) {
  void *retVal = m_userObject;
  if (detach) {
    m_userObject = NULL;
  }
  return retVal;
}

const char *PdxWrapper::getClassName() const {
  return (const char *)m_className;
}

bool PdxWrapper::operator==(const CacheableKey &other) const {
  PdxWrapper *wrapper =
      dynamic_cast<PdxWrapper *>(const_cast<CacheableKey *>(&other));
  if (wrapper == NULL) {
    return false;
  }
  return (intptr_t)m_userObject == (intptr_t)wrapper->m_userObject;
}

uint32_t PdxWrapper::hashcode() const {
  uint64_t hash = static_cast<uint64_t>((intptr_t)m_userObject);
  return apache::geode::client::serializer::hashcode(hash);
}

void PdxWrapper::toData(PdxWriterPtr output) {
  if (m_userObject != NULL) {
    m_serializer->toData(m_userObject, (const char *)m_className, output);
  } else {
    LOGERROR("User object is NULL or detached in PdxWrapper toData");
    throw IllegalStateException(
        "User object is NULL or detached in PdxWrapper toData");
  }
}

void PdxWrapper::fromData(PdxReaderPtr input) {
  m_userObject = m_serializer->fromData((const char *)m_className, input);
}

void PdxWrapper::toData(DataOutput &output) const {
  PdxHelper::serializePdx(output, *this);
}

Serializable *PdxWrapper::fromData(DataInput &input) {
  LOGERROR("PdxWrapper fromData should not have been called");
  throw IllegalStateException(
      "PdxWrapper fromData should not have been called");
}

uint32_t PdxWrapper::objectSize() const {
  if (m_sizer == NULL || m_userObject == NULL) {
    return 0;
  } else {
    return m_sizer(m_userObject, (const char *)m_className);
  }
}

CacheableStringPtr PdxWrapper::toString() const {
  std::string message = "PdxWrapper for ";
  message += m_className;
  return CacheableString::create(message.c_str());
}

PdxWrapper::~PdxWrapper() {
  if (m_userObject != NULL) {
    m_deallocator(m_userObject, (const char *)m_className);
  }
  delete[] m_className;
}
}  // namespace client
}  // namespace geode
}  // namespace apache
