/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
* PdxType.cpp
*
*  Created on: Nov 3, 2011
*      Author: npatel
*/

#include "PdxType.hpp"
#include "PdxHelper.hpp"
#include "GemfireTypeIdsImpl.hpp"
#include "PdxFieldType.hpp"
#include "Utils.hpp"
#include "PdxTypeRegistry.hpp"
#include "PdxHelper.hpp"
#include <ace/OS.h>

namespace gemfire {

const char* PdxType::m_javaPdxClass = "org.apache.geode.pdx.internal.PdxType";

PdxType::~PdxType() {
  GF_SAFE_DELETE(m_pdxFieldTypes);
  GF_SAFE_DELETE_ARRAY(m_remoteToLocalFieldMap);
  GF_SAFE_DELETE_ARRAY(m_localToRemoteFieldMap);
  GF_SAFE_DELETE_ARRAY(m_className);
}

PdxType::PdxType() : Serializable() {
  // m_lockObj = NULLPTR;
  m_className = NULL;
  m_isLocal = false;
  m_numberOfVarLenFields = 0;
  m_varLenFieldIdx = 0;  // start with 0
  m_isVarLenFieldAdded = false;
  // m_fieldNameVsPdxType = CacheableHashMap::create();
  m_noJavaClass = false;
  // m_pdxDomainType = nullptr;
  m_pdxFieldTypes = new std::vector<PdxFieldTypePtr>();
  m_localToRemoteFieldMap = NULL;
  m_remoteToLocalFieldMap = NULL;
  m_gemfireTypeId = 0;
  /* adongre
   * Coverity - II
   * CID 29288: Uninitialized scalar field (UNINIT_CTOR)
   * Non-static class member "m_numberOfFieldsExtra" is not
   * initialized in this constructor nor in any functions that it calls.
   * Fix : Initialized the memeber
   */
  m_numberOfFieldsExtra = 0;
}

PdxType::PdxType(const char* pdxDomainClassName, bool isLocal)
    : Serializable() {
  // m_lockObj = NULLPTR;
  m_className = Utils::copyString(pdxDomainClassName);
  m_isLocal = isLocal;
  m_numberOfVarLenFields = 0;
  m_varLenFieldIdx = 0;  // start with 0
  m_isVarLenFieldAdded = false;
  // m_fieldNameVsPdxType = CacheableHashMap::create();
  m_noJavaClass = false;
  m_pdxFieldTypes = new std::vector<PdxFieldTypePtr>();
  m_localToRemoteFieldMap = NULL;
  m_remoteToLocalFieldMap = NULL;
  m_gemfireTypeId = 0;
  /* adongre
   * Coverity - II
   * CID 29287: Uninitialized scalar field (UNINIT_CTOR)
   * Non-static class member "m_numberOfFieldsExtra" is not
   * initialized in this constructor nor in any functions that it calls.
   * Fix : Initialized the memeber
   */
  m_numberOfFieldsExtra = 0;
}

void PdxType::toData(DataOutput& output) const {
  output.write(
      static_cast<int8_t>(GemfireTypeIdsImpl::DataSerializable));  // 45
  output.write(static_cast<int8_t>(GemfireTypeIdsImpl::Class));    // 43
  output.write(static_cast<int8_t>(GemfireTypeIds::CacheableASCIIString));
  output.writeUTF(m_javaPdxClass);

  // m_className
  output.write(static_cast<int8_t>(GemfireTypeIds::CacheableASCIIString));
  output.writeUTF(m_className);

  // m_noJavaClass
  output.writeBoolean(m_noJavaClass);

  // m_gemfireTypeId
  output.writeInt(m_gemfireTypeId);

  // m_varLenFieldIdx
  output.writeInt(m_varLenFieldIdx);

  output.writeArrayLen(static_cast<int32_t>(m_pdxFieldTypes->size()));

  for (std::vector<PdxFieldTypePtr>::iterator it = m_pdxFieldTypes->begin();
       it != m_pdxFieldTypes->end(); ++it) {
    PdxFieldTypePtr pdxPtr = *it;
    pdxPtr->toData(output);
  }
}

Serializable* PdxType::fromData(DataInput& input) {
  int8_t dsByte;
  input.read(&dsByte);

  int8_t classByte;
  input.read(&classByte);

  int8_t classtypeId;
  input.read(&classtypeId);
  input.readUTF(const_cast<char**>(&m_javaPdxClass));

  int8_t classtypeId2;
  input.read(&classtypeId2);
  input.readUTF(&m_className);

  input.readBoolean(&m_noJavaClass);

  input.readInt(&m_gemfireTypeId);

  input.readInt(&m_varLenFieldIdx);

  int len;
  input.readArrayLen(&len);

  bool foundVarLenType = false;

  for (int i = 0; i < len; i++) {
    PdxFieldTypePtr pft(new PdxFieldType());
    pft->fromData(input);

    m_pdxFieldTypes->push_back(pft);

    if (pft->IsVariableLengthType() == true) foundVarLenType = true;
  }

  // as m_varLenFieldIdx starts with 0
  if (m_varLenFieldIdx != 0) {
    m_numberOfVarLenFields = m_varLenFieldIdx + 1;
  } else if (foundVarLenType) {
    m_numberOfVarLenFields = 1;
  }

  InitializeType();
  return this;
}

void PdxType::addFixedLengthTypeField(const char* fieldName,
                                      const char* className, int8_t typeId,
                                      int32 size) {
  if (fieldName == NULL /*|| *fieldName == '\0'*/ ||
      m_fieldNameVsPdxType.find(fieldName) !=
          m_fieldNameVsPdxType
              .end()) {  // COVERITY ---> 30289 Same on both sides
    char excpStr[256] = {0};
    /* adongre
     * Coverity - II
    * CID 29269: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(
        excpStr, 256,
        "Field: %s is either already added into PdxWriter or it is null ",
        fieldName);
    throw IllegalStateException(excpStr);
  }
  PdxFieldTypePtr pfxPtr(new PdxFieldType(
      fieldName, className, typeId, static_cast<int32>(m_pdxFieldTypes->size()),
      false, size, 0));
  m_pdxFieldTypes->push_back(pfxPtr);
  m_fieldNameVsPdxType[fieldName] = pfxPtr;
}

void PdxType::addVariableLengthTypeField(const char* fieldName,
                                         const char* className, int8_t typeId) {
  if (fieldName == NULL /*|| *fieldName == '\0'*/ ||
      m_fieldNameVsPdxType.find(fieldName) !=
          m_fieldNameVsPdxType
              .end()) {  // COVERITY ---> 30289 Same on both sides
    char excpStr[256] = {0};
    ACE_OS::snprintf(
        excpStr, 256,
        "Field: %s is either already added into PdxWriter or it is null ",
        fieldName);
    throw IllegalStateException(excpStr);
  }

  if (m_isVarLenFieldAdded) {
    m_varLenFieldIdx++;  // it initial value is zero so variable length field
                         // idx start with zero
  }
  m_numberOfVarLenFields++;
  m_isVarLenFieldAdded = true;
  PdxFieldTypePtr pfxPtr(new PdxFieldType(
      fieldName, className, typeId, static_cast<int32>(m_pdxFieldTypes->size()),
      true, -1, m_varLenFieldIdx));
  m_pdxFieldTypes->push_back(pfxPtr);
  m_fieldNameVsPdxType[fieldName] = pfxPtr;
}

void PdxType::initRemoteToLocal() {
  // This method is to check if we have some extra fields in remote PdxType that
  // are absent in local PdxType.
  // 1. Get local PdxType from type registry
  // 2. Get list of PdxFieldTypes from it
  // 3. Use m_pdxFieldTypes for remote PdxType, that is populated in fromData
  // 4. Iterate over local and remote PdxFieldTypes to compare individual type
  // 5. If found update in m_remoteToLocalFieldMap that field there in remote
  // type. else update m_numberOfFieldsExtra
  // 6. else update in m_remoteToLocalFieldMap that local type don't have this
  // fields
  // 7. 1 = field there in remote type
  //  -1 = local type don't have this VariableLengthType field
  //  -2 = local type don't have this FixedLengthType field

  PdxTypePtr localPdxType = NULLPTR;
  //[TODO - open this up once PdxTypeRegistry is done]
  localPdxType = PdxTypeRegistry::getLocalPdxType(m_className);
  m_numberOfFieldsExtra = 0;

  if (localPdxType != NULLPTR) {
    std::vector<PdxFieldTypePtr>* localPdxFields =
        localPdxType->getPdxFieldTypes();
    int32 fieldIdx = 0;

    m_remoteToLocalFieldMap = new int32_t[m_pdxFieldTypes->size()];
    LOGDEBUG(
        "PdxType::initRemoteToLocal m_pdxFieldTypes->size() =%d AND "
        "localPdxFields->size()=%d",
        m_pdxFieldTypes->size(), localPdxFields->size());
    for (std::vector<PdxFieldTypePtr>::iterator remotePdxField =
             m_pdxFieldTypes->begin();
         remotePdxField != m_pdxFieldTypes->end(); ++remotePdxField) {
      bool found = false;

      for (std::vector<PdxFieldTypePtr>::iterator localPdxfield =
               localPdxFields->begin();
           localPdxfield != localPdxFields->end(); ++localPdxfield) {
        // PdxFieldType* remotePdx = (*(remotePdxField)).ptr();
        // PdxFieldType* localPdx = (*(localPdxfield)).ptr();
        if ((*localPdxfield)->equals(*remotePdxField)) {
          found = true;
          m_remoteToLocalFieldMap[fieldIdx++] = 1;  // field there in remote
                                                    // type
          break;
        }
      }

      if (!found) {
        // while writing take this from weakhashmap
        // local field is not in remote type
        // if((*(remotePdxField)).ptr()->IsVariableLengthType()) {
        if ((*remotePdxField)->IsVariableLengthType()) {
          m_remoteToLocalFieldMap[fieldIdx++] =
              -1;  // local type don't have this fields
        } else {
          m_remoteToLocalFieldMap[fieldIdx++] =
              -2;  // local type don't have this fields
        }
        m_numberOfFieldsExtra++;
      }
    }
  }
}

void PdxType::initLocalToRemote() {
  // This method is to check if we have some extra fields in remote PdxType that
  // are absent in local PdxType.
  // 1. Get local PdxType from type registry
  // 2. Get list of PdxFieldTypes from it
  // 3. Iterate over local PdxFields to compare it with remote PdxFields and if
  // they r equal that means there sequence is same mark it with -2
  // 4. Iterate over local PdxFields and remote PdxFields and if they are equal
  // then mark m_localToRemoteFieldMap with remotePdxField sequenceId
  // 5. else if local field is not in remote type then -1

  PdxTypePtr localPdxType = NULLPTR;
  localPdxType = PdxTypeRegistry::getLocalPdxType(m_className);

  if (localPdxType != NULLPTR) {
    std::vector<PdxFieldTypePtr>* localPdxFields =
        localPdxType->getPdxFieldTypes();

    int32_t fieldIdx = 0;
    // type which need to read/write should control local type
    int32_t localToRemoteFieldMapSize =
        static_cast<int32>(localPdxType->m_pdxFieldTypes->size());
    m_localToRemoteFieldMap = new int32_t[localToRemoteFieldMapSize];

    for (int32_t i = 0; i < localToRemoteFieldMapSize &&
                        i < static_cast<int32_t>(m_pdxFieldTypes->size());
         i++) {
      // PdxFieldType* localPdx = localPdxFields->at(fieldIdx).ptr();
      // PdxFieldType* remotePdx = m_pdxFieldTypes->at(i).ptr();
      if (localPdxFields->at(fieldIdx)->equals(m_pdxFieldTypes->at(i))) {
        // fields are in same order, we can read as it is
        m_localToRemoteFieldMap[fieldIdx++] = -2;
      } else {
        break;
      }
    }

    for (; fieldIdx < localToRemoteFieldMapSize;) {
      PdxFieldTypePtr localPdxField =
          localPdxType->m_pdxFieldTypes->at(fieldIdx);
      bool found = false;

      for (std::vector<PdxFieldTypePtr>::iterator remotePdxfield =
               m_pdxFieldTypes->begin();
           remotePdxfield != m_pdxFieldTypes->end(); ++remotePdxfield)
      // for each(PdxFieldType^ remotePdxfield in m_pdxFieldTypes)
      {
        // PdxFieldType* localPdx = localPdxField.ptr();
        PdxFieldType* remotePdx = (*(remotePdxfield)).ptr();
        if (localPdxField->equals(*remotePdxfield)) {
          found = true;
          // store pdxfield type position to get the offset quickly
          m_localToRemoteFieldMap[fieldIdx++] = remotePdx->getSequenceId();
          break;
        }
      }

      if (!found) {
        // local field is not in remote type
        m_localToRemoteFieldMap[fieldIdx++] = -1;
      }
    }
  }
}

void PdxType::InitializeType() {
  initRemoteToLocal();  // for writing
  initLocalToRemote();  // for reading
  generatePositionMap();
}

int32_t PdxType::getFieldPosition(const char* fieldName,
                                  uint8_t* offsetPosition, int32_t offsetSize,
                                  int32_t pdxStreamlen) {
  PdxFieldTypePtr pft = this->getPdxField(fieldName);
  if (pft != NULLPTR) {
    if (pft->IsVariableLengthType()) {
      return variableLengthFieldPosition(pft, offsetPosition, offsetSize,
                                         pdxStreamlen);
    } else {
      return fixedLengthFieldPosition(pft, offsetPosition, offsetSize,
                                      pdxStreamlen);
    }
  }
  return -1;
}

int32_t PdxType::getFieldPosition(int32_t fieldIdx, uint8_t* offsetPosition,
                                  int32_t offsetSize, int32_t pdxStreamlen) {
  PdxFieldTypePtr pft = m_pdxFieldTypes->at(fieldIdx);
  if (pft != NULLPTR) {
    if (pft->IsVariableLengthType()) {
      return variableLengthFieldPosition(pft, offsetPosition, offsetSize,
                                         pdxStreamlen);
    } else {
      return fixedLengthFieldPosition(pft, offsetPosition, offsetSize,
                                      pdxStreamlen);
    }
  }
  return -1;
}

int32_t PdxType::fixedLengthFieldPosition(PdxFieldTypePtr fixLenField,
                                          uint8_t* offsetPosition,
                                          int32_t offsetSize,
                                          int32_t pdxStreamlen) {
  int32_t offset = fixLenField->getVarLenOffsetIndex();
  if (fixLenField->getRelativeOffset() >= 0) {
    // starting fields
    return fixLenField->getRelativeOffset();
  } else if (offset == -1)  // Pdx length
  {
    // there is no var len field so just subtracts relative offset from behind
    return pdxStreamlen + fixLenField->getRelativeOffset();
  } else {
    // need to read offset and then subtract relative offset
    // TODO
    return PdxHelper::readInt(
               offsetPosition +
                   (m_numberOfVarLenFields - offset - 1) * offsetSize,
               offsetSize) +
           fixLenField->getRelativeOffset();
  }
}

int32_t PdxType::variableLengthFieldPosition(PdxFieldTypePtr varLenField,
                                             uint8_t* offsetPosition,
                                             int32_t offsetSize,
                                             int32_t pdxStreamlen) {
  int32_t offset = varLenField->getVarLenOffsetIndex();
  if (offset == -1) {
    return /*first var len field*/ varLenField->getRelativeOffset();
  } else {
    // we write offset from behind
    return PdxHelper::readInt(
        offsetPosition + (m_numberOfVarLenFields - offset - 1) * offsetSize,
        offsetSize);
  }
}

int32_t* PdxType::getLocalToRemoteMap() {
  if (m_localToRemoteFieldMap != NULL) {
    return m_localToRemoteFieldMap;
  }

  ReadGuard guard(m_lockObj);
  if (m_localToRemoteFieldMap != NULL) {
    return m_localToRemoteFieldMap;
  }
  initLocalToRemote();

  return m_localToRemoteFieldMap;
}

int32_t* PdxType::getRemoteToLocalMap() {
  if (m_remoteToLocalFieldMap != NULL) {
    return m_remoteToLocalFieldMap;
  }

  ReadGuard guard(m_lockObj);
  if (m_remoteToLocalFieldMap != NULL) {
    return m_remoteToLocalFieldMap;
  }
  initRemoteToLocal();

  return m_remoteToLocalFieldMap;
}

PdxTypePtr PdxType::isContains(PdxTypePtr first, PdxTypePtr second) {
  int j = 0;
  for (int i = 0; i < static_cast<int>(second->m_pdxFieldTypes->size()); i++) {
    PdxFieldTypePtr secondPdt = second->m_pdxFieldTypes->at(i);
    bool matched = false;
    for (; j < static_cast<int>(first->m_pdxFieldTypes->size()); j++) {
      PdxFieldTypePtr firstPdt = first->m_pdxFieldTypes->at(j);
      // PdxFieldType* firstType = firstPdt.ptr();
      // PdxFieldType* secondType = secondPdt.ptr();
      if (firstPdt->equals(secondPdt)) {
        matched = true;
        break;
      }
    }
    if (!matched) return NULLPTR;
  }
  return first;
}

PdxTypePtr PdxType::clone() {
  PdxTypePtr clone(new PdxType(m_className, false));
  clone->m_gemfireTypeId = 0;
  clone->m_numberOfVarLenFields = m_numberOfVarLenFields;

  for (std::vector<PdxFieldTypePtr>::iterator it = m_pdxFieldTypes->begin();
       it != m_pdxFieldTypes->end(); ++it) {
    PdxFieldTypePtr pdxPtr = *it;
    clone->m_pdxFieldTypes->push_back(pdxPtr);
  }
  return clone;
}

PdxTypePtr PdxType::isLocalTypeContains(PdxTypePtr otherType) {
  if (m_pdxFieldTypes->size() >= otherType->m_pdxFieldTypes->size()) {
    return isContains(PdxTypePtr(this), otherType);
  }
  return NULLPTR;
}

PdxTypePtr PdxType::isRemoteTypeContains(PdxTypePtr remoteType) {
  if (m_pdxFieldTypes->size() <= remoteType->m_pdxFieldTypes->size()) {
    return isContains(remoteType, PdxTypePtr(this));
  }
  return NULLPTR;
}

PdxTypePtr PdxType::mergeVersion(PdxTypePtr otherVersion) {
  // int nTotalFields = otherVersion->m_pdxFieldTypes->size();
  PdxTypePtr contains = NULLPTR;

  if (isLocalTypeContains(otherVersion) != NULLPTR) return PdxTypePtr(this);

  if (isRemoteTypeContains(otherVersion) != NULLPTR) return otherVersion;

  // need to create new one, clone of local
  PdxTypePtr newone = clone();
  int varLenFields = newone->getNumberOfVarLenFields();

  for (std::vector<PdxFieldTypePtr>::iterator it =
           otherVersion->m_pdxFieldTypes->begin();
       it != otherVersion->m_pdxFieldTypes->end(); ++it) {
    bool found = false;
    // for each(PdxFieldType^ tmpNew in newone->m_pdxFieldTypes)
    for (std::vector<PdxFieldTypePtr>::iterator it2 =
             newone->m_pdxFieldTypes->begin();
         it2 != newone->m_pdxFieldTypes->end(); ++it2) {
      if ((*it2)->equals(*it)) {
        found = true;
        break;
      }
    }
    if (!found) {
      PdxFieldTypePtr newFt(new PdxFieldType(
          (*it)->getFieldName(), (*it)->getClassName(), (*it)->getTypeId(),
          static_cast<int32>(newone->m_pdxFieldTypes->size()),  // sequence id
          (*it)->IsVariableLengthType(), (*it)->getFixedSize(),
          ((*it)->IsVariableLengthType()
               ? varLenFields++ /*it increase after that*/
               : 0)));
      newone->m_pdxFieldTypes->push_back(
          newFt);  // fieldnameVsPFT will happen after that
    }
  }

  newone->setNumberOfVarLenFields(varLenFields);
  if (varLenFields > 0) newone->setVarLenFieldIdx(varLenFields);

  // need to keep all versions in local version
  // m_otherVersions->Add(newone);
  return newone;
}

void PdxType::generatePositionMap() {
  bool foundVarLen = false;
  int lastVarLenSeqId = 0;
  int prevFixedSizeOffsets = 0;
  // set offsets from back first
  PdxFieldTypePtr previousField = NULLPTR;

  for (int i = static_cast<int>(m_pdxFieldTypes->size()) - 1; i >= 0; i--) {
    PdxFieldTypePtr tmpft = m_pdxFieldTypes->at(i);
    std::string temp = tmpft->getFieldName();
    std::pair<std::string, PdxFieldTypePtr> pc(temp, tmpft);
    m_fieldNameVsPdxType.insert(pc);

    if (tmpft->IsVariableLengthType()) {
      tmpft->setVarLenOffsetIndex(tmpft->getVarLenFieldIdx());
      tmpft->setRelativeOffset(0);
      foundVarLen = true;
      lastVarLenSeqId = tmpft->getVarLenFieldIdx();
    } else {
      if (foundVarLen) {
        tmpft->setVarLenOffsetIndex(lastVarLenSeqId);
        // relative offset is subtracted from var len offsets
        tmpft->setRelativeOffset(-tmpft->getFixedSize() +
                                 previousField->getRelativeOffset());
      } else {
        tmpft->setVarLenOffsetIndex(-1);  // Pdx header length
        // relative offset is subtracted from var len offsets
        tmpft->setRelativeOffset(-tmpft->getFixedSize());
        if (previousField != NULLPTR) {  // boundary condition
          tmpft->setRelativeOffset(-tmpft->getFixedSize() +
                                   previousField->getRelativeOffset());
        }
      }
    }

    previousField = tmpft;
  }

  foundVarLen = false;
  prevFixedSizeOffsets = 0;
  // now do optimization till you don't fine var len
  for (uint32 i = 0; (i < m_pdxFieldTypes->size()) && !foundVarLen; i++) {
    PdxFieldTypePtr tmpft = m_pdxFieldTypes->at(i);

    if (tmpft->IsVariableLengthType()) {
      tmpft->setVarLenOffsetIndex(-1);  // first var len field
      tmpft->setRelativeOffset(prevFixedSizeOffsets);
      foundVarLen = true;
    } else {
      tmpft->setVarLenOffsetIndex(0);  // no need to read offset
      tmpft->setRelativeOffset(prevFixedSizeOffsets);
      prevFixedSizeOffsets += tmpft->getFixedSize();
    }
  }
}

bool PdxType::Equals(PdxTypePtr otherObj) {
  if (otherObj == NULLPTR) return false;

  PdxType* ot = dynamic_cast<PdxType*>(otherObj.ptr());

  if (ot == NULL) return false;

  if (ot == this) return true;

  if (ot->m_pdxFieldTypes->size() != m_pdxFieldTypes->size()) return false;

  for (uint32_t i = 0; i < m_pdxFieldTypes->size(); i++) {
    if (!ot->m_pdxFieldTypes->at(i)->equals(m_pdxFieldTypes->at(i))) {
      return false;
    }
  }
  return true;
}

bool PdxType::operator<(const PdxType& other) const {
  return ACE_OS::strcmp(this->m_className, other.m_className) < 0;
}
}  // namespace gemfire
