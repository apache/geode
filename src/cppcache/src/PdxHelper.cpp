/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PdxHelper.cpp
 *
 *  Created on: Dec 13, 2011
 *      Author: npatel
 */

#include "PdxHelper.hpp"
#include "PdxTypeRegistry.hpp"
#include "PdxWriterWithTypeCollector.hpp"
#include "SerializationRegistry.hpp"
#include "PdxLocalReader.hpp"
#include "PdxRemoteReader.hpp"
#include "PdxType.hpp"
#include "PdxReaderWithTypeCollector.hpp"
#include "PdxInstanceImpl.hpp"
#include "Utils.hpp"
#include "PdxRemoteWriter.hpp"
#include "CacheRegionHelper.hpp"
#include <gfcpp/Cache.hpp>

namespace gemfire {
uint8_t PdxHelper::PdxHeader = 8;

PdxHelper::PdxHelper() {}

PdxHelper::~PdxHelper() {}

CacheImpl* PdxHelper::getCacheImpl() {
  CachePtr cache = CacheFactory::getAnyInstance();
  if (cache == NULLPTR) {
    throw IllegalStateException("cache has not been created yet.");
    ;
  }
  if (cache->isClosed()) {
    throw IllegalStateException("cache has been closed. ");
  }
  return CacheRegionHelper::getCacheImpl(cache.ptr());
}

void PdxHelper::serializePdx(DataOutput& output,
                             const PdxSerializable& pdxObject) {
  const char* pdxClassname = NULL;
  // bool isPdxWrapper = false;

  PdxSerializable& pdx = const_cast<PdxSerializable&>(pdxObject);
  PdxInstanceImpl* pdxII = dynamic_cast<PdxInstanceImpl*>(&pdx /*.ptr()*/);

  if (pdxII != NULL) {
    PdxTypePtr piPt = pdxII->getPdxType();
    if (piPt != NULLPTR &&
        piPt->getTypeId() ==
            0)  // from pdxInstance factory need to get typeid from server
    {
      int typeId = PdxTypeRegistry::getPDXIdForType(piPt, output.getPoolName());
      pdxII->setPdxId(typeId);
    }
    PdxLocalWriterPtr plw(new PdxLocalWriter(output, piPt));
    pdxII->toData(plw);
    plw->endObjectWriting();  // now write typeid
    int len = 0;
    uint8_t* pdxStream = plw->getPdxStream(len);
    pdxII->updatePdxStream(pdxStream, len);

    /*
    ==18904== 265,458 bytes in 2 blocks are definitely lost in loss record 928
    of 932
    ==18904==    at 0x400791A: operator new[](unsigned int)
    (vg_replace_malloc.c:378)
    ==18904==    by 0x4324354: gemfire::PdxLocalWriter::getPdxStream(int&)
    (DataOutput.hpp:744)
    ==18904==    by 0x4303E6F:
    gemfire::PdxHelper::serializePdx(gemfire::DataOutput&,
    gemfire::PdxSerializable const&) (PdxHelper.cpp:64)
    ==18904==    by 0x4377028:
    gemfire::TcrMessage::writeObjectPart(gemfire::SharedPtr<gemfire::Serializable>
    const&, bool, bool, gemfire::VectorOfCacheableKey const*)
    (DataOutput.hpp:580)
    ==18904==    by 0x437BB9E:
    gemfire::TcrMessage::TcrMessage(gemfire::TcrMessage::MsgType,
    gemfire::Region const*, gemfire::SharedPtr<gemfire::CacheableKey> const&,
    gemfire::SharedPtr<gemfire::Serializable> const&,
    gemfire::SharedPtr<gemfire::Serializable> const&, bool,
    gemfire::ThinClientBaseDM*, bool, bool, char const*) (TcrMessage.cpp:2216)
    ==18904==    by 0x43B9EE5:
    gemfire::ThinClientRegion::putNoThrow_remote(gemfire::SharedPtr<gemfire::CacheableKey>
    const&, gemfire::SharedPtr<gemfire::Serializable> const&,
    gemfire::SharedPtr<gemfire::Serializable> const&,
    gemfire::SharedPtr<gemfire::VersionTag>&, bool) (ThinClientRegion.cpp:909)
    ==18904==    by 0x42EFCEF: GfErrType
    gemfire::LocalRegion::updateNoThrow<gemfire::PutActions>(gemfire::SharedPtr<gemfire::CacheableKey>
    const&, gemfire::SharedPtr<gemfire::Serializable> const&,
    gemfire::SharedPtr<gemfire::Serializable> const&,
    gemfire::SharedPtr<gemfire::Serializable>&, int, gemfire::CacheEventFlags,
    gemfire::SharedPtr<gemfire::VersionTag>, gemfire::DataInput*,
    gemfire::SharedPtr<gemfire::EventId>) (LocalRegion.cpp:1097)
    ==18904==    by 0x42E4DC6:
    gemfire::LocalRegion::putNoThrow(gemfire::SharedPtr<gemfire::CacheableKey>
    const&, gemfire::SharedPtr<gemfire::Serializable> const&,
    gemfire::SharedPtr<gemfire::Serializable> const&,
    gemfire::SharedPtr<gemfire::Serializable>&, int, gemfire::CacheEventFlags,
    gemfire::SharedPtr<gemfire::VersionTag>, gemfire::DataInput*,
    gemfire::SharedPtr<gemfire::EventId>) (LocalRegion.cpp:1799)
    ==18904==    by 0x42DBEB4:
    gemfire::LocalRegion::put(gemfire::SharedPtr<gemfire::CacheableKey> const&,
    gemfire::SharedPtr<gemfire::Serializable> const&,
    gemfire::SharedPtr<gemfire::Serializable> const&) (LocalRegion.cpp:342)
    ==18904==    by 0x8069744: void
    gemfire::Region::put<gemfire::SharedPtr<gemfire::WritablePdxInstance>
    >(gemfire::SharedPtr<gemfire::CacheableKey> const&,
    gemfire::SharedPtr<gemfire::WritablePdxInstance> const&,
    gemfire::SharedPtr<gemfire::Serializable> const&) (in
    /export/pnq-gst-dev01a/users/adongre/cedar_dev_Nov12/build-artifacts/linux/tests/cppcache/testThinClientPdxInstance)
    ==18904==    by 0x808D107: Task_modifyPdxInstance::doTask() (in
    /export/pnq-gst-dev01a/users/adongre/cedar_dev_Nov12/build-artifacts/linux/tests/cppcache/testThinClientPdxInstance)
    ==18904==    by 0x80986CA: dunit::TestSlave::begin() (in
    /export/pnq-gst-dev01a/users/adongre/cedar_dev_Nov12/build-artifacts/linux/tests/cppcache/testThinClientPdxInstance)
    ==18904==
    */
    delete[] pdxStream;

    return;
  }

  const char* pdxType = pdx.getClassName();
  pdxClassname = pdxType;
  PdxTypePtr localPdxType = PdxTypeRegistry::getLocalPdxType(pdxType);

  if (localPdxType == NULLPTR) {
    // need to grab type info, as fromdata is not called yet

    PdxWriterWithTypeCollectorPtr ptc(
        new PdxWriterWithTypeCollector(output, pdxType));
    PdxWriterPtr pwp(dynCast<PdxWriterPtr>(ptc));
    pdx.toData(pwp);
    PdxTypePtr nType = ptc->getPdxLocalType();

    nType->InitializeType();

    // int32 nTypeId =(int32)
    // SerializationRegistry::GetPDXIdForType(output.getPoolName(), nType);
    int32 nTypeId = PdxTypeRegistry::getPDXIdForType(
        pdxType, output.getPoolName(), nType, true);
    nType->setTypeId(nTypeId);

    ptc->endObjectWriting();
    PdxTypeRegistry::addLocalPdxType(pdxType, nType);
    PdxTypeRegistry::addPdxType(nTypeId, nType);

    //[ToDo] need to write bytes for stats
    CacheImpl* cacheImpl = PdxHelper::getCacheImpl();
    if (cacheImpl != NULL) {
      uint8_t* stPos = const_cast<uint8_t*>(output.getBuffer()) +
                       ptc->getStartPositionOffset();
      int pdxLen = PdxHelper::readInt32(stPos);
      cacheImpl->m_cacheStats->incPdxSerialization(
          pdxLen + 1 + 2 * 4);  // pdxLen + 93 DSID + len + typeID
    }

  } else  // we know locasl type, need to see preerved data
  {
    // if object got from server than create instance of RemoteWriter otherwise
    // local writer.

    PdxSerializablePtr pdxObjptr = NULLPTR;
    pdxObjptr = PdxSerializablePtr(&pdx);

    PdxRemotePreservedDataPtr pd = PdxTypeRegistry::getPreserveData(pdxObjptr);

    // now always remotewriter as we have API Read/WriteUnreadFields
    // so we don't know whether user has used those or not;; Can we do some
    // trick here?
    PdxRemoteWriterPtr prw = NULLPTR;

    if (pd != NULLPTR) {
      PdxTypePtr mergedPdxType =
          PdxTypeRegistry::getPdxType(pd->getMergedTypeId());
      prw = PdxRemoteWriterPtr(new PdxRemoteWriter(output, mergedPdxType, pd));
    } else {
      prw = PdxRemoteWriterPtr(new PdxRemoteWriter(output, pdxClassname));
    }
    PdxWriterPtr pwptr(dynCast<PdxWriterPtr>(prw));
    pdx.toData(pwptr);
    prw->endObjectWriting();

    //[ToDo] need to write bytes for stats
    CacheImpl* cacheImpl = PdxHelper::getCacheImpl();
    if (cacheImpl != NULL) {
      uint8_t* stPos = const_cast<uint8_t*>(output.getBuffer()) +
                       prw->getStartPositionOffset();
      int pdxLen = PdxHelper::readInt32(stPos);
      cacheImpl->m_cacheStats->incPdxSerialization(
          pdxLen + 1 + 2 * 4);  // pdxLen + 93 DSID + len + typeID
    }
  }
}

PdxSerializablePtr PdxHelper::deserializePdx(DataInput& dataInput,
                                             bool forceDeserialize,
                                             int32 typeId, int32 length) {
  char* pdxClassname = NULL;
  // char* pdxDomainClassname = NULL;
  PdxSerializablePtr pdxObjectptr = NULLPTR;
  PdxTypePtr pdxLocalType = NULLPTR;

  PdxTypePtr pType = PdxTypeRegistry::getPdxType(typeId);
  if (pType != NULLPTR) {  // this may happen with PdxInstanceFactory {
    pdxLocalType = PdxTypeRegistry::getLocalPdxType(
        pType->getPdxClassName());  // this should be fine for IPdxTypeMapper
  }
  if (pType != NULLPTR && pdxLocalType != NULLPTR)  // type found
  {
    pdxClassname = pType->getPdxClassName();
    LOGDEBUG("deserializePdx ClassName = %s, isLocal = %d ",
             pType->getPdxClassName(), pType->isLocal());

    pdxObjectptr = SerializationRegistry::getPdxType(pdxClassname);
    if (pType->isLocal())  // local type no need to read Unread data
    {
      PdxLocalReaderPtr plr(new PdxLocalReader(dataInput, pType, length));
      PdxReaderPtr prp(dynCast<PdxReaderPtr>(plr));
      pdxObjectptr->fromData(prp);
      plr->MoveStream();
    } else {
      PdxRemoteReaderPtr prr(new PdxRemoteReader(dataInput, pType, length));
      PdxReaderPtr prp(dynCast<PdxReaderPtr>(prr));
      pdxObjectptr->fromData(prp);
      PdxTypePtr mergedVersion =
          PdxTypeRegistry::getMergedType(pType->getTypeId());

      PdxRemotePreservedDataPtr preserveData =
          prr->getPreservedData(mergedVersion, pdxObjectptr);
      if (preserveData != NULLPTR) {
        PdxTypeRegistry::setPreserveData(
            pdxObjectptr, preserveData);  // it will set data in weakhashmap
      }
      prr->MoveStream();
    }
  } else {
    // type not found; need to get from server
    if (pType == NULLPTR) {
      pType = SerializationRegistry::GetPDXTypeById(dataInput.getPoolName(),
                                                    typeId);
      pdxLocalType = PdxTypeRegistry::getLocalPdxType(pType->getPdxClassName());
    }
    /* adongre  - Coverity II
     * CID 29298: Unused pointer value (UNUSED_VALUE)
     * Pointer "pdxClassname" returned by "pType->getPdxClassName()" is never
     * used.
     * Fix : Commented the line
     */
    // pdxClassname = pType->getPdxClassName();
    pdxObjectptr = SerializationRegistry::getPdxType(pType->getPdxClassName());
    PdxSerializablePtr pdxRealObject = pdxObjectptr;
    // bool isPdxWrapper = false;
    /*
    PdxWrapperPtr pdxWrapper = dynamic_cast<PdxWrapperPtr>(pdxObject);

    if(pdxWrapper != nullptr)
    {
      isPdxWrapper = true;
    }
    else{}*/
    if (pdxLocalType == NULLPTR)  // need to know local type
    {
      PdxReaderWithTypeCollectorPtr prtc(
          new PdxReaderWithTypeCollector(dataInput, pType, length));
      PdxReaderPtr prp(dynCast<PdxReaderPtr>(prtc));
      pdxObjectptr->fromData(prp);

      // Check for the PdxWrapper

      pdxLocalType = prtc->getLocalType();

      if (pType->Equals(pdxLocalType)) {
        PdxTypeRegistry::addLocalPdxType(pdxRealObject->getClassName(), pType);
        PdxTypeRegistry::addPdxType(pType->getTypeId(), pType);
        pType->setLocal(true);
      } else {
        // Need to know local type and then merge type
        pdxLocalType->InitializeType();
        pdxLocalType->setTypeId(PdxTypeRegistry::getPDXIdForType(
            pdxObjectptr->getClassName(), dataInput.getPoolName(), pdxLocalType,
            true));
        pdxLocalType->setLocal(true);
        PdxTypeRegistry::addLocalPdxType(pdxRealObject->getClassName(),
                                         pdxLocalType);  // added local type
        PdxTypeRegistry::addPdxType(pdxLocalType->getTypeId(), pdxLocalType);

        pType->InitializeType();
        PdxTypeRegistry::addPdxType(pType->getTypeId(),
                                    pType);  // adding remote type

        // create merge type
        createMergedType(pdxLocalType, pType, dataInput);

        PdxTypePtr mergedVersion =
            PdxTypeRegistry::getMergedType(pType->getTypeId());

        PdxRemotePreservedDataPtr preserveData =
            prtc->getPreservedData(mergedVersion, pdxObjectptr);
        if (preserveData != NULLPTR) {
          PdxTypeRegistry::setPreserveData(pdxObjectptr, preserveData);
        }
      }
      prtc->MoveStream();
    } else {  // remote reader will come here as local type is there
      pType->InitializeType();
      LOGDEBUG("Adding type %d ", pType->getTypeId());
      PdxTypeRegistry::addPdxType(pType->getTypeId(),
                                  pType);  // adding remote type
      PdxRemoteReaderPtr prr(new PdxRemoteReader(dataInput, pType, length));
      PdxReaderPtr prp(dynCast<PdxReaderPtr>(prr));
      pdxObjectptr->fromData(prp);

      // Check for PdxWrapper to getObject.

      createMergedType(pdxLocalType, pType, dataInput);

      PdxTypePtr mergedVersion =
          PdxTypeRegistry::getMergedType(pType->getTypeId());

      PdxRemotePreservedDataPtr preserveData =
          prr->getPreservedData(mergedVersion, pdxObjectptr);
      if (preserveData != NULLPTR) {
        PdxTypeRegistry::setPreserveData(pdxObjectptr, preserveData);
      }
      prr->MoveStream();
    }
  }
  return pdxObjectptr;
}

PdxSerializablePtr PdxHelper::deserializePdx(DataInput& dataInput,
                                             bool forceDeserialize) {
  if (PdxTypeRegistry::getPdxReadSerialized() == false || forceDeserialize) {
    // Read Length
    int32_t len;
    dataInput.readInt(&len);

    int32_t typeId;
    // read typeId
    dataInput.readInt(&typeId);

    CacheImpl* cacheImpl = PdxHelper::getCacheImpl();
    if (cacheImpl != NULL) {
      cacheImpl->m_cacheStats->incPdxDeSerialization(len +
                                                     9);  // pdxLen + 1 + 2*4
    }
    return PdxHelper::deserializePdx(dataInput, forceDeserialize, (int32)typeId,
                                     (int32)len);

  } else {
    // PdxSerializablePtr pdxObject = NULLPTR;
    // Read Length
    int32_t len;
    dataInput.readInt(&len);

    int typeId;
    // read typeId
    dataInput.readInt(&typeId);

    PdxTypePtr pType = PdxTypeRegistry::getPdxType(typeId);

    if (pType == NULLPTR) {
      PdxTypePtr pType = SerializationRegistry::GetPDXTypeById(
          dataInput.getPoolName(), typeId);
      PdxTypeRegistry::addLocalPdxType(pType->getPdxClassName(), pType);
      PdxTypeRegistry::addPdxType(pType->getTypeId(), pType);
    }

    /*
    ==2018== 398,151 bytes in 3 blocks are definitely lost in loss record 707 of
    710
    ==2018==    at 0x400791A: operator new[](unsigned int)
    (vg_replace_malloc.c:378)
    ==2018==    by 0x4303969:
    gemfire::PdxHelper::deserializePdx(gemfire::DataInput&, bool)
    (DataInput.hpp:1007)
    ==2018==    by 0x43210D4:
    gemfire::PdxInstantiator::fromData(gemfire::DataInput&)
    (PdxInstantiator.cpp:30)
    ==2018==    by 0x434A72E:
    gemfire::SerializationRegistry::deserialize(gemfire::DataInput&, signed
    char) (SerializationRegistry.cpp:378)
    ==2018==    by 0x42280FB:
    gemfire::DataInput::readObjectInternal(gemfire::SharedPtr<gemfire::Serializable>&,
    signed char) (DataInput.cpp:9)
    ==2018==    by 0x438107E:
    gemfire::TcrMessage::readObjectPart(gemfire::DataInput&, bool)
    (DataInput.hpp:694)
    ==2018==    by 0x4375638: gemfire::TcrMessage::handleByteArrayResponse(char
    const*, int, unsigned short) (TcrMessage.cpp:1087)
    ==2018==    by 0x4375B2E: gemfire::TcrMessage::setData(char const*, int,
    unsigned short) (TcrMessage.cpp:3933)
    ==2018==    by 0x4367EE6:
    gemfire::TcrEndpoint::sendRequestConn(gemfire::TcrMessage const&,
    gemfire::TcrMessage&, gemfire::TcrConnection*, stlp_std::basic_string<char,
    stlp_std::char_traits<char>, stlp_std::allocator<char> >&)
    (TcrEndpoint.cpp:764)
    ==2018==    by 0x43682E3:
    gemfire::TcrEndpoint::sendRequestWithRetry(gemfire::TcrMessage const&,
    gemfire::TcrMessage&, gemfire::TcrConnection*&, bool&,
    stlp_std::basic_string<char, stlp_std::char_traits<char>,
    stlp_std::allocator<char> >&, int, bool, long long, bool)
    (TcrEndpoint.cpp:869)
    ==2018==    by 0x4368EBA:
    gemfire::TcrEndpoint::sendRequestConnWithRetry(gemfire::TcrMessage const&,
    gemfire::TcrMessage&, gemfire::TcrConnection*&, bool) (TcrEndpoint.cpp:1060)
    ==2018==    by 0x4393D2D:
    gemfire::ThinClientPoolDM::sendSyncRequest(gemfire::TcrMessage&,
    gemfire::TcrMessage&, bool, bool,
    gemfire::SharedPtr<gemfire::BucketServerLocation> const&)
    (ThinClientPoolDM.cpp:1408)
    ==2018==
    */
    // TODO::Enable it once the PdxInstanceImple is CheckedIn.
    PdxSerializablePtr pdxObject(new PdxInstanceImpl(
        const_cast<uint8_t*>(dataInput.currentBufferPosition()), len, typeId));

    dataInput.advanceCursor(len);

    // dataInput->AdvanceCursorPdx(len);
    // dataInput->AdvanceUMCursor();
    // dataInput->SetBuffer();

    CacheImpl* cacheImpl = PdxHelper::getCacheImpl();
    if (cacheImpl != NULL) {
      cacheImpl->m_cacheStats->incPdxInstanceCreations();
    }
    return pdxObject;
  }
}

void PdxHelper::createMergedType(PdxTypePtr localType, PdxTypePtr remoteType,
                                 DataInput& dataInput) {
  PdxTypePtr mergedVersion = localType->mergeVersion(remoteType);

  if (mergedVersion->Equals(localType)) {
    PdxTypeRegistry::setMergedType(remoteType->getTypeId(), localType);
  } else if (mergedVersion->Equals(remoteType)) {
    PdxTypeRegistry::setMergedType(remoteType->getTypeId(), remoteType);
  } else {  // need to create new version
    mergedVersion->InitializeType();
    if (mergedVersion->getTypeId() == 0) {
      mergedVersion->setTypeId(SerializationRegistry::GetPDXIdForType(
          dataInput.getPoolName(), mergedVersion));
    }

    // PdxTypeRegistry::AddPdxType(remoteType->TypeId, mergedVersion);
    PdxTypeRegistry::addPdxType(mergedVersion->getTypeId(), mergedVersion);
    PdxTypeRegistry::setMergedType(remoteType->getTypeId(), mergedVersion);
    PdxTypeRegistry::setMergedType(mergedVersion->getTypeId(), mergedVersion);
  }
}

int32 PdxHelper::readInt32(uint8_t* offsetPosition) {
  int32 data = offsetPosition[0];
  data = (data << 8) | offsetPosition[1];
  data = (data << 8) | offsetPosition[2];
  data = (data << 8) | offsetPosition[3];

  return data;
}

void PdxHelper::writeInt32(uint8_t* offsetPosition, int32 value) {
  offsetPosition[0] = static_cast<uint8_t>(value >> 24);
  offsetPosition[1] = static_cast<uint8_t>(value >> 16);
  offsetPosition[2] = static_cast<uint8_t>(value >> 8);
  offsetPosition[3] = static_cast<uint8_t>(value);
}

int32 PdxHelper::readInt16(uint8_t* offsetPosition) {
  int16_t data = offsetPosition[0];
  data = (data << 8) | offsetPosition[1];
  return static_cast<int32>(data);
}

int32 PdxHelper::readUInt16(uint8_t* offsetPosition) {
  uint16 data = offsetPosition[0];
  data = (data << 8) | offsetPosition[1];
  return static_cast<int32>(data);
}

int32 PdxHelper::readByte(uint8_t* offsetPosition) {
  return static_cast<int32>(offsetPosition[0]);
}

void PdxHelper::writeInt16(uint8_t* offsetPosition, int32 value) {
  int16 val = static_cast<int16>(value);
  offsetPosition[0] = static_cast<uint8_t>(val >> 8);
  offsetPosition[1] = static_cast<uint8_t>(val);
}

void PdxHelper::writeByte(uint8_t* offsetPosition, int32 value) {
  offsetPosition[0] = static_cast<uint8_t>(value);
}

int32 PdxHelper::readInt(uint8_t* offsetPosition, int size) {
  switch (size) {
    case 1:
      return readByte(offsetPosition);
    case 2:
      return readUInt16(offsetPosition);
    case 4:
      return readInt32(offsetPosition);
  }
  throw;
}

int32 PdxHelper::getEnumValue(const char* enumClassName, const char* enumName,
                              int hashcode) {
  EnumInfoPtr ei(new EnumInfo(enumClassName, enumName, hashcode));
  return PdxTypeRegistry::getEnumValue(ei);
}

EnumInfoPtr PdxHelper::getEnum(int enumId) {
  EnumInfoPtr ei = PdxTypeRegistry::getEnum(enumId);
  return ei;
}
}  // namespace gemfire
