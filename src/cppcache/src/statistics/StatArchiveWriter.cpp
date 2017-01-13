/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>

#include <ace/ACE.h>
#include <ace/Thread_Mutex.h>
#include <ace/Task.h>
#include <ace/OS_NS_sys_utsname.h>
#include <ace/OS_NS_time.h>
#include <ace/OS_NS_sys_time.h>

#include "StatArchiveWriter.hpp"
#include "GemfireStatisticsFactory.hpp"
#include <NanoTimer.hpp>

using namespace gemfire;
using namespace gemfire_statistics;

// Constructor and Member functions of StatDataOutput class

StatDataOutput::StatDataOutput(std::string filename) {
  if (filename.length() == 0) {
    std::string s("undefined archive file name");
    throw IllegalArgumentException(s.c_str());
  }
  outFile = filename;
  closed = false;
  bytesWritten = 0;
  m_fp = fopen(outFile.c_str(), "a+b");
  if (m_fp == NULL) {
    std::string s("error in opening archive file for writing");
    throw NullPointerException(s.c_str());
  }
}

StatDataOutput::~StatDataOutput() {
  if (!closed && m_fp != NULL) {
    fclose(m_fp);
  }
}

int64 StatDataOutput::getBytesWritten() { return this->bytesWritten; }

void StatDataOutput::flush() {
  const uint8 *buffBegin = dataBuffer.getBuffer();
  if (buffBegin == NULL) {
    std::string s("undefined stat data buffer beginning");
    throw NullPointerException(s.c_str());
  }
  const uint8 *buffEnd = dataBuffer.getCursor();
  if (buffEnd == NULL) {
    std::string s("undefined stat data buffer end");
    throw NullPointerException(s.c_str());
  }
  int32 sizeOfUInt8 = sizeof(uint8);
  int32 len = static_cast<int32>(buffEnd - buffBegin);

  if (len > 0) {
    if (fwrite(buffBegin, sizeOfUInt8, len, m_fp) != static_cast<size_t>(len)) {
      LOGERROR("Could not write into the statistics file");
      throw GemfireIOException("Could not write into the statistics file");
    }
  }
  int rVal = fflush(m_fp);
  if (rVal != 0) {
    LOGERROR("Could not flush into the statistics file");
    throw GemfireIOException("Could not flush into the statistics file");
  }
}

void StatDataOutput::resetBuffer() {
  dataBuffer.reset();
  bytesWritten = 0;
}

void StatDataOutput::writeByte(int8 v) {
  dataBuffer.write((int8_t)v);
  bytesWritten += 1;
}

void StatDataOutput::writeBoolean(int8 v) { writeByte(v); }

void StatDataOutput::writeShort(int16 v) {
  dataBuffer.writeInt(v);
  bytesWritten += 2;
}

void StatDataOutput::writeInt(int32 v) {
  dataBuffer.writeInt(v);
  bytesWritten += 4;
}

void StatDataOutput::writeLong(int64 v) {
  dataBuffer.writeInt(v);
  bytesWritten += 8;
}

void StatDataOutput::writeString(std::string s) {
  size_t len = s.length();
  dataBuffer.writeASCII(s.data(), static_cast<uint32_t>(len));
  bytesWritten += len;
}

void StatDataOutput::writeUTF(std::wstring s) {
  size_t len = s.length();
  dataBuffer.writeUTF(s.data(), static_cast<uint32_t>(len));
  bytesWritten += len;
}

void StatDataOutput::close() {
  fclose(m_fp);
  m_fp = NULL;
  closed = true;
}

void StatDataOutput::openFile(std::string filename, int64 size) {
  m_fp = fopen(filename.c_str(), "a+b");
  if (m_fp == NULL) {
    std::string s("error in opening archive file for writing");
    throw NullPointerException(s.c_str());
  }
  closed = false;
  bytesWritten = size;
}

// Constructor and Member functions of ResourceType class

ResourceType::ResourceType(int32 idArg, StatisticsType *typeArg) {
  StatisticsType *typeImpl = dynamic_cast<StatisticsType *>(typeArg);
  if (typeImpl == NULL) {
    std::string s("could not down cast to StatisticsType");
    throw NullPointerException(s.c_str());
  }
  this->id = idArg;
  this->stats = typeImpl->getStatistics();
  int32 desc = typeImpl->getDescriptorsCount();
  this->numOfDescriptors = desc;
}

int32 ResourceType::getId() { return this->id; }

int32 ResourceType::getNumOfDescriptors() { return this->numOfDescriptors; }

StatisticDescriptor **ResourceType::getStats() { return this->stats; }

// Constructor and Member functions of ResourceInst class

ResourceInst::ResourceInst(int32 idArg, Statistics *resourceArg,
                           ResourceType *typeArg, StatDataOutput *dataOutArg) {
  this->id = idArg;
  this->resource = resourceArg;
  this->type = typeArg;
  this->dataOut = dataOutArg;
  int32 cnt = type->getNumOfDescriptors();
  archivedStatValues = new int64[cnt];
  // initialize to zero
  for (int32 i = 0; i < cnt; i++) {
    archivedStatValues[i] = 0;
  }
  numOfDescps = cnt;
  firstTime = true;
}

ResourceInst::~ResourceInst() { delete[] archivedStatValues; }

int32 ResourceInst::getId() { return this->id; }

Statistics *ResourceInst::getResource() { return this->resource; }

ResourceType *ResourceInst::getType() { return this->type; }

int64 ResourceInst::getStatValue(StatisticDescriptor *f) {
  return this->resource->getRawBits(f);
}

void ResourceInst::writeSample() {
  bool wroteInstId = false;
  bool checkForChange = true;
  StatisticDescriptor **stats = this->type->getStats();
  GF_D_ASSERT(stats != NULL);
  GF_D_ASSERT(*stats != NULL);
  if (this->resource->isClosed()) {
    return;
  }
  if (firstTime) {
    firstTime = false;
    checkForChange = false;
  }
  for (int32 i = 0; i < numOfDescps; i++) {
    int64 value = getStatValue(stats[i]);
    if (!checkForChange || value != archivedStatValues[i]) {
      int64 delta = value - archivedStatValues[i];
      archivedStatValues[i] = value;
      if (!wroteInstId) {
        wroteInstId = true;
        writeResourceInst(this->dataOut, this->id);
      }
      this->dataOut->writeByte(i);
      writeStatValue(stats[i], delta);
    }
  }
  if (wroteInstId) {
    this->dataOut->writeByte(static_cast<unsigned char>(ILLEGAL_STAT_OFFSET));
  }
}

void ResourceInst::writeStatValue(StatisticDescriptor *sd, int64 v) {
  StatisticDescriptorImpl *sdImpl = (StatisticDescriptorImpl *)sd;
  if (sdImpl == NULL) {
    throw NullPointerException("could not downcast to StatisticDescriptorImpl");
  }
  FieldType typeCode = sdImpl->getTypeCode();

  switch (typeCode) {
    /*  case GF_FIELDTYPE_BYTE:
        this->dataOut->writeByte((int8)v);
        break; */
    /*  case GF_FIELDTYPE_SHORT:
        this->dataOut->writeShort((int16)v);
        break; */
    case INT_TYPE:
    case LONG_TYPE:
    //   case GF_FIELDTYPE_FLOAT:
    case DOUBLE_TYPE:
      writeCompactValue(v);
      break;
    default:
      std::string s = "Unexpected type code";
      throw IllegalArgumentException(s.c_str());
      break;
  }
}

void ResourceInst::writeCompactValue(int64 v) {
  if (v <= MAX_1BYTE_COMPACT_VALUE && v >= MIN_1BYTE_COMPACT_VALUE) {
    this->dataOut->writeByte(static_cast<int8>(v));
  } else if (v <= MAX_2BYTE_COMPACT_VALUE && v >= MIN_2BYTE_COMPACT_VALUE) {
    this->dataOut->writeByte(COMPACT_VALUE_2_TOKEN);
    this->dataOut->writeShort(static_cast<int16>(v));
  } else {
    int8 buffer[8];
    int32 idx = 0;
    if (v < 0) {
      while (v != -1 && v != 0) {
        buffer[idx++] = static_cast<int8>(v & 0xFF);
        v >>= 8;
      }
      // On windows v goes to zero somtimes; seems like a bug
      if (v == 0) {
        // when this happens we end up with a bunch of -1 bytes
        // so strip off the high order ones
        while (0 < idx && buffer[idx - 1] == -1) {
          idx--;
        }
      }
      if (0 < idx && (buffer[idx - 1] & 0x80) == 0) {
        /* If the most significant byte does not have its high order bit set
         * then add a -1 byte so we know this is a negative number
         */
        buffer[idx++] = -1;
      }
    } else {
      while (v != 0) {
        buffer[idx++] = static_cast<int8>(v & 0xFF);
        v >>= 8;
      }
      if ((buffer[idx - 1] & 0x80) != 0) {
        /* If the most significant byte has its high order bit set
         * then add a zero byte so we know this is a positive number
         */
        buffer[idx++] = 0;
      }
    }
    int8 token = COMPACT_VALUE_2_TOKEN + (idx - 2);
    this->dataOut->writeByte(token);
    for (int32 i = idx - 1; i >= 0; i--) {
      this->dataOut->writeByte(buffer[i]);
    }
  }
}

void ResourceInst::writeResourceInst(StatDataOutput *dataOutArg, int32 instId) {
  if (instId > MAX_BYTE_RESOURCE_INST_ID) {
    if (instId > MAX_SHORT_RESOURCE_INST_ID) {
      dataOutArg->writeByte(static_cast<int8>(INT_RESOURCE_INST_ID_TOKEN));
      dataOutArg->writeInt(instId);
    } else {
      dataOutArg->writeByte(static_cast<int8>(SHORT_RESOURCE_INST_ID_TOKEN));
      dataOutArg->writeShort(instId);
    }
  } else {
    dataOutArg->writeByte(static_cast<int8>(instId));
  }
}

// Constructor and Member functions of StatArchiveWriter class
StatArchiveWriter::StatArchiveWriter(std::string outfile,
                                     HostStatSampler *samplerArg) {
  resourceTypeId = 0;
  resourceInstId = 0;
  statResourcesModCount = 0;
  archiveFile = outfile;
  bytesWrittenToFile = 0;

  /* adongre
   * CID 28982: Uninitialized scalar field (UNINIT_CTOR)
   */
  m_samplesize = 0;

  dataBuffer = new StatDataOutput(archiveFile);
  this->sampler = samplerArg;

  // write the time, system property etc.
  this->previousTimeStamp = NanoTimer::now();
  this->previousTimeStamp += NANOS_PER_MILLI / 2;
  this->previousTimeStamp /= NANOS_PER_MILLI;
  ACE_Time_Value now = ACE_OS::gettimeofday();
  int64 epochsec = now.sec();
  int64 initialDate = epochsec * 1000;

  this->dataBuffer->writeByte(HEADER_TOKEN);
  this->dataBuffer->writeByte(ARCHIVE_VERSION);
  this->dataBuffer->writeLong(initialDate);
  int64 sysId = sampler->getSystemId();
  this->dataBuffer->writeLong(sysId);
  int64 sysStartTime = sampler->getSystemStartTime();
  this->dataBuffer->writeLong(sysStartTime);
  int32 tzOffset = ACE_OS::timezone();
  // offset in milli seconds
  tzOffset = tzOffset * -1 * 1000;
  // tzOffset = tzOffset  * 1000;
  this->dataBuffer->writeInt(tzOffset);

  struct tm *tm_val;
  time_t clock = ACE_OS::time();
  tm_val = localtime(&clock);
  char buf[512] = {0};
  ACE_OS::strftime(buf, sizeof(buf), "%Z", tm_val);
  std::string tzId(buf);
  this->dataBuffer->writeString(tzId);

  std::string sysDirPath = sampler->getSystemDirectoryPath();
  this->dataBuffer->writeString(sysDirPath);
  std::string prodDesc = sampler->getProductDescription();

  this->dataBuffer->writeString(prodDesc);
  ACE_utsname u;
  ACE_OS::uname(&u);
  std::string os(u.sysname);
  os += " ";
  /* This version name returns date of release of the version which
   creates confusion about the creation time of the vsd file. Hence
   removing it now. Later I'll change it to just show version without
   date. For now only name of the OS will be displayed.
   */
  // os += u.version;

  this->dataBuffer->writeString(os);
  std::string machineInfo(u.machine);
  machineInfo += " ";
  machineInfo += u.nodename;
  this->dataBuffer->writeString(machineInfo);

  resampleResources();
}

StatArchiveWriter::~StatArchiveWriter() {
  if (dataBuffer != NULL) {
    delete dataBuffer;
    dataBuffer = NULL;
  }
  std::map<StatisticsType *, ResourceType *>::iterator p;
  for (p = resourceTypeMap.begin(); p != resourceTypeMap.end(); p++) {
    ResourceType *rt = (*p).second;
    GF_SAFE_DELETE(rt);
  }
}

int64 StatArchiveWriter::bytesWritten() { return bytesWrittenToFile; }

int64 StatArchiveWriter::getSampleSize() { return m_samplesize; }

void StatArchiveWriter::sample(int64 timeStamp) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(sampler->getStatListMutex());
  m_samplesize = dataBuffer->getBytesWritten();

  sampleResources();
  this->dataBuffer->writeByte(SAMPLE_TOKEN);
  writeTimeStamp(timeStamp);
  std::map<Statistics *, ResourceInst *>::iterator p;
  for (p = resourceInstMap.begin(); p != resourceInstMap.end(); p++) {
    ResourceInst *ri = (*p).second;
    if (!!ri && (*p).first != NULL) {
      ri->writeSample();
    }
  }
  writeResourceInst(this->dataBuffer,
                    static_cast<int32>(ILLEGAL_RESOURCE_INST_ID));
  m_samplesize = dataBuffer->getBytesWritten() - m_samplesize;
}

void StatArchiveWriter::sample() {
  int64 timestamp = NanoTimer::now();
  sample(timestamp);
}

void StatArchiveWriter::close() {
  sample();
  this->dataBuffer->flush();
  this->dataBuffer->close();
}

void StatArchiveWriter::closeFile() { this->dataBuffer->close(); }

void StatArchiveWriter::openFile(std::string filename) {
  // this->dataBuffer->openFile(filename, m_samplesize);

  StatDataOutput *p_dataBuffer = new StatDataOutput(filename);

  /*
  ACE_Time_Value now = ACE_OS::gettimeofday();
  int64 epochsec = now.sec();
  int64 initialDate = (int64)epochsec * 1000;

  p_dataBuffer->writeByte(HEADER_TOKEN);
  p_dataBuffer->writeByte(ARCHIVE_VERSION);
  p_dataBuffer->writeLong(initialDate);
  int64 sysId = sampler->getSystemId();
  p_dataBuffer->writeLong((int64)sysId);
  int64 sysStartTime =  sampler->getSystemStartTime();
  p_dataBuffer->writeLong(sysStartTime);
  int32 tzOffset = ACE_OS::timezone();
  // offset in milli seconds
  tzOffset = tzOffset * -1 * 1000;
  // tzOffset = tzOffset  * 1000;
  p_dataBuffer->writeInt(tzOffset);

  struct tm* tm_val;
  time_t clock = ACE_OS::time();
  tm_val = localtime(&clock);
  char buf[512] = {0};
  ACE_OS::strftime(buf, sizeof(buf), "%Z", tm_val);
  std::string tzId(buf);
  p_dataBuffer->writeString(tzId);

  std::string sysDirPath = sampler->getSystemDirectoryPath();
  p_dataBuffer->writeString(sysDirPath);
  std::string prodDesc = sampler->getProductDescription();

  p_dataBuffer->writeString(prodDesc);
  ACE_utsname u;
  ACE_OS::uname(&u);
  std::string os(u.sysname);
  os += " ";

  p_dataBuffer->writeString(os);
  std::string machineInfo(u.machine);
  machineInfo += " ";
  machineInfo += u.nodename;
  p_dataBuffer->writeString(machineInfo);
  */

  const uint8 *buffBegin = dataBuffer->dataBuffer.getBuffer();
  if (buffBegin == NULL) {
    std::string s("undefined stat data buffer beginning");
    throw NullPointerException(s.c_str());
  }
  const uint8 *buffEnd = dataBuffer->dataBuffer.getCursor();
  if (buffEnd == NULL) {
    std::string s("undefined stat data buffer end");
    throw NullPointerException(s.c_str());
  }
  int32 len = static_cast<int32>(buffEnd - buffBegin);

  for (int pos = 0; pos < len; pos++) {
    p_dataBuffer->writeByte(buffBegin[pos]);
  }

  delete dataBuffer;
  dataBuffer = p_dataBuffer;

  // sample();
}

void StatArchiveWriter::flush() {
  this->dataBuffer->flush();
  bytesWrittenToFile += dataBuffer->getBytesWritten();
  this->dataBuffer->resetBuffer();
  /*
    // have to figure out the problem with this code.
    delete dataBuffer;
    dataBuffer = NULL;

    dataBuffer = new StatDataOutput(archiveFile);
   */
}

void StatArchiveWriter::sampleResources() {
  // Allocate ResourceInst for newly added stats ( Locked lists already )
  std::vector<Statistics *> &newStatsList = sampler->getNewStatistics();
  std::vector<Statistics *>::iterator newlistIter;
  for (newlistIter = newStatsList.begin(); newlistIter != newStatsList.end();
       ++newlistIter) {
    if (!resourceInstMapHas(*newlistIter)) {
      allocateResourceInst(*newlistIter);
    }
  }
  newStatsList.clear();

  // for closed stats, write token and then delete from statlist and
  // resourceInstMap.
  std::map<Statistics *, ResourceInst *>::iterator mapIter;
  std::vector<Statistics *> &statsList = sampler->getStatistics();
  std::vector<Statistics *>::iterator statlistIter = statsList.begin();
  while (statlistIter != statsList.end()) {
    if ((*statlistIter)->isClosed()) {
      mapIter = resourceInstMap.find(*statlistIter);
      if (mapIter != resourceInstMap.end()) {
        // Write delete token to file and delete from map
        ResourceInst *rinst = (*mapIter).second;
        int32 id = rinst->getId();
        this->dataBuffer->writeByte(RESOURCE_INSTANCE_DELETE_TOKEN);
        this->dataBuffer->writeInt(id);
        resourceInstMap.erase(mapIter);
        delete rinst;
      }
      // Delete stats object stat list
      StatisticsManager::deleteStatistics(*statlistIter);
      statsList.erase(statlistIter);
      statlistIter = statsList.begin();
    } else {
      ++statlistIter;
    }
  }
}

void StatArchiveWriter::resampleResources() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(sampler->getStatListMutex());
  std::vector<Statistics *> &statsList = sampler->getStatistics();
  std::vector<Statistics *>::iterator statlistIter = statsList.begin();
  while (statlistIter != statsList.end()) {
    if (!(*statlistIter)->isClosed()) {
      allocateResourceInst(*statlistIter);
    }
    ++statlistIter;
  }
}

void StatArchiveWriter::writeTimeStamp(int64 timeStamp) {
  timeStamp += NANOS_PER_MILLI / 2;
  timeStamp /= NANOS_PER_MILLI;
  int64 delta = timeStamp - this->previousTimeStamp;
  // printf("delta = %lld\n", delta);
  if (delta > MAX_SHORT_TIMESTAMP) {
    dataBuffer->writeShort(static_cast<int16_t>(INT_TIMESTAMP_TOKEN));
    dataBuffer->writeInt(static_cast<int32>(delta));
  } else {
    dataBuffer->writeShort(static_cast<uint16>(delta));
  }
  this->previousTimeStamp = timeStamp;
}

bool StatArchiveWriter::resourceInstMapHas(Statistics *sp) {
  std::map<Statistics *, ResourceInst *>::iterator p;
  p = resourceInstMap.find(sp);
  if (p != resourceInstMap.end()) {
    return true;
  } else {
    return false;
  }
}

void StatArchiveWriter::allocateResourceInst(Statistics *s) {
  if (s->isClosed()) return;
  ResourceType *type = getResourceType(s);

  ResourceInst *ri = new ResourceInst(resourceInstId, s, type, dataBuffer);
  if (ri == NULL) {
    std::string s("could not create new resource instance");
    throw NullPointerException(s.c_str());
  }
  resourceInstMap.insert(std::pair<Statistics *, ResourceInst *>(s, ri));
  this->dataBuffer->writeByte(RESOURCE_INSTANCE_CREATE_TOKEN);
  this->dataBuffer->writeInt(resourceInstId);
  this->dataBuffer->writeString(s->getTextId());
  this->dataBuffer->writeLong(s->getNumericId());
  this->dataBuffer->writeInt(type->getId());

  resourceInstId++;
}

ResourceType *StatArchiveWriter::getResourceType(Statistics *s) {
  StatisticsType *type = s->getType();
  if (type == NULL) {
    std::string s("could not know the type of the statistics object");
    throw NullPointerException(s.c_str());
  }
  ResourceType *rt = NULL;
  std::map<StatisticsType *, ResourceType *>::iterator p;
  p = resourceTypeMap.find(type);
  if (p != resourceTypeMap.end()) {
    rt = (*p).second;
  } else {
    rt = new ResourceType(resourceTypeId, type);
    if (type == NULL) {
      std::string s("could not allocate memory for a new resourcetype");
      throw NullPointerException(s.c_str());
    }
    resourceTypeMap.insert(
        std::pair<StatisticsType *, ResourceType *>(type, rt));
    // write the type to the archive
    this->dataBuffer->writeByte(RESOURCE_TYPE_TOKEN);
    this->dataBuffer->writeInt(resourceTypeId);
    this->dataBuffer->writeString(type->getName());
    this->dataBuffer->writeString(type->getDescription());
    StatisticDescriptor **stats = rt->getStats();
    int32 descCnt = rt->getNumOfDescriptors();
    this->dataBuffer->writeShort(static_cast<int16>(descCnt));
    for (int32 i = 0; i < descCnt; i++) {
      std::string statsName = stats[i]->getName();
      this->dataBuffer->writeString(statsName);
      StatisticDescriptorImpl *sdImpl = (StatisticDescriptorImpl *)stats[i];
      if (sdImpl == NULL) {
        std::string err("could not down cast to StatisticDescriptorImpl");
        throw NullPointerException(err.c_str());
      }
      this->dataBuffer->writeByte(static_cast<int8>(sdImpl->getTypeCode()));
      this->dataBuffer->writeBoolean(stats[i]->isCounter());
      this->dataBuffer->writeBoolean(stats[i]->isLargerBetter());
      this->dataBuffer->writeString(stats[i]->getUnit());
      this->dataBuffer->writeString(stats[i]->getDescription());
    }
    // increment resourceTypeId
    resourceTypeId++;
  }
  return rt;
}

void StatArchiveWriter::writeResourceInst(StatDataOutput *dataOut,
                                          int32 instId) {
  if (instId > MAX_BYTE_RESOURCE_INST_ID) {
    if (instId > MAX_SHORT_RESOURCE_INST_ID) {
      dataOut->writeByte(static_cast<int8>(INT_RESOURCE_INST_ID_TOKEN));
      dataOut->writeInt(instId);
    } else {
      dataOut->writeByte(static_cast<int8>(SHORT_RESOURCE_INST_ID_TOKEN));
      dataOut->writeShort(instId);
    }
  } else {
    dataOut->writeByte(static_cast<int8>(instId));
  }
}
