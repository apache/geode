/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef _GEMFIRE_STATISTICS_STATARCHIVERITER_HPP_
#define _GEMFIRE_STATISTICS_STATARCHIVERITER_HPP_

#include <map>
#include <list>
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include "StatsDef.hpp"
#include <gfcpp/statistics/Statistics.hpp>
#include <gfcpp/statistics/StatisticDescriptor.hpp>
#include "StatisticDescriptorImpl.hpp"
#include <gfcpp/statistics/StatisticsType.hpp>
#include "HostStatSampler.hpp"
#include <gfcpp/Log.hpp>
#include <gfcpp/DataOutput.hpp>
#include <NonCopyable.hpp>

using namespace gemfire;

/**
 * some constants to be used while archiving
 */
const int8 ARCHIVE_VERSION = 4;
const int8 SAMPLE_TOKEN = 0;
const int8 RESOURCE_TYPE_TOKEN = 1;
const int8 RESOURCE_INSTANCE_CREATE_TOKEN = 2;
const int8 RESOURCE_INSTANCE_DELETE_TOKEN = 3;
const int8 RESOURCE_INSTANCE_INITIALIZE_TOKEN = 4;
const int8 HEADER_TOKEN = 77;
const int8 ILLEGAL_RESOURCE_INST_ID = -1;
const int16 MAX_BYTE_RESOURCE_INST_ID = 252;
const int16 SHORT_RESOURCE_INST_ID_TOKEN = 253;
const int32 INT_RESOURCE_INST_ID_TOKEN = 254;
const int16 ILLEGAL_RESOURCE_INST_ID_TOKEN = -1;
const int32 MAX_SHORT_RESOURCE_INST_ID = 65535;
const int32 MAX_SHORT_TIMESTAMP = 65534;
const int32 INT_TIMESTAMP_TOKEN = 65535;
const int64 NANOS_PER_MILLI = 1000000;

/** @file
*/

namespace gemfire_statistics {
/**
 * Some of the classes which are used by the StatArchiveWriter Class
 * 1. StatDataOutput // Just a wrapper around DataOutput so that the number of
 *                 // bytes written is incremented automatically.
 * 2. ResourceType // The ResourceType and the ResourceInst class is used
 * 3. ResourceInst // by the StatArchiveWriter class for keeping track of
 *                 // of the Statistics object and the value of the
 *                 // descriptors in the previous sample run.
 */

class CPPCACHE_EXPORT StatDataOutput {
 public:
  StatDataOutput() : bytesWritten(0), m_fp(NULL), closed(false) {}
  StatDataOutput(std::string);
  ~StatDataOutput();
  /**
   * Returns the number of bytes written into the buffer so far.
   * This does not takes the compression into account.
   */
  int64 getBytesWritten();
  /**
   * Writes the buffer into the outfile.
   */
  void flush();
  /**
   * Writes the buffer into the outfile and sets bytesWritten to zero.
   */
  void resetBuffer();
  /**
   * Writes 8 bit integer value in the buffer.
   */
  void writeByte(int8 v);
  /**
   * Writes boolean value in the buffer.
   * Nothing but an int8.
   */
  void writeBoolean(int8 v);
  /**
   * Writes 16 bit integer value in the buffer.
   */
  void writeShort(int16 v);
  /**
   * Writes Long value 32 bit in the buffer.
   */
  void writeInt(int32 v);
  /**
   * Writes Double value 64 bit in the buffer.
   */
  void writeLong(int64 v);
  /**
   * Writes string value in the buffer.
   */
  void writeString(std::string v);
  /**
   * Writes wstring value in the buffer.
   */
  void writeUTF(std::wstring v);
  /**
   * This method is for the unit tests only for this class.
   */
  const uint8_t *getBuffer() { return dataBuffer.getBuffer(); }
  void close();

  void openFile(std::string, int64);

 private:
  int64 bytesWritten;
  DataOutput dataBuffer;
  std::string outFile;
  FILE *m_fp;
  bool closed;
  friend class StatArchiveWriter;
};

class CPPCACHE_EXPORT ResourceType : private NonCopyable,
                                     private NonAssignable {
 public:
  ResourceType(int32 id, StatisticsType *type);
  int32 getId();
  StatisticDescriptor **getStats();
  int32 getNumOfDescriptors();

 private:
  int32 id;
  StatisticDescriptor **stats;
  int32 numOfDescriptors;
};

/* adongre
 * CID 28735: Other violation (MISSING_COPY)
 * Class "gemfire_statistics::ResourceInst" owns resources
 * that are managed in its constructor and destructor but has no user-written
 * copy constructor.
 * CID 28721: Other violation (MISSING_ASSIGN)
 * Class "gemfire_statistics::ResourceInst" owns resources that are managed
 * in its constructor and destructor but has no user-written assignment
 * operator.
 *
 * FIX : Make the class NonCopyable
 */

class CPPCACHE_EXPORT ResourceInst : private NonCopyable,
                                     private NonAssignable {
 public:
  ResourceInst(int32 id, Statistics *, ResourceType *, StatDataOutput *);
  ~ResourceInst();
  int32 getId();
  Statistics *getResource();
  ResourceType *getType();
  int64 getStatValue(StatisticDescriptor *f);
  void writeSample();
  void writeStatValue(StatisticDescriptor *s, int64 v);
  void writeCompactValue(int64 v);
  void writeResourceInst(StatDataOutput *, int32);

 private:
  int32 id;
  Statistics *resource;
  ResourceType *type;
  /* This will contain the previous values of the descriptors */
  int64 *archivedStatValues;
  StatDataOutput *dataOut;
  /* Number of descriptors this resource instnace has */
  int32 numOfDescps;
  /* To know whether the instance has come for the first time */
  bool firstTime;
};

class HostStatSampler;
/**
 * @class StatArchiveWriter
 */

class CPPCACHE_EXPORT StatArchiveWriter {
 private:
  HostStatSampler *sampler;
  StatDataOutput *dataBuffer;
  int64 previousTimeStamp;
  int32 resourceTypeId;
  int32 resourceInstId;
  int32 statResourcesModCount;
  int64 bytesWrittenToFile;
  int64 m_samplesize;
  std::string archiveFile;
  std::map<Statistics *, ResourceInst *> resourceInstMap;
  std::map<StatisticsType *, ResourceType *> resourceTypeMap;

  /* private member functions */
  void allocateResourceInst(Statistics *r);
  void sampleResources();
  void resampleResources();
  void writeResourceInst(StatDataOutput *, int32);
  void writeTimeStamp(int64 timeStamp);
  void writeStatValue(StatisticDescriptor *f, int64 v, DataOutput dataOut);
  ResourceType *getResourceType(Statistics *);
  bool resourceInstMapHas(Statistics *sp);

 public:
  StatArchiveWriter(std::string archiveName, HostStatSampler *sampler);
  ~StatArchiveWriter();
  /**
   * Returns the number of bytes written so far to this archive.
   * This does not take compression into account.
   */
  int64 bytesWritten();
  /**
   * Archives a sample snapshot at the given timeStamp.
   * @param timeStamp a value obtained using NanoTimer::now.
   */
  void sample(int64 timeStamp);
  /**
   * Archives a sample snapshot at the current time.
   */
  void sample();
  /**
   * Closes the statArchiver by flushing its data to disk and closing its
   * output stream.
   */
  void close();

  /**
   * Closes the statArchiver by closing its output stream.
   */
  void closeFile();

  /**
   * Opens the statArchiver by opening the file provided as a parameter.
   * @param std::string filename.
   */
  void openFile(std::string);

  /**
   * Returns the size of number of bytes written so far to this archive.
   */
  int64 getSampleSize();

  /**
   * Flushes the contents of the dataBuffer to the archiveFile
   */
  void flush();
};
};      // namespace gemfire_statistics
#endif  // _GEMFIRE_STATISTICS_STATARCHIVERITER_HPP_
