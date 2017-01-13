/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>

#include <ace/Atomic_Op_T.h>
#include <ace/Recursive_Thread_Mutex.h>
#include <ace/OS_NS_stdio.h>
#include "AtomicStatisticsImpl.hpp"
#include "StatisticsTypeImpl.hpp"
#include "StatisticDescriptorImpl.hpp"

using namespace gemfire_statistics;
/**
 * An implementation of {@link Statistics} that stores its statistics
 * in local  memory and supports atomic operations.
 *
 */
//////////////////////  Static Methods  //////////////////////

int64 AtomicStatisticsImpl::calcNumericId(StatisticsFactory* system,
                                          int64 userValue) {
  int64 result;
  if (userValue != 0) {
    result = userValue;
  } else {
    result = system->getId();
  }
  return result;
}

const char* AtomicStatisticsImpl::calcTextId(StatisticsFactory* system,
                                             const char* userValue) {
  if (userValue != NULL && strcmp(userValue, "") != 0) {
    return userValue;
  } else {
    if (system != NULL) {
      return system->getName();
    } else {
      return "";
    }
  }
}

///////////////////////  Constructors  ///////////////////////

/**
 * Creates a new statistics instance of the given type
 *
 * @param type
 *        A description of the statistics
 * @param textId
 *        Text that identifies this statistic when it is monitored
 * @param numericId
 *        A number that displayed when this statistic is monitored
 * @param uniqueId
 *        A number that uniquely identifies this instance
 * @param system
 *        The distributed system that determines whether or not these
 *        statistics are stored (and collected) in local memory
 */
AtomicStatisticsImpl::AtomicStatisticsImpl(StatisticsType* typeArg,
                                           const char* textIdArg,
                                           int64 numericIdArg,
                                           int64 uniqueIdArg,
                                           StatisticsFactory* system)

{
  try {
    this->textId = calcTextId(system, textIdArg);
    std::string tmp(this->textId);
    textIdStr = tmp;
    this->numericId = calcNumericId(system, numericIdArg);
    this->uniqueId = uniqueIdArg;
    this->closed = false;
    this->statsType = dynamic_cast<StatisticsTypeImpl*>(typeArg);
    GF_D_ASSERT(this->statsType != NULL);
    int32 intCount = statsType->getIntStatCount();
    int32 longCount = statsType->getLongStatCount();
    int32 doubleCount = statsType->getDoubleStatCount();

    if (intCount > 0) {
      intStorage =
          new ACE_Atomic_Op<ACE_Recursive_Thread_Mutex, int32>[ intCount ];
      for (int32 i = 0; i < intCount; i++) {
        intStorage[i] = 0;  // Un-initialized state
      }

    } else {
      intStorage = NULL;
    }
    if (longCount > 0) {
      longStorage =
          new ACE_Atomic_Op<ACE_Recursive_Thread_Mutex, int64>[ longCount ];
      for (int32 i = 0; i < longCount; i++) {
        longStorage[i] = 0;  // Un-initialized state
      }

    } else {
      longStorage = NULL;
    }
    if (doubleCount > 0) {
      doubleStorage =
          new ACE_Atomic_Op<ACE_Recursive_Thread_Mutex, double>[ doubleCount ];
      for (int32 i = 0; i < doubleCount; i++) {
        doubleStorage[i] = 0;  // Un-initialized state
      }
    } else {
      doubleStorage = NULL;
    }
  } catch (...) {
    statsType = NULL;  // Will be deleted by the class who calls this ctor
  }
}

AtomicStatisticsImpl::~AtomicStatisticsImpl() {
  try {
    statsType = NULL;
    if (intStorage != NULL) {
      delete[] intStorage;
      intStorage = NULL;
    }
    if (longStorage != NULL) {
      delete[] longStorage;
      longStorage = NULL;
    }
    if (doubleStorage != NULL) {
      delete[] doubleStorage;
      doubleStorage = NULL;
    }
  } catch (...) {
  }
}

//////////////////////  Instance Methods  //////////////////////

bool AtomicStatisticsImpl::isShared() { return false; }

bool AtomicStatisticsImpl::isAtomic() {
  return true;  // will always be true for this class
}

void AtomicStatisticsImpl::close() {
  // Just mark closed,Will be actually deleted when token written in archive
  // file.
  closed = true;
}

////////////////////////  store() Methods  ///////////////////////

void AtomicStatisticsImpl::_setInt(int32 offset, int32 value) {
  if (offset >= statsType->getIntStatCount()) {
    char s[128] = {'\0'};
    ACE_OS::snprintf(
        s, 128, "setInt:The id (%d) of the Statistic Descriptor is not valid ",
        offset);
    throw IllegalArgumentException(s);
  }
  intStorage[offset] = value;
}

void AtomicStatisticsImpl::_setLong(int32 offset, int64 value) {
  if (offset >= statsType->getLongStatCount()) {
    char s[128] = {'\0'};
    /* adongre  - Coverity II
     * CID 29273: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
     * "sprintf" can cause a
     * buffer overflow when done incorrectly. Because sprintf() assumes an
     * arbitrarily long string,
     * callers must be careful not to overflow the actual space of the
     * destination.
     * Use snprintf() instead, or correct precision specifiers.
     * Fix : using ACE_OS::snprintf
     */
    // sprintf(s, "setLong:The id (%d) of the Statistic Descriptor is not valid
    // ", offset);
    ACE_OS::snprintf(
        s, 128, "setLong:The id (%d) of the Statistic Descriptor is not valid ",
        offset);
    throw IllegalArgumentException(s);
  }

  longStorage[offset] = value;
}

void AtomicStatisticsImpl::_setDouble(int32 offset, double value) {
  if (offset >= statsType->getDoubleStatCount()) {
    char s[128] = {'\0'};
    ACE_OS::snprintf(
        s, 128,
        "setDouble:The id (%d) of the Statistic Descriptor is not valid ",
        offset);
    throw IllegalArgumentException(s);
  }

  doubleStorage[offset] = value;
}

///////////////////////  get() Methods  ///////////////////////

int32 AtomicStatisticsImpl::_getInt(int32 offset) {
  if (offset >= statsType->getIntStatCount()) {
    char s[128] = {'\0'};
    ACE_OS::snprintf(
        s, 128, "getInt:The id (%d) of the Statistic Descriptor is not valid ",
        offset);
    throw IllegalArgumentException(s);
  }

  return intStorage[offset].value();
}

int64 AtomicStatisticsImpl::_getLong(int32 offset) {
  if (offset >= statsType->getLongStatCount()) {
    char s[128] = {'\0'};
    ACE_OS::snprintf(
        s, 128, "getLong:The id (%d) of the Statistic Descriptor is not valid ",
        offset);
    throw IllegalArgumentException(s);
  }
  return longStorage[offset].value();
}

double AtomicStatisticsImpl::_getDouble(int32 offset) {
  if (offset >= statsType->getDoubleStatCount()) {
    char s[128] = {'\0'};
    ACE_OS::snprintf(
        s, 128,
        "getDouble:The id (%d) of the Statistic Descriptor is not valid ",
        offset);
    throw IllegalArgumentException(s);
  }
  return doubleStorage[offset].value();
}

int64 AtomicStatisticsImpl::_getRawBits(StatisticDescriptor* statDscp) {
  StatisticDescriptorImpl* stat =
      dynamic_cast<StatisticDescriptorImpl*>(statDscp);
  switch (stat->getTypeCode()) {
    case INT_TYPE:
      return getInt(stat->getId());

    case LONG_TYPE:
      return _getLong(stat->getId());

    case DOUBLE_TYPE: {
      double value = _getDouble(stat->getId());
      int64* temp = reinterpret_cast<int64*>(&value);
      return *temp;
    }
    default:
      return 0;
      /*throw RuntimeException("unexpected stat descriptor type code: " +
                                stat->getTypeCode());*/
  }
}

int64 AtomicStatisticsImpl::getRawBits(char* name) {
  return getRawBits(nameToDescriptor(name));
}

int64 AtomicStatisticsImpl::getRawBits(StatisticDescriptor* descriptor) {
  if (isOpen()) {
    return _getRawBits(descriptor);
  } else {
    return 0;
  }
}

////////////////////////  inc() Methods  ////////////////////////

int32 AtomicStatisticsImpl::_incInt(int32 offset, int32 delta) {
  if (offset >= statsType->getIntStatCount()) {
    char s[128] = {'\0'};
    ACE_OS::snprintf(
        s, 128, "incInt:The id (%d) of the Statistic Descriptor is not valid ",
        offset);
    throw IllegalArgumentException(s);
  }

  return (intStorage[offset] += delta);
}

int64 AtomicStatisticsImpl::_incLong(int32 offset, int64 delta) {
  if (offset >= statsType->getLongStatCount()) {
    char s[128] = {'\0'};
    /* adongre  - Coverity II
     * CID 29273: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
     * "sprintf" can cause a
     * buffer overflow when done incorrectly. Because sprintf() assumes an
     * arbitrarily long string,
     * callers must be careful not to overflow the actual space of the
     * destination.
     * Use snprintf() instead, or correct precision specifiers.
     * Fix : using ACE_OS::snprintf
     */
    // sprintf(s, "incLong:The id (%d) of the Statistic Descriptor is not valid
    // ", offset);

    ACE_OS::snprintf(
        s, 128, "incLong:The id (%d) of the Statistic Descriptor is not valid ",
        offset);
    throw IllegalArgumentException(s);
  }

  return (longStorage[offset] += delta);
}

double AtomicStatisticsImpl::_incDouble(int32 offset, double delta) {
  if (offset >= statsType->getDoubleStatCount()) {
    char s[128] = {'\0'};
    ACE_OS::snprintf(
        s, 128,
        "incDouble:The id (%d) of the Statistic Descriptor is not valid ",
        offset);
    throw IllegalArgumentException(s);
  }

  return (doubleStorage[offset] += delta);
}

/**************************Base class methods ********************/

//////////////////////  Instance Methods  //////////////////////

int32 AtomicStatisticsImpl::nameToId(const char* name) {
  return statsType->nameToId(name);
}

StatisticDescriptor* AtomicStatisticsImpl::nameToDescriptor(const char* name) {
  return statsType->nameToDescriptor(name);
}

bool AtomicStatisticsImpl::isClosed() { return closed; }

bool AtomicStatisticsImpl::isOpen() { return !closed; }

////////////////////////  attribute Methods  ///////////////////////

StatisticsType* AtomicStatisticsImpl::getType() { return statsType; }

const char* AtomicStatisticsImpl::getTextId() { return textIdStr.c_str(); }

int64 AtomicStatisticsImpl::getNumericId() { return numericId; }

/**
 * Gets the unique id for this resource
 */
int64 AtomicStatisticsImpl::getUniqueId() { return uniqueId; }

////////////////////////  set() Methods  ///////////////////////

void AtomicStatisticsImpl::setInt(char* name, int32 value) {
  int32 id = getIntId(nameToDescriptor(name));
  setInt(id, value);
}

void AtomicStatisticsImpl::setInt(StatisticDescriptor* descriptor,
                                  int32 value) {
  int32 id = getIntId(descriptor);
  setInt(id, value);
}

void AtomicStatisticsImpl::setInt(int32 id, int32 value) {
  if (isOpen()) {
    _setInt(id, value);
  }
}
////////////////////////////////LONG METHODS/////////////////////////////

void AtomicStatisticsImpl::setLong(char* name, int64 value) {
  setLong(nameToDescriptor(name), value);
}

void AtomicStatisticsImpl::setLong(StatisticDescriptor* descriptor,
                                   int64 value) {
  setLong(getLongId(descriptor), value);
}

void AtomicStatisticsImpl::setLong(int32 id, int64 value) {
  if (isOpen()) {
    _setLong(id, value);
  }
}
////////////////////////////////////////DOUBLE METHODS////////////////////

void AtomicStatisticsImpl::setDouble(char* name, double value) {
  setDouble(nameToDescriptor(name), value);
}

void AtomicStatisticsImpl::setDouble(StatisticDescriptor* descriptor,
                                     double value) {
  setDouble(getDoubleId(descriptor), value);
}

void AtomicStatisticsImpl::setDouble(int32 id, double value) {
  if (isOpen()) {
    _setDouble(id, value);
  }
}

int32 AtomicStatisticsImpl::getInt(char* name) {
  int32 id = getIntId(nameToDescriptor(name));
  return getInt(id);
}

int32 AtomicStatisticsImpl::getInt(StatisticDescriptor* descriptor) {
  int32 id = getIntId(descriptor);
  return getInt(id);
}

int32 AtomicStatisticsImpl::getInt(int32 id) {
  if (isOpen()) {
    return _getInt(id);
  } else {
    return 0;
  }
}

int64 AtomicStatisticsImpl::getLong(char* name) {
  return getLong(nameToDescriptor(name));
}

int64 AtomicStatisticsImpl::getLong(StatisticDescriptor* descriptor) {
  return getLong(getLongId(descriptor));
}

int64 AtomicStatisticsImpl::getLong(int32 id) {
  if (isOpen()) {
    return _getLong(id);
  } else {
    return 0;
  }
}

double AtomicStatisticsImpl::getDouble(char* name) {
  return getDouble(nameToDescriptor(name));
}

double AtomicStatisticsImpl::getDouble(StatisticDescriptor* descriptor) {
  return getDouble(getDoubleId(descriptor));
}

double AtomicStatisticsImpl::getDouble(int32 id) {
  if (isOpen()) {
    return _getDouble(id);
  } else {
    return 0;
  }
}

/*
 *Increment the value of the int32 decriptor by delta
 */
int32 AtomicStatisticsImpl::incInt(char* name, int32 delta) {
  int32 id = getIntId(nameToDescriptor(name));
  return incInt(id, delta);
}

int32 AtomicStatisticsImpl::incInt(StatisticDescriptor* descriptor,
                                   int32 delta) {
  int32 id = getIntId(descriptor);
  return incInt(id, delta);
}

int32 AtomicStatisticsImpl::incInt(int32 id, int32 delta) {
  if (isOpen()) {
    return _incInt(id, delta);
  } else {
    return 0;
  }
}

/*
 *Increment the value of the int64 decriptor by delta
 */

int64 AtomicStatisticsImpl::incLong(char* name, int64 delta) {
  return incLong(nameToDescriptor(name), delta);
}

int64 AtomicStatisticsImpl::incLong(StatisticDescriptor* descriptor,
                                    int64 delta) {
  return incLong(getLongId(descriptor), delta);
}

int64 AtomicStatisticsImpl::incLong(int32 id, int64 delta) {
  if (isOpen()) {
    return _incLong(id, delta);
  } else {
    return 0;
  }
}

/*
 *Increment the value of the double decriptor by delta
 */

double AtomicStatisticsImpl::incDouble(char* name, double delta) {
  return incDouble(nameToDescriptor(name), delta);
}

double AtomicStatisticsImpl::incDouble(StatisticDescriptor* descriptor,
                                       double delta) {
  return incDouble(getDoubleId(descriptor), delta);
}

double AtomicStatisticsImpl::incDouble(int32 id, double delta) {
  if (isOpen()) {
    return _incDouble(id, delta);
  } else {
    return 0;
  }
}

int32 AtomicStatisticsImpl::getIntId(StatisticDescriptor* descriptor) {
  StatisticDescriptorImpl* realDescriptor =
      dynamic_cast<StatisticDescriptorImpl*>(descriptor);
  return realDescriptor->checkInt();
}

int32 AtomicStatisticsImpl::getLongId(StatisticDescriptor* descriptor) {
  StatisticDescriptorImpl* realDescriptor =
      dynamic_cast<StatisticDescriptorImpl*>(descriptor);
  return realDescriptor->checkLong();
}

int32 AtomicStatisticsImpl::getDoubleId(StatisticDescriptor* descriptor) {
  StatisticDescriptorImpl* realDescriptor =
      dynamic_cast<StatisticDescriptorImpl*>(descriptor);
  return realDescriptor->checkDouble();
}
