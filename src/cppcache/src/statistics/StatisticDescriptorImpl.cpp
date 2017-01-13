/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
*/

#include "StatisticDescriptorImpl.hpp"

#include <ace/OS.h>

using namespace gemfire_statistics;

const char* StatisticDescriptorImpl::IntTypeName = "int_t";
const char* StatisticDescriptorImpl::LongTypeName = "Long";
const char* StatisticDescriptorImpl::DoubleTypeName = "Float";

/**
 * Describes an individual statistic whose value is updated by an
 * application and may be archived by GemFire.  These descriptions are
 * gathered together in a {@link StatisticsType}.
 * <P>
 * To get an instance of this interface use an instance of
 * {@link StatisticsFactory}.
 * <P>
 * StatisticDescriptors are naturally ordered by their name.
 *
 */

StatisticDescriptorImpl::StatisticDescriptorImpl(const char* statName,
                                                 FieldType statDescriptorType,
                                                 const char* statDescription,
                                                 const char* statUnit,
                                                 int8 statIsStatCounter,
                                                 int8 statIsStatLargerBetter) {
  name = statName;
  descriptorType = statDescriptorType;
  if (strcmp(statDescription, "") == 0) {
    description = "";
  } else {
    description = statDescription;
  }
  if (strcmp(statUnit, "") == 0) {
    unit = "";
  } else {
    unit = statUnit;
  }
  isStatCounter = statIsStatCounter;
  isStatLargerBetter = statIsStatLargerBetter;
  id = -1;
}

StatisticDescriptorImpl::~StatisticDescriptorImpl() {}

/////////////////////// Create functions ///////////////////////////////////

StatisticDescriptor* StatisticDescriptorImpl::createIntCounter(
    const char* statName, const char* description, const char* units,
    int8 isLargerBetter) throw(OutOfMemoryException) {
  FieldType fieldType = INT_TYPE;
  StatisticDescriptorImpl* sdi = new StatisticDescriptorImpl(
      statName, fieldType, description, units, true, isLargerBetter);
  if (sdi == NULL) {
    throw OutOfMemoryException(
        "StatisticDescriptorImpl::createIntCounter: out of memory");
  }
  return sdi;
}

StatisticDescriptor* StatisticDescriptorImpl::createLongCounter(
    const char* name, const char* description, const char* units,
    int8 isLargerBetter) throw(OutOfMemoryException) {
  FieldType fieldType = LONG_TYPE;
  StatisticDescriptorImpl* sdi = new StatisticDescriptorImpl(
      name, fieldType, description, units, true, isLargerBetter);
  if (sdi == NULL) {
    throw OutOfMemoryException(
        "StatisticDescriptorImpl::createLongCounter: out of memory");
  }
  return sdi;
}

StatisticDescriptor* StatisticDescriptorImpl::createDoubleCounter(
    const char* name, const char* description, const char* units,
    int8 isLargerBetter) throw(OutOfMemoryException) {
  FieldType fieldType = DOUBLE_TYPE;
  StatisticDescriptorImpl* sdi = new StatisticDescriptorImpl(
      name, fieldType, description, units, true, isLargerBetter);
  if (sdi == NULL) {
    throw OutOfMemoryException(
        "StatisticDescriptorImpl::createDoubleCounter: out of memory");
  }
  return sdi;
}

StatisticDescriptor* StatisticDescriptorImpl::createIntGauge(
    const char* name, const char* description, const char* units,
    int8 isLargerBetter) throw(OutOfMemoryException) {
  FieldType fieldType = INT_TYPE;
  StatisticDescriptorImpl* sdi = new StatisticDescriptorImpl(
      name, fieldType, description, units, false, isLargerBetter);
  if (sdi == NULL) {
    throw OutOfMemoryException(
        "StatisticDescriptorImpl::createIntGauge: out of memory");
  }
  return sdi;
}

StatisticDescriptor* StatisticDescriptorImpl::createLongGauge(
    const char* name, const char* description, const char* units,
    int8 isLargerBetter) throw(OutOfMemoryException) {
  FieldType fieldType = LONG_TYPE;
  StatisticDescriptorImpl* sdi = new StatisticDescriptorImpl(
      name, fieldType, description, units, false, isLargerBetter);

  if (sdi == NULL) {
    throw OutOfMemoryException(
        "StatisticDescriptorImpl::createLongGauge: out of memory");
  }
  return sdi;
}

StatisticDescriptor* StatisticDescriptorImpl::createDoubleGauge(
    const char* name, const char* description, const char* units,
    int8 isLargerBetter) throw(OutOfMemoryException) {
  FieldType fieldType = DOUBLE_TYPE;
  StatisticDescriptorImpl* sdi = new StatisticDescriptorImpl(
      name, fieldType, description, units, false, isLargerBetter);
  if (sdi == NULL) {
    throw OutOfMemoryException(
        "StatisticDescriptorImpl::createDoubleGauge: out of memory");
  }
  return sdi;
}

/////////////////////// StatisticDescriptor(Base class)
/// Methods///////////////////////////

const char* StatisticDescriptorImpl::getName() { return name.c_str(); }

const char* StatisticDescriptorImpl::getDescription() {
  return description.c_str();
}

int32 StatisticDescriptorImpl::getStorageBits() {
  return getTypeCodeBits(descriptorType);
}

const char* StatisticDescriptorImpl::getTypeCodeName(FieldType code) throw(
    IllegalArgumentException) {
  switch (code) {
    case INT_TYPE:
      return IntTypeName;
    case LONG_TYPE:
      return LongTypeName;
    case DOUBLE_TYPE:
      return DoubleTypeName;
    default: {
      char buf[20];
      ACE_OS::snprintf(buf, 20, "%d", code);
      std::string temp(buf);
      std::string s = "Unknown type code:" + temp;
      throw IllegalArgumentException(s.c_str());
    }
  }
}

/**
 * Returns the number of bits needed to represent a value of the given type
 * @throws IllegalArgumentException/
 *         <code>code</code> is an unknown type
 */
int32 StatisticDescriptorImpl::getTypeCodeBits(FieldType code) throw(
    IllegalArgumentException) {
  switch (code) {
    case INT_TYPE:
      return 32;
    case LONG_TYPE:
      return 64;
    case DOUBLE_TYPE:
      return 64;
    default:
      std::string temp(getTypeCodeName(code));
      std::string s = "Unknown type code: " + temp;
      throw IllegalArgumentException(s.c_str());
  }
}

int8 StatisticDescriptorImpl::isCounter() { return isStatCounter; }

int8 StatisticDescriptorImpl::isLargerBetter() { return isStatLargerBetter; }

const char* StatisticDescriptorImpl::getUnit() { return unit.c_str(); }

/* adongre
 * CID 28912: Uncaught exception (UNCAUGHT_EXCEPT)An exception of type
 * "gemfire::IllegalStateException *"
 * is thrown but the throw list "throw (gemfire::IllegalArgumentException)"
 * doesn't allow it to be thrown.
 * This will cause a call to unexpected() which usually calls terminate().
 */
// int32 StatisticDescriptorImpl::getId() throw(IllegalArgumentException)
int32 StatisticDescriptorImpl::getId() throw(IllegalStateException) {
  if (id == -1) {
    std::string s = "The id has not been initialized yet.";
    throw IllegalStateException(s.c_str());
  }
  return id;
}

///////////////////////// Instance Methods///////////////// ////////////////////

FieldType StatisticDescriptorImpl::getTypeCode() { return descriptorType; }

void StatisticDescriptorImpl::setId(int32 statId) { id = statId; }

int32 StatisticDescriptorImpl::checkInt() throw(IllegalArgumentException) {
  if (descriptorType != INT_TYPE) {
    std::string sb;
    std::string typeCode(getTypeCodeName(getTypeCode()));
    sb = "The statistic " + name;
    sb += " is of type " + typeCode;
    sb += " and it was expected to be an int";
    throw IllegalArgumentException(sb.c_str());
  }
  return id;
}
int32 StatisticDescriptorImpl::checkLong() throw(IllegalArgumentException) {
  if (descriptorType != LONG_TYPE) {
    std::string sb;
    std::string typeCode(getTypeCodeName(getTypeCode()));
    sb = "The statistic " + name;
    sb += " is of type " + typeCode;
    sb += " and it was expected to be an long";
    throw IllegalArgumentException(sb.c_str());
  }
  return id;
}

int32 StatisticDescriptorImpl::checkDouble() throw(IllegalArgumentException) {
  if (descriptorType != DOUBLE_TYPE) {
    std::string sb;
    std::string typeCode(getTypeCodeName(getTypeCode()));

    sb = "The statistic " + name;
    sb += " is of type " + typeCode;
    sb += " and it was expected to be an long";
    throw IllegalArgumentException(sb.c_str());
  }
  return id;
}
