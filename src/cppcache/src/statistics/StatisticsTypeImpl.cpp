/*=========================================================================
* Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/

#include "StatisticsTypeImpl.hpp"
#include "StatisticDescriptorImpl.hpp"
#include <string>
#include <ace/OS.h>
using namespace gemfire_statistics;

/**
* Gathers together a number of {@link StatisticDescriptor statistics}
* into one logical type.
*
*/

/**
* Creates a new <code>StatisticsType</code> with the given name,
* description, and statistics.
*
* @param name
*        The name of this statistics type (for example,
*        <code>"DatabaseStatistics"</code>)
* @param description
*        A description of this statistics type (for example,
*        "Information about the application's use of the
*        database").
* @param stats
*        Descriptions of the individual statistics grouped together
*        in this statistics type{@link StatisticDescriptor}.
*
* @throws NullPointerException
*         If either <code>name</code> or <code>stats</code> is
*         <code>null</code>.
*/

StatisticsTypeImpl::StatisticsTypeImpl(const char* nameArg,
                                       const char* descriptionArg,
                                       StatisticDescriptor** statsArg,
                                       int32 statsLengthArg) {
  if (strcmp(nameArg, "") == 0) {
    const char* s = "Cannot have a null statistics type name";
    throw NullPointerException(s);
  }
  if (statsArg == NULL) {
    const char* s = "Cannot have a null statistic descriptors";
    throw NullPointerException(s);
  }
  if (statsLengthArg > MAX_DESCRIPTORS_PER_TYPE) {
    char buffer[100];
    ACE_OS::snprintf(buffer, 100, "%d", statsLengthArg);
    std::string temp(buffer);
    std::string s = "The requested descriptor count " + temp +
                    " exceeds the maximum which is ";
    ACE_OS::snprintf(buffer, 100, "%d", MAX_DESCRIPTORS_PER_TYPE);
    std::string buf(buffer);
    s += buf + ".";
    throw IllegalArgumentException(s.c_str());
  }
  this->name = nameArg;
  this->description = descriptionArg;
  this->stats = statsArg;
  this->statsLength = statsLengthArg;
  int32 intCount = 0;
  int32 longCount = 0;
  int32 doubleCount = 0;
  for (int32 i = 0; i < this->statsLength; i++) {
    // Concrete class required to set the ids only.
    StatisticDescriptorImpl* sd =
        dynamic_cast<StatisticDescriptorImpl*>(stats[i]);
    /* adongre
     * CID 28694: Dereference after null check (FORWARD_NULL)Comparing "sd" to
     * null implies that "sd" might be null.
     *
     * FIX : Do not check null from the dynamic_cast
     *       catch the exception thrown by the dynamic_cast
     */

    if (sd != NULL) {
      if (sd->getTypeCode() == INT_TYPE) {
        sd->setId(intCount);
        intCount++;
      } else if (sd->getTypeCode() == LONG_TYPE) {
        sd->setId(longCount);
        longCount++;
      } else if (sd->getTypeCode() == DOUBLE_TYPE) {
        sd->setId(doubleCount);
        doubleCount++;
      }
      std::string str = stats[i]->getName();
      StatisticsDescMap::iterator iterFind = statsDescMap.find(str);
      if (iterFind != statsDescMap.end()) {
        std::string s = "Duplicate StatisticDescriptor named ";
        s += sd->getName();
        throw IllegalArgumentException(s.c_str());
      } else {
        // statsDescMap.insert(make_pair(stats[i]->getName(), stats[i]));
        statsDescMap.insert(
            StatisticsDescMap::value_type(stats[i]->getName(), stats[i]));
      }
    }
  }  // for
  this->intStatCount = intCount;
  this->longStatCount = longCount;
  this->doubleStatCount = doubleCount;
}

///////////////////////////////Dtor/////////////////////////

StatisticsTypeImpl::~StatisticsTypeImpl() {
  try {
    // Delete the descriptor pointers from the array
    for (int32 i = 0; i < statsLength; i++) {
      delete stats[i];
      stats[i] = NULL;
    }
    // same pointers are also stored in this map.
    // So, Set the pointers to null.
    StatisticsDescMap::iterator iterFind = statsDescMap.begin();
    while (iterFind != statsDescMap.end()) {
      (*iterFind).second = NULL;
      iterFind++;
    }
  } catch (...) {
  }
}

//////////////////////  StatisticsType Methods  //////////////////////

const char* StatisticsTypeImpl::getName() { return name.c_str(); }

const char* StatisticsTypeImpl::getDescription() { return description.c_str(); }

StatisticDescriptor** StatisticsTypeImpl::getStatistics() { return stats; }

int32 StatisticsTypeImpl::nameToId(const char* nameArg) {
  return nameToDescriptor(nameArg)->getId();
}

StatisticDescriptor* StatisticsTypeImpl::nameToDescriptor(const char* nameArg) {
  std::string str(nameArg);
  StatisticsDescMap::iterator iterFind = statsDescMap.find(str);
  if (iterFind == statsDescMap.end()) {
    std::string statName(nameArg);
    std::string s = "There is no statistic named " + statName +
                    " in this statistics instance ";
    LOGWARN("StatisticsTypeImpl::nameToDescriptor %s", s.c_str());
    throw IllegalArgumentException(s.c_str());
  } else {
    return (*iterFind).second;
  }
}
//////////////////////  Instance Methods  //////////////////////

/**
* Gets the number of statistics in this type that are ints.
*/
int32 StatisticsTypeImpl::getIntStatCount() { return intStatCount; }

/**
* Gets the number of statistics in this type that are longs.
*/
int32 StatisticsTypeImpl::getLongStatCount() { return longStatCount; }

/**
* Gets the number of statistics that are doubles.
*/
int32 StatisticsTypeImpl::getDoubleStatCount() { return doubleStatCount; }

/**
* Gets the total number of statistic descriptors.
*/
int32 StatisticsTypeImpl::getDescriptorsCount() { return statsLength; }
