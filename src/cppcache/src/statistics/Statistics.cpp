/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/statistics/Statistics.hpp>
using namespace gemfire_statistics;

void Statistics::close() {}

////////////////////////  accessor Methods  ///////////////////////

int32 Statistics::nameToId(const char* name) { return 0; }

StatisticDescriptor* Statistics::nameToDescriptor(const char* name) {
  return NULL;
}

int64 Statistics::getUniqueId() { return 0; }

StatisticsType* Statistics::getType() { return NULL; }

const char* Statistics::getTextId() { return ""; }

int64 Statistics::getNumericId() { return 0; }

bool Statistics::isAtomic() { return 0; }

bool Statistics::isShared() { return 0; }

bool Statistics::isClosed() { return 0; }

////////////////////////  set() Methods  ///////////////////////

void Statistics::setInt(int32 id, int32 value) {}

void Statistics::setInt(char* name, int32 value) {}

void Statistics::setInt(StatisticDescriptor* descriptor, int32 value) {}

void Statistics::setLong(int32 id, int64 value) {}

void Statistics::setLong(StatisticDescriptor* descriptor, int64 value) {}

void Statistics::setLong(char* name, int64 value) {}

void Statistics::setDouble(int32 id, double value) {}

void Statistics::setDouble(StatisticDescriptor* descriptor, double value) {}

void setDouble(char* name, double value) {}

///////////////////////  get() Methods  ///////////////////////

int32 Statistics::getInt(int32 id) { return 0; }

int32 Statistics::getInt(StatisticDescriptor* descriptor) { return 0; }

int32 Statistics::getInt(char* name) { return 0; }

int64 Statistics::getLong(int32 id) { return 0; }

int64 Statistics::getLong(StatisticDescriptor* descriptor) { return 0; }

int64 Statistics::getLong(char* name) { return 0; }

double Statistics::getDouble(int32 id) { return 0; }

double Statistics::getDouble(StatisticDescriptor* descriptor) { return 0; }

double Statistics::getDouble(char* name) { return 0; }

// Number Statistics::get(StatisticDescriptor* descriptor){ return }

// Number Statistics::get(char* name){ return }

/**
 * Returns the bits that represent the raw value of the described statistic.
 *
 * @param descriptor a statistic descriptor obtained with {@link
 * #nameToDescriptor}
 * or {@link StatisticsType#nameToDescriptor}.
 * @throws IllegalArgumentException
 *         If the described statistic does not exist
 */
//  int64 Statistics::getRawBits(StatisticDescriptor* descriptor){ return }

/**
 * Returns the bits that represent the raw value of the named statistic.
 *
 * @throws IllegalArgumentException
 *         If the named statistic does not exist
 //  int64  Statistics::getRawBits(char* name){ return }
 */

////////////////////////  inc() Methods  ////////////////////////

/**
 * Increments the value of the identified statistic of type <code>int</code>
 * by the given amount.
 *
 * @param id a statistic id obtained with {@link #nameToId}
 * or {@link StatisticsType#nameToId}.
 *
 * @return The value of the statistic after it has been incremented
 *
 * @throws IllegalArgumentException
 *         If the id is invalid.
 */
int32 Statistics::incInt(int32 id, int32 delta) { return 0; }

int32 Statistics::incInt(StatisticDescriptor* descriptor, int32 delta) {
  return 0;
}

int32 Statistics::incInt(char* name, int32 delta) { return 0; }

int64 Statistics::incLong(int32 id, int64 delta) { return 0; }

int64 Statistics::incLong(StatisticDescriptor* descriptor, int64 delta) {
  return 0;
}

int64 Statistics::incLong(char* name, int64 delta) { return 0; }

double Statistics::incDouble(int32 id, double delta) { return 0; }

double Statistics::incDouble(StatisticDescriptor* descriptor, double delta) {
  return 0;
}

double Statistics::incDouble(char* name, double delta) { return 0; }

Statistics::~Statistics() {}
