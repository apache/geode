/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __PerfStat_hpp__
#define __PerfStat_hpp__

#include <GemfireCppCache.hpp>
#include "fwklib/ClientTask.hpp"
#include "fwklib/FrameworkTest.hpp"
#include "fwklib/FwkLog.hpp"
#include <statistics/Statistics.hpp>
#include <statistics/StatisticsFactory.hpp>

using namespace gemfire_statistics;
namespace {
  const char PERF_CREATES[] = "creates";
  const char PERF_CREATE_TIME[] = "createTime";
  const char PERF_PUTS[]="puts";
  const char PERF_PUT_TIME[]="putTime";
  const char PERF_UPDATE_EVENTS[]="updateEvents";
  const char PERF_UPDATE_LATENCY[]="updateLatency";
  const char PERF_LATENCY_SPIKES[] = "latencySpikes";
  const char PERF_NEGATIVE_LATENCIES[] = "negativeLatencies";
  const char PERF_OPS[] = "operations";
  const char PERF_OP_TIME[] = "operationTime";
  const char PERF_CONNECTS[] = "connects";
  const char PERF_CONNECT_TIME[] = "connectTime";
  const char PERF_DISCONNECTS[] = "disconnects";
  const char PERF_DISCONNECT_TIME[] = "disconnectTime";
  const char PERF_GETS[] = "gets";
  const char PERF_GET_TIME[] = "getTime";
  const char PERF_QUERIES[] = "queries";
  const char PERF_QUERY_TIME[] = "queryTime";
  const char PERF_UPDATES[] = "updates";
  const char PERF_UPDATES_TIME[] = "updateTime";
  const char PERF_GETALL[] = "getAll";
  const char PERF_PUTALL[] = "putAll";
}
namespace gemfire {
 namespace testframework {
   namespace smokeperf {

class PerfStat {

  public:
    PerfStat(uint32_t threadID);
    virtual ~PerfStat();

    uint64_t startCreate() {
       return returnTime();
    }
    void endCreate(uint64_t start, bool isMainWorkload) {
      endCreate(start,1,isMainWorkload);
    }
    void endCreate(uint64_t start, int amount,bool isMainWorkload) {
      uint64_t elapsed = returnTime() - start;
      if(isMainWorkload){
        testStat->incInt(statsType->nameToId(PERF_OPS), amount);
        testStat->incLong(statsType->nameToId(PERF_OP_TIME), elapsed);
      }
      testStat->incInt(statsType->nameToId(PERF_CREATES), amount);
      testStat->incLong(statsType->nameToId(PERF_CREATE_TIME), elapsed);
    }
    uint64_t startPut() {
       return returnTime();
    }
    void endPut(uint64_t start, bool isMainWorkload) {
      endPut(start, 1, isMainWorkload);
    }
    void endPut(uint64_t start,int amount, bool isMainWorkload) {
      uint64_t elapsed = returnTime() - start;
      if(isMainWorkload){
        testStat->incInt(statsType->nameToId(PERF_OPS), amount);
        testStat->incLong(statsType->nameToId(PERF_OP_TIME), elapsed);
      }
      testStat->incInt(statsType->nameToId(PERF_PUTS), amount);
      testStat->incLong(statsType->nameToId(PERF_PUT_TIME), elapsed);
    }
    void endPutAll(uint64_t start,int amount, bool isMainWorkload) {
       testStat->incInt(statsType->nameToId(PERF_PUTALL),1);
       uint64_t elapsed = returnTime() - start;
       if(isMainWorkload){
         testStat->incInt(statsType->nameToId(PERF_OPS), amount);
         testStat->incLong(statsType->nameToId(PERF_OP_TIME), elapsed);
       }
       testStat->incInt(statsType->nameToId(PERF_PUTS), amount);
       testStat->incLong(statsType->nameToId(PERF_PUT_TIME), elapsed);
    }
    uint64_t startGet() {
       return returnTime();
    }
    void endGet(uint64_t start,bool isMainWorkload) {
      endGet(start,1,isMainWorkload);
    }
    void endGet(uint64_t start,int amount, bool isMainWorkload) {
      uint64_t elapsed = returnTime() - start;
      if(isMainWorkload){
        testStat->incInt(statsType->nameToId(PERF_OPS), amount);
        testStat->incLong(statsType->nameToId(PERF_OP_TIME), elapsed);
      }
      testStat->incInt(statsType->nameToId(PERF_GETS), amount);
      testStat->incLong(statsType->nameToId(PERF_GET_TIME), elapsed);
    }
    void endGetAll(uint64_t start,int amount, bool isMainWorkload) {
      testStat->incInt(statsType->nameToId(PERF_GETALL),1);
      uint64_t elapsed = returnTime() - start;
      if(isMainWorkload){
        testStat->incInt(statsType->nameToId(PERF_OPS), amount);
        testStat->incLong(statsType->nameToId(PERF_OP_TIME), elapsed);
      }
        testStat->incInt(statsType->nameToId(PERF_GETS), amount);
        testStat->incLong(statsType->nameToId(PERF_GET_TIME), elapsed);
    }
    inline void close(){
      testStat->close();
    }
    void incUpdateLatency(uint64_t amount) {
      uint64_t nonZeroAmount = amount;
      if (nonZeroAmount == 0) { // make non-zero to ensure non-flatline
        nonZeroAmount = 1; // nanosecond
      }
      testStat->incInt(statsType->nameToId(PERF_UPDATE_EVENTS), 1);
      testStat->incLong(statsType->nameToId(PERF_UPDATE_LATENCY), nonZeroAmount);
    }

    void incLatencySpikes(int amount) {
      testStat->incInt(statsType->nameToId(PERF_LATENCY_SPIKES), amount);
    }

    void incNegativeLatencies(int amount) {
     testStat->incInt(statsType->nameToId(PERF_NEGATIVE_LATENCIES), amount);
    }
    void setOps(int amount) {
      testStat->setInt(statsType->nameToId(PERF_OPS), amount);
    }
    void setOpTime(long amount) {
      testStat->setLong(statsType->nameToId(PERF_OP_TIME), amount);
    }
    uint64_t startConnect() {
       return returnTime();
    }
    void endConnect(uint64_t start,bool isMainWorkload) {
      uint64_t elapsed = returnTime() - start;
      if(isMainWorkload){
        testStat->incInt(statsType->nameToId(PERF_OPS), 1);
        testStat->incLong(statsType->nameToId(PERF_OP_TIME), elapsed);
      }
      testStat->incInt(statsType->nameToId(PERF_CONNECTS), 1);
      testStat->incLong(statsType->nameToId(PERF_CONNECT_TIME), elapsed);
    }
    int64 getConnectTime() {
       return testStat->getLong((char *)PERF_CONNECT_TIME);
     }
    uint64_t startQuery() {
      return returnTime();
    }
    void endQuery(uint64_t start,bool isMainWorkload) {
      endQuery(start,1,isMainWorkload);
    }
    void endQuery(uint64_t start,int amount, bool isMainWorkload) {
      uint64_t elapsed = returnTime() - start;
      if(isMainWorkload){
        testStat->incInt(statsType->nameToId(PERF_OPS), amount);
        testStat->incLong(statsType->nameToId(PERF_OP_TIME), elapsed);
      }
        testStat->incInt(statsType->nameToId(PERF_QUERIES), amount);
        testStat->incLong(statsType->nameToId(PERF_QUERY_TIME), elapsed);
      }
    void incUpdateEvents(int amount) {
      testStat->incInt(statsType->nameToId(PERF_UPDATE_EVENTS), amount);
    }
    int getOps() {
       return testStat->getInt((char *)PERF_OPS);
     }
    /**
     * @return the timestamp that marks the start of the update
     */
    uint64_t startUpdate() {
      return returnTime();
    }
    void endUpdate(uint64_t start, bool isMainWorkload) {
      endUpdate(start, 1, isMainWorkload);
    }
    void endUpdate(uint64_t start, int amount, bool isMainWorkload) {
      uint64_t elapsed = returnTime() - start;
      if (isMainWorkload) {
        testStat->incInt(statsType->nameToId(PERF_OPS), amount);
        testStat->incLong(statsType->nameToId(PERF_OP_TIME), elapsed);
      }
      testStat->incInt(statsType->nameToId(PERF_UPDATES), amount);
      testStat->incLong(statsType->nameToId(PERF_UPDATES_TIME), elapsed);
    }

  private:
    gemfire_statistics::Statistics* testStat;
    StatisticsType * statsType;
    inline uint64_t returnTime()
    {
      ACE_Time_Value startTime;
      startTime = ACE_OS::gettimeofday();
      ACE_UINT64 tusec;
      startTime.to_usec(tusec);
      return tusec*1000;
    }
};

class PerfStatType {

private:
  static int8 instanceFlag;
  static PerfStatType  *single;

public:
  static PerfStatType * getInstance();

  StatisticsType * getStatType();

  static void clean();


private:
  PerfStatType();
  StatisticDescriptor* statDes[22];
};

    }
  }
}
#endif
