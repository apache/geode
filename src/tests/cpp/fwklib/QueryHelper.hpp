/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef TEST_QUERYHELPER_HPP
#define TEST_QUERYHELPER_HPP

#include <gfcpp/GemfireCppCache.hpp>
#include <stdlib.h>
#include <gfcpp/SystemProperties.hpp>
#include <ace/OS.h>

#include "DistributedSystemImpl.hpp"

#include "../testobject/Portfolio.hpp"
#include "../testobject/Position.hpp"
#include "../testobject/PdxType.hpp"
#include "../testobject/PortfolioPdx.hpp"
#include "../testobject/PositionPdx.hpp"
#include "../pdxautoserializerclass/PortfolioPdx.hpp"
#include "../pdxautoserializerclass/PositionPdx.hpp"
#include "fwklib/FwkLog.hpp"

#include <gfcpp/ResultSet.hpp>
#include <gfcpp/StructSet.hpp>

#ifndef ROOT_SCOPE
#define ROOT_SCOPE LOCAL
#endif

using namespace gemfire;
using namespace PdxTests;
using namespace AutoPdxTests;

namespace testData {

const int RS_ARRAY_SIZE = 41;
const int SS_ARRAY_SIZE = 35;
const int CQRS_ARRAY_SIZE = 24;
const int RSP_ARRAY_SIZE = 13;
const int SSP_ARRAY_SIZE = 17;

const int MAX_QRY_LENGTH = 512;
enum queryCategory {
  singleRegion = 1,
  multiRegion,
  operators,
  constants,
  functions,
  collectionOps,
  keywords,
  regionInterface,
  nestedQueries,
  importAndSelect,
  canonicalization,
  unsupported,
  queryAndIndexing,
  misc,
  qcEnd /*to mark the end of enum*/
} qcType;

class QueryStrings {
 public:
  QueryStrings(queryCategory pcategory, const char* pquery,
               bool pisLargeResultset = false) {
    int32_t querylen = static_cast<int32_t>(strlen(pquery));
    if (querylen < MAX_QRY_LENGTH) memcpy(_query, pquery, querylen);
    memset(&_query[querylen], '\0', 1);
    category = pcategory;
    haveLargeResultset = pisLargeResultset;
  }

  static int RSsize() { return RS_ARRAY_SIZE; };
  static int SSsize() { return SS_ARRAY_SIZE; };
  static int CQRSsize() { return CQRS_ARRAY_SIZE; };
  static int RSPsize() { return RSP_ARRAY_SIZE; };
  static int SSPsize() { return SSP_ARRAY_SIZE; };

  const char* query() const { return _query; };

 public:
  char _query[MAX_QRY_LENGTH];
  queryCategory category;
  bool haveLargeResultset;

 private:
  QueryStrings();
  QueryStrings* operator=(QueryStrings&);
};

#define QRY(category, query) QueryStrings(category, query)

// for large resultset
#define LQRY(category, query) QueryStrings(category, query, true)

const QueryStrings resultsetQueries[RS_ARRAY_SIZE] = {
    // 0 idx
    QRY(singleRegion, "select distinct * from /Portfolios port"),
    QRY(singleRegion, "select distinct pkid from /Portfolios"),
    LQRY(singleRegion,
         "select distinct port from /Portfolios port, "
         "port.Positions.values"),
    QRY(singleRegion,
        "select distinct pf from /Portfolios pf , pf.positions.values "
        "pos where pos.getSecId = 'IBM' and NOT pf.isActive")

    // 4 idx
    ,
    LQRY(multiRegion,
         "Select distinct pf from /Portfolios pf, /Portfolios2, "
         "/Portfolios3 where pf.isActive = TRUE"),
    LQRY(multiRegion,
         "select distinct pos from /Portfolios, /Portfolios/Positions "
         "pos"),
    LQRY(multiRegion,
         "select distinct port from /Portfolios port , /Positions pos "
         "where pos.getSecId in (select distinct pos.getSecId from "
         "port.positions.values pos)")

    // 6 idx
    ,
    QRY(operators, "select distinct * from /Portfolios where ID =  2"),
    QRY(operators, "select distinct * from /Portfolios where ID != 2")

    // 9 idx
    ,
    QRY(constants, "select distinct * from /Portfolios where ID = NULL"),
    QRY(constants, "select distinct * from /Portfolios where ID = UNDEFINED"),
    QRY(constants, "select distinct * from /Portfolios where NULL"),
    QRY(constants, "select distinct * from /Portfolios where UNDEFINED"),
    QRY(constants, "select distinct * from /Portfolios where TRUE"),
    QRY(constants, "select distinct * from /Portfolios where 1=1"),
    QRY(constants, "select distinct * from /Portfolios where 'a'<>'a'"),
    QRY(constants,
        "select distinct port from /Portfolios port, "
        "port.Positions.values where FALSE"),
    QRY(constants,
        "select distinct port from /Portfolios port, "
        "port.Positions.values where UNDEFINED = 1"),
    QRY(constants,
        "select distinct port from /Portfolios port where "
        "IS_UNDEFINED(UNDEFINED)")

    // 19 idx
    ,
    QRY(functions,
        "select distinct * from /Portfolios port where "
        "IS_DEFINED(TRUE)"),
    QRY(functions,
        "select distinct * from /Portfolios port where "
        "IS_UNDEFINED(ID)"),
    LQRY(nestedQueries,
         "select distinct this.value from /Portfolios.entrySet this "
         "where element(select distinct * from /Portfolios.keySet p_k "
         "where p_k = this.key ) = 'port1-3' and this.value.status = "
         "'inactive'"),
    QRY(functions, "select distinct nvl(NULL, 'foundNull') from /Portfolios")
    //, QRY(functions   , "select distinct nvl('notNull', 'foundNull')
    //= 'notNull' from /Portfolios" )
    ,
    QRY(functions,
        "select distinct nvl('notNull', 'foundNull') from /Portfolios"),
    QRY(functions,
        "select distinct * from /Portfolios pf where "
        "nvl(pf.position2,'foundNull') = 'foundNull'"),
    QRY(functions,
        "select distinct nvl(pf.position2, 'inProjection') from "
        "/Portfolios pf where nvl(pf.position2,'foundNull') = "
        "'foundNull'")  // constant

    // 26 idx
    ,
    QRY(regionInterface,
        "select distinct * from /Portfolios.keys where toString = "
        "'port1-1'")  // constant
    ,
    QRY(regionInterface,
        "select distinct * from /Portfolios.values where ID = 1"),
    QRY(regionInterface,
        "select distinct value from /Portfolios.entries where value.ID "
        "= 1 and key = 'port1-1'")  // constant
    ,
    LQRY(regionInterface,
         "select distinct * from /Portfolios.keySet where toString = "
         "'port1-1'")  // constant
    ,
    LQRY(regionInterface,
         "select distinct key from /Portfolios.entrySet , "
         "value.positions.values   where value.ID = 1 and key = "
         "'port1-1'")  // constant

    // 31 idx
    ,
    QRY(functions,
        "select distinct * from /Portfolios WHERE NOT(SELECT DISTINCT "
        "* FROM positions.values p WHERE p.secId = 'IBM').isEmpty"),
    LQRY(nestedQueries,
         "select distinct x.value from /Portfolios.entrySet x where "
         "x.value.status = ELEMENT(select distinct p.value from "
         "/Portfolios.entrySet p where x.key = p.key).status"),
    LQRY(nestedQueries,
         "select distinct * from /Portfolios x where status = "
         "ELEMENT(select distinct p_k.value from /Portfolios.entrySet "
         "p_k where p_k.key = 'port1-1').status"),
    LQRY(nestedQueries,
         "select distinct p from /Portfolios p, (select distinct * "
         "from /Portfolios x, x.positions as myPos) where "
         "myPos.value.secId = 'YHOO'")

    // 35 idx
    ,
    LQRY(importAndSelect,
         "import javaobject.Position;"
         " select distinct secId FROM /Portfolios,  positions.values "
         "pos TYPE Position WHERE ID > 0 ")

    // 36 idx
    ,
    QRY(unsupported,
        "element(select distinct * from /Portfolios where ID "
        "<=3).status"),
    QRY(unsupported,
        "select distinct * from /Portfolios.subregions(false) where "
        "remove('5')!= null")
    //, QRY(unsupported, "element(select distinct * from /Portfolios
    // where ID =1).status")
    ,
    QRY(unsupported, "select distinct * from NULL"),
    QRY(unsupported, "select distinct * from UNDEFINED"),
    LQRY(misc,
         "select distinct * from /Portfolios WHERE NOT (select "
         "distinct * from positions.values p WHERE p.secId = "
         "'IBM').isEmpty")};  // end of resultsetQueries

const int resultsetRowCounts[RS_ARRAY_SIZE] = {
    20, 20, 20, 1,  10, 20, 20, 1, 19, 0, 0, 0,  0,  20, 20, 0, 0, 0, 20, 20, 0,
    1,  1,  1,  10, 1,  1,  1,  1, 1,  1, 2, 20, 10, 20, 10, 0, 0, 0, 0,  2};

const int constantExpectedRowsRS[10] = {1, 21, 22, 23, 25, 26, 28, 29, 30, 35};

const QueryStrings resultsetparamQueries[RSP_ARRAY_SIZE] = {
    // 0 idx
    QRY(operators, "select distinct * from /Portfolios where ID =  $1"),
    QRY(operators, "select distinct * from /Portfolios where ID != $1")

    // 2 idx
    ,
    QRY(constants, "select distinct * from /Portfolios where $1=$2"),
    QRY(constants, "select distinct * from /Portfolios where $1<>$2"),
    QRY(constants,
        "select distinct port from /Portfolios port, port.Positions.values "
        "where UNDEFINED = $1")

    // 5 idx
    ,
    LQRY(functions,
         "select distinct this.value from /Portfolios.entrySet this where "
         "element(select distinct * from /Portfolios.keySet p_k where p_k = "
         "this.key ) = $1 and this.value.status = $2"),
    QRY(functions,
        "select distinct * from /Portfolios pf where nvl(pf.position2,$1) = "
        "$2"),
    QRY(functions,
        "select distinct nvl(pf.position2, $1) from /Portfolios pf where "
        "nvl(pf.position2,$2) = $3")

    // 8 idx
    ,
    QRY(regionInterface,
        "select distinct * from /Portfolios.keys where toString = $1"),
    QRY(regionInterface,
        "select distinct * from /Portfolios.values where ID = $1"),
    QRY(regionInterface,
        "select distinct value from /Portfolios.entries where value.ID = $1 "
        "and key = $2"),
    LQRY(regionInterface,
         "select distinct * from /Portfolios.keySet where toString = $1"),
    LQRY(regionInterface,
         "select distinct key from /Portfolios.entrySet , "
         "value.positions.values   where value.ID = $1 and key = $2")};

const int noofQueryParam[] = {1, 1, 2, 2, 1, 2, 2, 3, 1, 1, 2, 1, 2};

const char queryparamSet[13][3][15] = {
    {"2"},
    {"2"},
    {"1", "1"},
    {"a", "a"},
    {"1"},
    {"port1-3", "inactive"},
    {"foundNull", "foundNull"},
    {"inProjection", "foundNull", "foundNull"},
    {"port1-1"},
    {"1"},
    {"1", "port1-1"},
    {"port1-1"},
    {"1", "port1-1"}};

const int resultsetRowCountsPQ[RSP_ARRAY_SIZE] = {1, 19, 20, 0, 0, 1, 10,
                                                  1, 1,  1,  1, 1, 1};

const int constantExpectedRowsPQRS[6] = {5, 7, 8, 10, 11, 12};

const QueryStrings structsetQueries[SS_ARRAY_SIZE] = {
    // 0 idx
    QRY(singleRegion,
        "select distinct ID, pkid, position1, position2, getType, status "
        "from /Portfolios")

    // 1 idx
    ,
    QRY(multiRegion,
        "select distinct ptf,pos from (select distinct * from /Portfolios "
        "ptf, positions.values pos) outp where outp.pos.secId = 'IBM'"),
    QRY(multiRegion,
        "select distinct ptf,pos from (select distinct * from ptf IN "
        "/Portfolios, pos IN positions.values) where pos.secId = 'IBM'")
    // to be enabled, QRY(multiRegion , "select distinct * from (select
    // distinct ptf.ID, pos AS myPos from /Portfolios ptf, positions.values
    // pos) ")
    /*to be removed */,
    QRY(multiRegion,
        "select distinct myPos, ID from (select distinct pos AS myPos, "
        "ptf.ID as ID from /Portfolios ptf, positions.values pos) ")
    // need to see later , QRY(multiRegion , "select distinct * from (select
    // distinct * from /Portfolios ptf, positions pos) p where
    // p.get('pos').value.secId = 'IBM'")
    ,
    QRY(multiRegion,
        "select distinct secId, getSharesOutstanding from /Positions")

    // 5 idx
    ,
    QRY(constants,
        "select distinct id, positions from /Portfolios where ID = NULL"),
    QRY(constants,
        "select distinct pkid, getPk() from /Portfolios where ID = "
        "UNDEFINED"),
    QRY(constants,
        "select distinct getPositions, getPositions from /Portfolios where "
        "TRUE"),
    QRY(constants,
        "select distinct id, status from /Portfolios where UNDEFINED"),
    QRY(constants,
        "select distinct getPositions, ID, status, pkid from /Portfolios "
        "where TRUE"),
    QRY(constants,
        "select distinct ID, pkid, getType from /Portfolios where 1=1"),
    QRY(constants,
        "select distinct ID, pkid, names from /Portfolios where 'a'<>'a'"),
    QRY(constants,
        "select distinct port.ID, port.status, port.names from /Portfolios "
        "port, port.Positions.values where TRUE"),
    QRY(constants,
        "select distinct port.ID, port.pkid, port.names from /Portfolios "
        "port, port.Positions.values where UNDEFINED = 1"),
    QRY(constants,
        "select distinct port.ID, port.pkid, port.getType from /Portfolios "
        "port where IS_UNDEFINED(UNDEFINED)")

    // 15 idx
    ,
    QRY(functions,
        "select distinct port.ID, port.status from /Portfolios port where "
        "IS_DEFINED(TRUE)"),
    QRY(functions,
        "select distinct port.ID, port.status from /Portfolios port where "
        "IS_UNDEFINED(ID)"),
    LQRY(nestedQueries,
         "select distinct oP.ID, oP.status, oP.getType from /Portfolios oP "
         "where element(select distinct p.ID, p.status, p.getType from "
         "/Portfolios p where p.ID = oP.ID).status = 'inactive'"),
    QRY(functions,
        "select distinct nvl(NULL, 'foundNull')='foundNull', 'dummy Col' "
        "from /Portfolios"),
    QRY(functions,
        "select distinct nvl('notNull', 'foundNull') as NtNul , 'dummy "
        "Col' as Dumbo from /Portfolios"),
    QRY(functions,
        "select distinct pf.ID, pf.status, pf.getType from /Portfolios pf "
        "where nvl(pf.position2,'foundNull') = 'foundNull'"),
    QRY(functions,
        "select distinct nvl(pf.position2, 'inProjection'), 'dummy Col' "
        "from /Portfolios pf where nvl(pf.position2,'foundNull') = "
        "'foundNull'")

    // 22 idx
    ,
    LQRY(nestedQueries,
         "select distinct keys, v.key, v.value.ID, v.value.toString from "
         "/Portfolios.keys keys, /Portfolios.entrySet v where "
         "v.value.toString in (select distinct p.toString from /Portfolios "
         "p where p.ID = v.value.ID) and keys = v.key"),
    LQRY(regionInterface,
         "select distinct ID, pkid, names, position2.secId from "
         "/Portfolios.values where nvl(position2,'nopes') != 'nopes' and "
         "ID > 1"),
    QRY(regionInterface,
        "select distinct key, value.ID, value.pkid from "
        "/Portfolios.entries where value.ID = 1 and key in Set('port1-1')"),
    LQRY(regionInterface,
         "select distinct key, secType, secId, underlyer, cnt from "
         "/Portfolios.entrySet , value.positions.values   where value.ID = "
         "10 and key = 'port1-10'"),
    LQRY(nestedQueries,
         "select distinct k, value.ID, value from /Portfolios.keySet k, "
         "(select distinct value from /Portfolios.entrySet) value where "
         "k.toString = 'port1-1'")

    // 27 idx
    ,
    QRY(nestedQueries,
        "select distinct x.ID, x.status, x.getType, myPos.value.secType, "
        "myPos.value.secId as security from (select distinct * from "
        "/Portfolios x, x.positions as myPos)"),
    LQRY(nestedQueries,
         "select distinct derivedProjAttrbts, key: p.key, id: p.value.ID  "
         "from /Portfolios.entries p, (select distinct x.ID, x.status, "
         "x.getType, myPos.secId from /Portfolios x, x.positions.values as "
         "myPos) derivedProjAttrbts where p.value.ID = "
         "derivedProjAttrbts.ID and derivedProjAttrbts.secId = 'IBM'"),
    LQRY(nestedQueries,
         "select distinct eP.key, eP.value from /Portfolios.entrySet eP, "
         "/Portfolios/Positions pos where pos.secId in (select distinct "
         "portPos.value.secId from (select distinct * from "
         "eP.value.positions) portPos)"),
    LQRY(nestedQueries,
         "select distinct pos.secId, pos.getSharesOutstanding from "
         "/Portfolios/Positions pos where pos.getSharesOutstanding in "
         "(select distinct inPos.getSharesOutstanding from /Positions "
         "inPos where inPos.secId = pos.secId)")

    // 31 idx
    //, QRY(canonicalization, "Select distinct pos.ID, security from
    /// Portfolios.getPositions pos, pos.secId security where length > 1 and
    // pos.ID > 0")
    ,
    QRY(canonicalization, "Select distinct ID, position1  from /Portfolios")

    // 32 idx
    ,
    QRY(unsupported,
        "select element(select distinct * from /Portfolios where ID "
        "<=1).status = 'active'"),
    QRY(unsupported,
        "select distinct * from /Portfolios.subregions(false) where "
        "remove('5')!= null"),
    LQRY(misc,
         "select distinct key: key, iD: entry.value.ID, secId: "
         "posnVal.secId  from /Portfolios.entries entry, "
         "entry.value.positions.values posnVal WHERE entry.value.getType = "
         "'type0' AND posnVal.secId = 'YHOO'")};  // end of structsetQueries

const int structsetRowCounts[SS_ARRAY_SIZE] = {
    20, 2, 2,  20, 20, 0, 0, 20, 0,  20, 20, 0,  20, 0,  20, 20, 0, 10,
    1,  1, 10, 1,  20, 9, 1, 1,  20, 20, 2,  20, 20, 20, 0,  1,  1};

const int structsetFieldCounts[SS_ARRAY_SIZE] = {
    6, 2, 2, 2, 2, 0, 0, 2, 0, 4, 3, 0, 3, 0, 3, 2, 0, 3,
    2, 2, 3, 2, 4, 4, 3, 5, 3, 5, 3, 2, 2, 2, 0, 3, 3};
const int constantExpectedRowsSS[16] = {4,  10, 12, 13, 14, 15, 17, 18,
                                        19, 20, 21, 23, 24, 25, 27, 30};

const QueryStrings structsetParamQueries[SSP_ARRAY_SIZE] = {
    // 0 idx
    QRY(multiRegion,
        "select distinct ptf,pos from (select distinct * from /Portfolios ptf, "
        "positions.values pos) outp where outp.pos.secId = $1")

    // 1 idx
    ,
    QRY(multiRegion,
        "select distinct ptf,pos from (select distinct * from ptf IN "
        "/Portfolios, pos IN positions.values) where pos.secId = $1"),
    QRY(constants,
        "select distinct ID, pkid, getType from /Portfolios where $1 = $2"),
    QRY(constants,
        "select distinct ID, pkid, names from /Portfolios where $1 <> $2")

    // 4 idx
    ,
    QRY(constants,
        "select distinct port.ID, port.pkid, port.names from /Portfolios port, "
        "port.Positions.values where UNDEFINED = $1"),
    LQRY(functions,
         "select distinct oP.ID, oP.status, oP.getType from /Portfolios oP "
         "where element(select distinct p.ID, p.status, p.getType from "
         "/Portfolios p where p.ID = oP.ID).status = $1"),
    QRY(functions,
        "select distinct nvl(NULL, $1)=$2, 'dummy Col' from /Portfolios"),
    QRY(functions,
        "select distinct nvl($1, $2) as NtNul , $3 as Dumbo from /Portfolios"),
    QRY(functions,
        "select distinct pf.ID, pf.status, pf.getType from /Portfolios pf "
        "where nvl(pf.position2,$1) = $2"),
    QRY(functions,
        "select distinct nvl(pf.position2, $1), $2 from /Portfolios pf where "
        "nvl(pf.position2, $3) = $4")

    // 10 idx
    ,
    LQRY(regionInterface,
         "select distinct ID, pkid, names, position2.secId from "
         "/Portfolios.values where nvl(position2, $1) != $1 and ID > $2"),
    QRY(regionInterface,
        "select distinct key, value.ID, value.pkid from /Portfolios.entries "
        "where value.ID = $1 and key in Set($2)"),
    LQRY(regionInterface,
         "select distinct key, secType, secId, underlyer, cnt from "
         "/Portfolios.entrySet , value.positions.values   where value.ID = $1 "
         "and key = $2"),
    LQRY(regionInterface,
         "select distinct k, value.ID, value from /Portfolios.keySet k, "
         "(select distinct value from /Portfolios.entrySet) value where "
         "k.toString = $1")

    // 14 idx
    ,
    LQRY(nestedQueries,
         "select distinct derivedProjAttrbts, key: p.key, id: p.value.ID  from "
         "/Portfolios.entries p, (select distinct x.ID, x.status, x.getType, "
         "myPos.secId from /Portfolios x, x.positions.values as myPos) "
         "derivedProjAttrbts where p.value.ID = derivedProjAttrbts.ID and "
         "derivedProjAttrbts.secId = $1"),
    QRY(unsupported,
        "select element(select distinct * from /Portfolios where ID <= "
        "$1).status = $2"),
    LQRY(misc,
         "select distinct key: key, iD: entry.value.ID, secId: posnVal.secId  "
         "from /Portfolios.entries entry, entry.value.positions.values posnVal "
         "WHERE entry.value.getType = $1 AND posnVal.secId = $2")};

const int numSSQueryParam[] = {1, 1, 2, 2, 1, 1, 2, 3, 2,
                               4, 2, 2, 2, 1, 1, 2, 2};

const char queryparamSetSS[17][4][15] = {
    {"IBM"},
    {"IBM"},
    {"1", "1"},
    {"a", "a"},
    {"1"},
    {"inactive"},
    {"foundNull", "foundNull"},
    {"notNull", "foundNull", "dummy Col"},
    {"foundNull", "foundNull"},
    {"inProjection", "dummy Col", "foundNull", "foundNull"},
    {"nopes", "1"},
    {"1", "port1-1"},
    {"10", "port1-10"},
    {"port1-1"},
    {"IBM"},
    {"1", "active"},
    {"type0", "YHOO"}};

const int structsetRowCountsPQ[SSP_ARRAY_SIZE] = {2, 2, 20, 0, 0,  10, 1, 1, 10,
                                                  1, 9, 1,  1, 20, 2,  0, 1};

const int structsetFieldCountsPQ[SSP_ARRAY_SIZE] = {2, 2, 3, 0, 0, 3, 2, 2, 3,
                                                    2, 4, 3, 5, 3, 3, 0, 3};

const int constantExpectedRowsSSPQ[10] = {2, 4, 5, 6, 7, 8, 9, 10, 11, 12};

const QueryStrings cqResultsetQueries[CQRS_ARRAY_SIZE] = {
    // 0 idx
    QRY(singleRegion, "select * from /Portfolios port")
    // 1 idx
    ,
    QRY(operators, "select  * from /Portfolios where ID =  2"),
    QRY(operators, "select  * from /Portfolios where ID != 2")

    // 3 idx
    ,
    QRY(constants, "select  * from /Portfolios where ID = NULL"),
    QRY(constants, "select  * from /Portfolios where ID = UNDEFINED"),
    QRY(constants, "select  * from /Portfolios where NULL"),
    QRY(constants, "select  * from /Portfolios where UNDEFINED"),
    QRY(constants, "select  * from /Portfolios where TRUE"),
    QRY(constants, "select  * from /Portfolios where 1=1"),
    QRY(constants, "select  * from /Portfolios where 'a'<>'a'")

    // 10 idx
    ,
    QRY(functions, "select  * from /Portfolios port where IS_DEFINED(TRUE)"),
    QRY(functions, "select  * from /Portfolios port where IS_UNDEFINED(ID)"),
    LQRY(nestedQueries,
         "select  this.value from /Portfolios.entrySet this where "
         "element(select  * from /Portfolios.keySet p_k where p_k = this.key ) "
         "= 'port1-3' and this.value.status = 'inactive'"),
    QRY(functions,
        "select  * from /Portfolios pf where nvl(pf.position2,'foundNull') = "
        "'foundNull'")

    // 14 idx
    ,
    QRY(functions,
        "select  * from /Portfolios WHERE NOT(SELECT DISTINCT * FROM "
        "positions.values p WHERE p.secId = 'IBM').isEmpty"),
    LQRY(nestedQueries,
         "select  x.value from /Portfolios.entrySet x where x.value.status = "
         "ELEMENT(select  p.value from /Portfolios.entrySet p where x.key = "
         "p.key).status"),
    LQRY(nestedQueries,
         "select  * from /Portfolios x where status = ELEMENT(select  "
         "p_k.value from /Portfolios.entrySet p_k where p_k.key = "
         "'port1-1').status"),
    LQRY(nestedQueries,
         "select  p from /Portfolios p, (select  * from /Portfolios x, "
         "x.positions as myPos) where myPos.value.secId = 'YHOO'")

    // 35 idx
    ,
    LQRY(importAndSelect,
         "import javaobject.Position;"
         " select  secId FROM /Portfolios,  positions.values pos TYPE Position "
         "WHERE ID > 0 ")

    // 36 idx
    ,
    QRY(unsupported, "element(select  * from /Portfolios where ID <=3).status"),
    QRY(unsupported,
        "select  * from /Portfolios.subregions(false) where remove('5')!= null")
    //, QRY(unsupported, "element(select  * from /Portfolios where ID
    //=1).status")
    ,
    QRY(unsupported, "select  * from NULL"),
    QRY(unsupported, "select  * from UNDEFINED"),
    LQRY(misc,
         "select  * from /Portfolios WHERE NOT (select  * from "
         "positions.values p WHERE p.secId = 'IBM').isEmpty")};

const int cqResultsetRowCounts[CQRS_ARRAY_SIZE] = {20, 1,  19, 0, 0, 0,  0, 20,
                                                   20, 0,  20, 0, 1, 10, 2, 20,
                                                   10, 20, 10, 0, 0, 0,  0, 2};

const int constantExpectedRowsCQRS[1] = {35};
};  // end of namespace
using namespace testData;
using namespace testobject;

class QueryHelper {
 public:
  static QueryHelper* singleton;

  static QueryHelper& getHelper() {
    if (singleton == NULL) {
      singleton = new QueryHelper();
    }
    return *singleton;
  }

  QueryHelper() {
    portfolioSetSize = 20;
    portfolioNumSets = 1;
    positionSetSize = 20;
    positionNumSets = 1;
  }

  virtual ~QueryHelper() { ; }

  virtual void populatePortfolioData(RegionPtr& pregion, int setSize,
                                     int numSets, int32_t objSize = 1);
  virtual void populatePortfolio(RegionPtr& rptr, int maxKey,
                                 int32_t objSize = 1);
  virtual void populatePositionData(RegionPtr& pregion, int setSize,
                                    int numSets);
  virtual void populatePortfolioPdxData(RegionPtr& pregion, int setSize,
                                        int numSets, int32_t objSize = 1,
                                        char** nm = NULL);
  virtual void populatePositionPdxData(RegionPtr& pregion, int setSize,
                                       int numSets);
  virtual void populateAutoPortfolioPdxData(RegionPtr& pregion, int setSize,
                                            int numSets, int32_t objSize = 1,
                                            char** nm = NULL);
  virtual void populateAutoPositionPdxData(RegionPtr& pregion, int setSize,
                                           int numSets);
  virtual void destroyPortfolioOrPositionData(RegionPtr& pregion, int setSize,
                                              int numSets,
                                              const char* dataType);
  virtual void invalidatePortfolioOrPositionData(RegionPtr& pregion,
                                                 int setSize, int numSets,
                                                 const char* dataType);

  // might need it in later.
  //  virtual void getRSQueryString(char category, int& queryIndex, std::string&
  //  query);
  //  virtual void getSSQueryString(char category, int& queryIndex, std::string&
  //  query);

  virtual bool verifyRS(SelectResultsPtr& resultset, int rowCount);
  virtual bool verifySS(SelectResultsPtr& structset, int rowCount,
                        int fieldCount);

  void populateRangePositionData(RegionPtr& rptr, int start, int end);
  bool compareTwoPositionObjects(SerializablePtr por1, SerializablePtr por2);
  SerializablePtr getExactPositionObject(int iForExactPosObject);
  void putExactPositionObject(RegionPtr& rptr, int iForExactPosObject);
  SerializablePtr getCachedPositionObject(RegionPtr& rptr,
                                          int iForExactPosObject);

  // utility methods
  virtual int getPortfolioSetSize() { return portfolioSetSize; };
  virtual int getPortfolioNumSets() { return portfolioNumSets; };
  virtual int getPositionSetSize() { return positionSetSize; };
  virtual int getPositionNumSets() { return positionNumSets; };

  bool isExpectedRowsConstantRS(int queryindex) {
    for (int i = (sizeof(constantExpectedRowsRS) / sizeof(int)) - 1; i > -1;
         i--)
      if (constantExpectedRowsRS[i] == queryindex) {
        FWKINFO("index " << constantExpectedRowsRS[i]
                         << " is having constant rows ");
        return true;
      }

    return false;
  }

  bool isExpectedRowsConstantSS(int queryindex) {
    for (int i = (sizeof(constantExpectedRowsSS) / sizeof(int)) - 1; i > -1;
         i--) {
      if (constantExpectedRowsSS[i] == queryindex) {
        FWKINFO("index " << constantExpectedRowsSS[i]
                         << " is having constant rows ");
        return true;
      }
    }

    return false;
  }

  bool isExpectedRowsConstantCQRS(int queryindex) {
    for (int i = (sizeof(constantExpectedRowsCQRS) / sizeof(int)) - 1; i > -1;
         i--)
      if (constantExpectedRowsCQRS[i] == queryindex) {
        FWKINFO("index " << constantExpectedRowsCQRS[i]
                         << " is having constant rows ");
        return true;
      }

    return false;
  }

  bool isExpectedRowsConstantPQRS(int queryindex) {
    for (int i = (sizeof(constantExpectedRowsPQRS) / sizeof(int)) - 1; i > -1;
         i--)
      if (constantExpectedRowsPQRS[i] == queryindex) {
        printf("index %d is having constant rows \n",
               constantExpectedRowsPQRS[i]);
        return true;
      }

    return false;
  }
  bool isExpectedRowsConstantSSPQ(int queryindex) {
    for (int i = (sizeof(constantExpectedRowsSSPQ) / sizeof(int)) - 1; i > -1;
         i--) {
      if (constantExpectedRowsSSPQ[i] == queryindex) {
        printf("index %d is having constant rows \n",
               constantExpectedRowsSSPQ[i]);
        return true;
      }
    }
    return false;
  }

 private:
  int portfolioSetSize;
  int portfolioNumSets;
  int positionSetSize;
  int positionNumSets;
  int32_t objectSize ATTR_UNUSED;
};

QueryHelper* QueryHelper::singleton = NULL;

//===========================================================================================

void QueryHelper::populateRangePositionData(RegionPtr& rptr, int start,
                                            int end) {
  for (int i = start; i <= end; i++) {
    CacheablePtr pos(new Position(i));
    char key[100];
    ACE_OS::sprintf(key, "pos%d", i);
    CacheableKeyPtr keyptr = CacheableKey::create(key);
    rptr->put(keyptr, pos);
  }
}

bool QueryHelper::compareTwoPositionObjects(SerializablePtr pos1,
                                            SerializablePtr pos2) {
  Position* p1 = dynamic_cast<Position*>(pos1.ptr());
  Position* p2 = dynamic_cast<Position*>(pos2.ptr());

  if (p1 == NULL || p2 == NULL) {
    printf("ERROR: The object(s) passed are not of Portflio type\n");
    return false;
  }

  DataOutput o1, o2;
  p1->toData(o1);
  p2->toData(o2);

  uint32_t len1 = o1.getBufferLength();
  uint32_t len2 = o2.getBufferLength();

  if (len1 != len2) {
    return false;
  }

  const uint8_t* ptr1 = o1.getBuffer();
  const uint8_t* ptr2 = o2.getBuffer();

  for (uint8_t i = 0; i < len1; i++) {
    if (*ptr1++ != *ptr2++) {
      return false;
    }
  }
  return true;
}

SerializablePtr QueryHelper::getExactPositionObject(int iForExactPosObject) {
  CacheablePtr pos(new Position(iForExactPosObject));
  return pos;
}

void QueryHelper::putExactPositionObject(RegionPtr& rptr,
                                         int iForExactPosObject) {
  char key[100];
  ACE_OS::sprintf(key, "pos%d", iForExactPosObject);
  CacheableKeyPtr keyptr = CacheableKey::create(key);
  CacheablePtr pos(new Position(iForExactPosObject));
  rptr->put(keyptr, pos);
}

SerializablePtr QueryHelper::getCachedPositionObject(RegionPtr& rptr,
                                                     int iForExactPosObject) {
  char key[100];
  ACE_OS::sprintf(key, "pos%d", iForExactPosObject);
  CacheableKeyPtr keyptr = CacheableKey::create(key);
  return rptr->get(keyptr);
}

//===========================================================================================

void QueryHelper::populatePortfolioData(RegionPtr& rptr, int setSize,
                                        int numSets, int32_t objSize) {
  // lets reset the counter for uniform population of position objects
  Position::resetCounter();
  CacheablePtr port;
  CacheableKeyPtr keyport;
  for (int set = 1; set <= numSets; set++) {
    for (int current = 1; current <= setSize; current++) {
      port = new Portfolio(current, objSize);

      char portname[100] = {0};
      ACE_OS::sprintf(portname, "port%d-%d", set, current);

      keyport = CacheableKey::create(portname);
      rptr->put(keyport, port);
    }
  }

  FWKINFO("all puts done \n");
}
void QueryHelper::populatePortfolio(RegionPtr& rptr, int maxKey,
                                    int32_t objSize) {
  // lets reset the counter for uniform population of position objects
  Position::resetCounter();
  CacheablePtr port;
  CacheableKeyPtr keyport;
  for (int current = 0; current <= maxKey; current++) {
    port = new Portfolio(current, objSize);

    char portname[1024] = {0};
    ACE_OS::sprintf(portname, "port%d-%d", current, current);

    keyport = CacheableKey::create(portname);
    rptr->put(keyport, port);
  }

  FWKINFO("All Portfolio puts done \n");
}

void QueryHelper::destroyPortfolioOrPositionData(RegionPtr& rptr, int setSize,
                                                 int numSets,
                                                 const char* dataType) {
  CacheableKeyPtr keyport;
  try {
    for (int set = 1; set <= numSets; set++) {
      for (int current = 1; current <= setSize; current++) {
        char portname[100] = {0};
        if (strcmp(dataType, "Portfolio") == 0 ||
            strcmp(dataType, "PortfolioPdx") == 0 ||
            strcmp(dataType, "AutoPortfolioPdx") == 0)
          ACE_OS::sprintf(portname, "port%d-%d", set, current);
        else if (strcmp(dataType, "Position") == 0 ||
                 strcmp(dataType, "PositionPdx") == 0 ||
                 strcmp(dataType, "AutoPositionPdx") == 0)
          ACE_OS::sprintf(portname, "pos%d-%d", set, current);
        else {
          throw gemfire::IllegalArgumentException("Unknown object type");
        }

        keyport = CacheableKey::create(portname);
        rptr->destroy(keyport);
      }
    }
  } catch (Exception& e) {
    FWKSEVERE("QueryHelper::destroyPortfolioOrPositionData Caught Exception: "
              << e.getMessage());
  }
  FWKINFO("destroy done \n");
}

void QueryHelper::invalidatePortfolioOrPositionData(RegionPtr& rptr,
                                                    int setSize, int numSets,
                                                    const char* dataType) {
  try {
    for (int set = 1; set <= numSets; set++) {
      for (int current = 1; current <= setSize; current++) {
        char portname[100] = {0};
        if (strcmp(dataType, "Portfolio") == 0)
          ACE_OS::sprintf(portname, "port%d-%d", set, current);
        else if (strcmp(dataType, "Position") == 0)
          ACE_OS::sprintf(portname, "pos%d-%d", set, current);

        CacheableKeyPtr keyport = CacheableKey::create(portname);
        rptr->invalidate(keyport);
      }
    }
  } catch (Exception& e) {
    FWKSEVERE(
        "QueryHelper::invalidatePortfolioOrPositionData Caught Exception: "
        << e.getMessage());
  }
  FWKINFO("Invalidation done \n");
}

const char* secIds[] = {"SUN", "IBM",  "YHOO", "GOOG", "MSFT",
                        "AOL", "APPL", "ORCL", "SAP",  "DELL"};

void QueryHelper::populatePositionData(RegionPtr& rptr, int setSize,
                                       int numSets) {
  int numSecIds = sizeof(secIds) / sizeof(char*);
  CacheableKeyPtr keypos;
  CacheablePtr pos;
  for (int set = 1; set <= numSets; set++) {
    for (int current = 1; current <= setSize; current++) {
      pos = new Position(secIds[current % numSecIds], current * 100);

      char posname[100] = {0};
      ACE_OS::sprintf(posname, "pos%d-%d", set, current);

      keypos = CacheableKey::create(posname);
      rptr->put(keypos, pos);
    }
  }
}

void QueryHelper::populatePortfolioPdxData(RegionPtr& rptr, int setSize,
                                           int numSets, int32_t objSize,
                                           char** nm) {
  // lets reset the counter for uniform population of position objects
  testobject::PositionPdx::resetCounter();

  for (int set = 1; set <= numSets; set++) {
    for (int current = 1; current <= setSize; current++) {
      CacheablePtr port(new testobject::PortfolioPdx(current, objSize));

      char portname[100] = {0};
      ACE_OS::sprintf(portname, "port%d-%d", set, current);

      CacheableKeyPtr keyport = CacheableKey::create(portname);

      rptr->put(keyport, port);
      LOGINFO("populatePortfolioPdxData:: Put for iteration current = %d done",
              current);
    }
  }
  // portfolioSetSize = setSize; portfolioNumSets = numSets; objectSize =
  // objSize;

  printf("all puts done \n");
}

void QueryHelper::populatePositionPdxData(RegionPtr& rptr, int setSize,
                                          int numSets) {
  int numSecIds = sizeof(secIds) / sizeof(char*);

  for (int set = 1; set <= numSets; set++) {
    for (int current = 1; current <= setSize; current++) {
      CacheablePtr pos(new testobject::PositionPdx(secIds[current % numSecIds],
                                                   current * 100));

      char posname[100] = {0};
      ACE_OS::sprintf(posname, "pos%d-%d", set, current);

      CacheableKeyPtr keypos = CacheableKey::create(posname);
      rptr->put(keypos, pos);
      LOGINFO("populatePositionPdxData:: Put for iteration current = %d done",
              current);
    }
  }
  // positionSetSize = setSize; positionNumSets = numSets;
}
void QueryHelper::populateAutoPortfolioPdxData(RegionPtr& rptr, int setSize,
                                               int numSets, int32_t objSize,
                                               char** nm) {
  // lets reset the counter for uniform population of position objects
  AutoPdxTests::PositionPdx::resetCounter();

  for (int set = 1; set <= numSets; set++) {
    for (int current = 1; current <= setSize; current++) {
      CacheablePtr port(new AutoPdxTests::PortfolioPdx(current, objSize));

      char portname[100] = {0};
      ACE_OS::sprintf(portname, "port%d-%d", set, current);

      CacheableKeyPtr keyport = CacheableKey::create(portname);

      rptr->put(keyport, port);
      LOGINFO(
          "populateAutoPortfolioPdxData:: Put for iteration current = %d done",
          current);
    }
  }
  // portfolioSetSize = setSize; portfolioNumSets = numSets; objectSize =
  // objSize;

  printf("all puts done \n");
}

void QueryHelper::populateAutoPositionPdxData(RegionPtr& rptr, int setSize,
                                              int numSets) {
  int numSecIds = sizeof(secIds) / sizeof(char*);

  for (int set = 1; set <= numSets; set++) {
    for (int current = 1; current <= setSize; current++) {
      CacheablePtr pos(new AutoPdxTests::PositionPdx(
          secIds[current % numSecIds], current * 100));

      char posname[100] = {0};
      ACE_OS::sprintf(posname, "pos%d-%d", set, current);

      CacheableKeyPtr keypos = CacheableKey::create(posname);
      rptr->put(keypos, pos);
      LOGINFO(
          "populateAutoPositionPdxData:: Put for iteration current = %d done",
          current);
    }
  }
  // positionSetSize = setSize; positionNumSets = numSets;
}
bool QueryHelper::verifyRS(SelectResultsPtr& resultSet, int expectedRows) {
  if (!instanceOf<ResultSetPtr>(resultSet)) {
    return false;
  }

  ResultSetPtr rsptr = staticCast<ResultSetPtr>(resultSet);

  int foundRows = 0;

  SelectResultsIterator iter = rsptr->getIterator();

  for (int32_t rows = 0; rows < rsptr->size(); rows++) {
    SerializablePtr ser = (*rsptr)[rows];  // iter.next();
    foundRows++;
  }

  FWKINFO("found rows " << foundRows << " expected " << expectedRows);
  if (foundRows == expectedRows) return true;

  return false;
}

bool QueryHelper::verifySS(SelectResultsPtr& structSet, int expectedRows,
                           int expectedFields) {
  FWKINFO("QueryHelper::verifySS : expectedRows = "
          << expectedRows << " ,expectedFields = " << expectedFields);

  if (!instanceOf<StructSetPtr>(structSet)) {
    if (expectedRows == 0 && expectedFields == 0)
      return true;  // quite possible we got a null set back.
    FWKINFO("we have structSet itself NULL");
    return false;
  }

  StructSetPtr ssptr = staticCast<StructSetPtr>(structSet);

  int foundRows = 0;

  SelectResultsIterator iter = ssptr->getIterator();

  for (int32_t rows = 0; rows < ssptr->size(); rows++) {
    SerializablePtr ser = (*ssptr)[rows];  // iter.next();
    foundRows++;

    Struct* siptr = dynamic_cast<Struct*>(ser.ptr());

    if (siptr == NULL) {
      LOGINFO("siptr is NULL \n\n");
      return false;
    }

    int foundFields = 0;

    for (int32_t cols = 0; cols < siptr->length(); cols++) {
      SerializablePtr field = (*siptr)[cols];  // siptr->next();
      foundFields++;
    }

    if (foundFields != expectedFields) {
      char buffer[1024] = {'\0'};
      sprintf(buffer, "found fields %d, expected fields %d \n", foundFields,
              expectedFields);
      FWKSEVERE(buffer);
      return false;
    }
  }

  if (foundRows == expectedRows) {
    FWKINFO("found rows " << foundRows << " expected " << expectedRows);
    return true;
  }

  // lets log and return in case of error only situation
  char buffer[1024] = {'\0'};
  sprintf(buffer, "found rows %d, expected rows %d\n", foundRows, expectedRows);
  FWKSEVERE(buffer);
  return false;
}

#endif  // TEST_QUERYHELPER_HPP
