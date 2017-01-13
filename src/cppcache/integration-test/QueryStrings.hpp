/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef TEST_QUERYSTRINGS_HPP
#define TEST_QUERYSTRINGS_HPP

#include <cstring>

namespace testData {

const int RS_ARRAY_SIZE = 40;
const int RSP_ARRAY_SIZE = 13;
const int RSOPL_ARRAY_SIZE =
    5;  // result set sizes for queries returned via ObjectPartList
const int SS_ARRAY_SIZE = 36;
const int SSP_ARRAY_SIZE = 17;
const int SSOPL_ARRAY_SIZE =
    4;  // struct set sizes for queries returned via ObjectPartList
const int RQ_ARRAY_SIZE = 21;  // for region.query() API queries
const int CQRS_ARRAY_SIZE = 24;

const int MAX_QRY_LENGTH = 512;
enum queryCategory {
  singleRegion = 0,
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
  regionQuery,
  qcEnd /*to mark the end of enum*/
} qcType;

class QueryStrings {
 public:
  QueryStrings(queryCategory pcategory, const char* pquery,
               bool pisLargeResultset = false) {
    size_t querylen = static_cast<int>(strlen(pquery));
    if (querylen < MAX_QRY_LENGTH) memcpy(_query, pquery, querylen);
    memset(&_query[querylen], '\0', 1);
    category = pcategory;
    haveLargeResultset = pisLargeResultset;
  }

  static int RSsize() { return RS_ARRAY_SIZE; };
  static int RSPsize() { return RSP_ARRAY_SIZE; };
  static int RSOPLsize() { return RSOPL_ARRAY_SIZE; };
  static int SSsize() { return SS_ARRAY_SIZE; };
  static int SSPsize() { return SSP_ARRAY_SIZE; };
  static int SSOPLsize() { return SSOPL_ARRAY_SIZE; };
  static int RQsize() { return RQ_ARRAY_SIZE; };
  static int CQRSsize() { return CQRS_ARRAY_SIZE; };
  const char* query() const { return _query; };

 public:
  char _query[MAX_QRY_LENGTH];
  queryCategory category;
  bool haveLargeResultset;

 private:
  QueryStrings();
  // QueryStrings(QueryStrings&);
  QueryStrings* operator=(QueryStrings&);
};

#define QRY(category, query) QueryStrings(category, query)

// for large resultset
#define LQRY(category, query) QueryStrings(category, query, true)

const QueryStrings resultsetQueries[RS_ARRAY_SIZE] = {
    // 0 idx
    QRY(singleRegion, "select distinct * from /Portfolios port"),
    QRY(singleRegion, "select distinct pkid from /Portfolios")

    // 2 idx
    ,
    LQRY(multiRegion,
         "select distinct port from /Portfolios port, "
         "port.Positions.values"),
    QRY(multiRegion,
        "select distinct pf from /Portfolios pf , pf.positions.values "
        "pos where pos.getSecId = 'IBM' and NOT pf.isActive"),
    LQRY(multiRegion,
         "Select distinct pf from /Portfolios pf, /Portfolios2, "
         "/Portfolios3 where pf.isActive = TRUE"),
    LQRY(multiRegion,
         "select distinct pos from /Portfolios, /Portfolios/Positions "
         "pos")

    // 6 idx
    ,
    QRY(operators, "select distinct * from /Portfolios where ID =  2"),
    QRY(operators, "select distinct * from /Portfolios where ID != 2"),
    LQRY(multiRegion,
         "select distinct port from /Portfolios port , /Positions pos "
         "where pos.secId in (select distinct pos.secId from "
         "port.positions.values pos)")

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
    LQRY(functions,
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
    QRY(nestedQueries,
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
    //, QRY(unsupported, "select distinct * from NULL")
    //, QRY(unsupported, "select distinct * from UNDEFINED")

    ,
    LQRY(misc,
         "select distinct * from /Portfolios WHERE NOT (select "
         "distinct * from positions.values p WHERE p.secId = "
         "'IBM').isEmpty"),
    QRY(misc,
        "select distinct to_date('21/10/07','dd/mm/yy') from "
        "/Portfolios")

    // 42 idx -- scalar result queries
    //, QRY(constants, "select /Portfolios.size from set(0)")
    //, QRY(constants, "/Portfolios.size")
    //, QRY(constants, "(select * from /Portfolios).size")
};  // end of resultsetQueries

const int resultsetRowCounts[RS_ARRAY_SIZE] = {
    20, 20, 20, 1, 10, 20, 1, 19, 20, 0, 0, 0, 0,  20, 20, 0,  0, 0, 20, 20,
    0,  1,  1,  1, 10, 1,  1, 1,  1,  1, 1, 2, 20, 10, 20, 10, 0, 0, 2,  1};

const int constantExpectedRowsRS[12] = {1,  21, 22, 23, 25, 26,
                                        28, 29, 30, 35, 39, 42};

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

const QueryStrings resultsetQueriesOPL[RSOPL_ARRAY_SIZE] =
    {  // queries returning result sets via ObjectPartList
        // 0 idx
        QRY(singleRegion, "select * from /Portfolios2"),
        QRY(singleRegion, "select * from /Portfolios2 limit 5"),
        QRY(singleRegion, "select count(*) from /Portfolios2"),
        QRY(singleRegion, "select ALL * from /Portfolios2"),
        QRY(singleRegion, "select * from /Portfolios2.values")};

const int resultsetRowCountsOPL[RSOPL_ARRAY_SIZE] = {20, 5, 1, 20, 20};

const QueryStrings structsetQueriesOPL[SSOPL_ARRAY_SIZE] = {
    // 0 idx
    QRY(multiRegion, "select * from /Portfolios2, /Portfolios3"),
    QRY(multiRegion, "select * from /Portfolios2, /Portfolios3 limit 5"),
    QRY(multiRegion, "select ALL * from /Portfolios2 p2, /Portfolios3 p3"),
    QRY(multiRegion, "select * from /Portfolios2.values, /Portfolios3.values")};

const int structsetRowCountsOPL[SSOPL_ARRAY_SIZE] = {400, 5, 400, 400};

const int structsetFieldCountsOPL[SSOPL_ARRAY_SIZE] = {2, 2, 2, 2};

const QueryStrings structsetQueries[SS_ARRAY_SIZE] = {
    // 0 idx
    QRY(singleRegion,
        "select distinct ID, pkid, position1, position2, getType, status from "
        "/Portfolios")

    // 1 idx
    ,
    QRY(multiRegion,
        "select distinct ptf,pos from (select distinct * from /Portfolios ptf, "
        "positions.values pos) outp where outp.pos.secId = 'IBM'"),
    QRY(multiRegion,
        "select distinct ptf,pos from (select distinct * from ptf IN "
        "/Portfolios, pos IN positions.values) where pos.secId = 'IBM'")
    // to be enabled, QRY(multiRegion , "select distinct * from (select distinct
    // ptf.ID, pos AS myPos from /Portfolios ptf, positions.values pos) ")
    /*to be removed */,
    QRY(multiRegion,
        "select distinct myPos, ID from (select distinct pos AS myPos, ID: "
        "ptf.ID from /Portfolios ptf, positions.values pos) ")
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
        "select distinct pkid, getPk() from /Portfolios where ID = UNDEFINED"),
    QRY(constants,
        "select distinct getPositions, getPositions from /Portfolios where "
        "TRUE"),
    QRY(constants,
        "select distinct id, status from /Portfolios where UNDEFINED"),
    QRY(constants,
        "select distinct getPositions, ID, status, pkid from /Portfolios where "
        "TRUE"),
    QRY(constants,
        "select distinct ID, pkid, getType from /Portfolios where 1=1"),
    QRY(constants,
        "select distinct ID, pkid, names from /Portfolios where 'a'<>'a'"),
    QRY(constants,
        "select distinct port.ID, port.status, port.names from /Portfolios "
        "port, port.Positions.values where TRUE"),
    QRY(constants,
        "select distinct port.ID, port.pkid, port.names from /Portfolios port, "
        "port.Positions.values where UNDEFINED = 1"),
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
    LQRY(functions,
         "select distinct oP.ID, oP.status, oP.getType from /Portfolios oP "
         "where element(select distinct p.ID, p.status, p.getType from "
         "/Portfolios p where p.ID = oP.ID).status = 'inactive'"),
    QRY(functions,
        "select distinct nvl(NULL, 'foundNull')='foundNull', 'dummy Col' from "
        "/Portfolios"),
    QRY(functions,
        "select distinct nvl('notNull', 'foundNull') as NtNul , 'dummy Col' as "
        "Dumbo from /Portfolios"),
    QRY(functions,
        "select distinct pf.ID, pf.status, pf.getType from /Portfolios pf "
        "where nvl(pf.position2,'foundNull') = 'foundNull'"),
    QRY(functions,
        "select distinct nvl(pf.position2, 'inProjection'), 'dummy Col' from "
        "/Portfolios pf where nvl(pf.position2,'foundNull') = 'foundNull'")

    // 22 idx
    ,
    LQRY(regionInterface,
         "select distinct keys, v.key, v.value.ID, v.value.toString from "
         "/Portfolios.keys keys, /Portfolios.entrySet v where v.value.toString "
         "in (select distinct p.toString from /Portfolios p where p.ID = "
         "v.value.ID) and keys = v.key"),
    LQRY(regionInterface,
         "select distinct ID, pkid, names, position2.secId from "
         "/Portfolios.values where nvl(position2,'nopes') != 'nopes' and ID > "
         "1"),
    QRY(regionInterface,
        "select distinct key, value.ID, value.pkid from /Portfolios.entries "
        "where value.ID = 1 and key in Set('port1-1')"),
    LQRY(regionInterface,
         "select distinct key, secType, secId, underlyer, cnt from "
         "/Portfolios.entrySet , value.positions.values   where value.ID = 10 "
         "and key = 'port1-10'"),
    LQRY(regionInterface,
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
         "select distinct derivedProjAttrbts, key: p.key, id: p.value.ID  from "
         "/Portfolios.entries p, (select distinct x.ID, x.status, x.getType, "
         "myPos.secId from /Portfolios x, x.positions.values as myPos) "
         "derivedProjAttrbts where p.value.ID = derivedProjAttrbts.ID and "
         "derivedProjAttrbts.secId = 'IBM'"),
    LQRY(nestedQueries,
         "select distinct eP.key, eP.value from /Portfolios.entrySet eP, "
         "/Portfolios/Positions pos where pos.secId in (select distinct "
         "portPos.value.secId from (select distinct * from eP.value.positions) "
         "portPos)"),
    LQRY(nestedQueries,
         "select distinct pos.secId, pos.getSharesOutstanding from "
         "/Portfolios/Positions pos where pos.getSharesOutstanding in (select "
         "distinct inPos.getSharesOutstanding from /Positions inPos where "
         "inPos.secId = pos.secId)")

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
         "select distinct key: key, iD: entry.value.ID, secId: posnVal.secId  "
         "from /Portfolios.entries entry, entry.value.positions.values posnVal "
         "WHERE entry.value.getType = 'type0' AND posnVal.secId = 'YHOO'")
//, QRY(misc        , "select distinct to_date('01/01/3000','mm/dd/yyyy'),
// to_date('01/01/1900','mm/dd/yyyy').toGMTString(),
// to_date('10/10/2020','mm/dd/yyyy').toLocaleString(),
// to_date('10/10/2020','mm/dd/yyyy') from /Portfolios")
#if !defined(WIN32)
        ,
    QRY(misc,
        "select distinct to_date('01/19/2038 03:848 AM','mm/dd/yyyy hh:sss a') "
        "as twoeighty, to_date('01/19/2038','mm/dd/yyyy'), to_date('12/14/1901 "
        "00:2753 PM','mm/dd/yyyy hh:ssss a') from /Portfolios")
#else
    // Windows does not support 'a' format
    ,
    QRY(misc,
        "select distinct to_date('01/19/2038 03:848 AM','mm/dd/yyyy hh:sss') "
        "as twoeighty, to_date('01/19/2038','mm/dd/yyyy'), to_date('12/14/1901 "
        "00:2753 AM','mm/dd/yyyy hh:ssss') from /Portfolios")
#endif
};  // end of structsetQueries

const int structsetRowCounts[SS_ARRAY_SIZE] = {
    20, 2, 2,  20, 20, 0, 0, 20, 0,  20, 20, 0,  20, 0,  20, 20, 0, 10,
    1,  1, 10, 1,  20, 9, 1, 1,  20, 20, 2,  20, 20, 20, 0,  1,  1, 1};

const int structsetFieldCounts[SS_ARRAY_SIZE] = {
    6, 2, 2, 2, 2, 0, 0, 2, 0, 4, 3, 0, 3, 0, 3, 2, 0, 3,
    2, 2, 3, 2, 4, 4, 3, 5, 3, 5, 3, 2, 2, 2, 0, 3, 3, 3};

const int constantExpectedRowsSS[17] = {4,  10, 12, 13, 14, 15, 17, 18, 19,
                                        20, 21, 23, 24, 25, 27, 30, 35};

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

const QueryStrings regionQueries[RQ_ARRAY_SIZE] =
    {  // intended to be run on the /Portfolios region.query API
        // 0 idx
        QRY(regionQuery, "ID = 2"), QRY(regionQuery, "ID != 2"),
        QRY(regionQuery, "ID = NULL"), QRY(unsupported, "ID = UNDEFINED")
        // 4 idx
        ,
        QRY(regionQuery, "NULL"), QRY(regionQuery, "UNDEFINED"),
        QRY(regionQuery, "UNDEFINED = 1"),
        QRY(regionQuery, "IS_UNDEFINED(UNDEFINED)"),
        QRY(regionQuery, "IS_DEFINED(TRUE)"),
        QRY(regionQuery, "IS_UNDEFINED(ID)"), QRY(regionQuery, "TRUE"),
        QRY(regionQuery, "FALSE"), QRY(regionQuery, "1=1"),
        QRY(regionQuery, "'a' <> 'a'")
        // 14 idx
        ,
        QRY(regionQuery, "toString = 'port1-1'"),
        QRY(regionQuery,
            "NOT (SELECT DISTINCT * FROM positions.values p WHERE p.secId = "
            "'IBM').isEmpty"),
        QRY(regionQuery,
            "status = ELEMENT(select distinct p_k.value from "
            "/Portfolios.entrySet p_k where p_k.key = 'port1-1').status")
        // 17 idx
        ,
        QRY(regionQuery,
            "select distinct port from /Portfolios port , /Positions pos where "
            "pos.secId in (select distinct pos.secId from "
            "port.positions.values pos)"),
        QRY(regionQuery,
            "select distinct pos.secId, pos.sharesOutstanding from "
            "/Portfolios/Positions pos where pos.sharesOutstanding in (select "
            "distinct inPos.sharesOutstanding from /Positions inPos where "
            "inPos.secId = pos.secId)")
        // 19 idx
        ,
        QRY(unsupported, "bad predicate"),
        QRY(unsupported,
            "select element(select distinct * from /Portfolios where ID "
            "<=1).status = 'active'")};  // end of regionQueries

const int regionQueryRowCounts[RQ_ARRAY_SIZE] =
    {  // results sizes for regionQuery
        1, 19, 0, 0, 0, 0,  0,  20, 20, 0, 20,
        0, 20, 0, 0, 2, 10, 20, 20, -1, -1};
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

#endif  // TEST_QUERYSTRINGS_HPP
