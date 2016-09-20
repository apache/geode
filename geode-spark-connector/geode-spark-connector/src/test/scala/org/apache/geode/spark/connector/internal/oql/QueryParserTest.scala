/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.spark.connector.internal.oql

import org.scalatest.FunSuite

class QueryParserTest extends FunSuite {

  test("select * from /r1") {
    val r = QueryParser.parseOQL("select * from /r1").get
    assert(r == "List(/r1)")
  }

  test("select c2 from /r1") {
    val r = QueryParser.parseOQL("select c2 from /r1").get
    assert(r == "List(/r1)")
  }

  test("select key, value from /r1.entries") {
    val r = QueryParser.parseOQL("select key, value from /r1.entries").get
    assert(r == "List(/r1.entries)")
  }

  test("select c1, c2 from /r1 where col1 > 100 and col2 <= 120 or c3 = 2") {
    val r = QueryParser.parseOQL("select c1, c2 from /r1 where col1 > 100 and col2 <= 120 or c3 = 2").get
    assert(r == "List(/r1)")
  }

  test("select * from /r1/r2 where c1 >= 200") {
    val r = QueryParser.parseOQL("select * from /r1/r2 where c1 >= 200").get
    assert(r == "List(/r1/r2)")
  }

  test("import io.pivotal select c1, c2, c3 from /r1/r2, /r3/r4 where c1 <= 15 and c2 = 100") {
    val r = QueryParser.parseOQL("import io.pivotal select c1, c2, c3 from /r1/r2, /r3/r4 where c1 <= 15 and c2 = 100").get
    assert(r == "List(/r1/r2, /r3/r4)")
  }

  test("SELECT distinct f1, f2 FROM /r1/r2 WHere f = 100") {
    val r = QueryParser.parseOQL("SELECT distinct f1, f2 FROM /r1/r2 WHere f = 100").get
    assert(r == "List(/r1/r2)")
  }

  test("IMPORT org.apache.geode IMPORT com.mypackage SELECT key,value FROM /root/sub.entries WHERE status = 'active' ORDER BY id desc") {
    val r = QueryParser.parseOQL("IMPORT org.apache.geode IMPORT com.mypackage SELECT key,value FROM /root/sub.entries WHERE status = 'active' ORDER BY id desc").get
    assert(r == "List(/root/sub.entries)")
  }

  test("select distinct p.ID, p.status from /region p where p.ID > 5 order by p.status") {
    val r = QueryParser.parseOQL("select distinct p.ID, p.status from /region p where p.ID > 5 order by p.status").get
    assert(r == "List(/region)")
  }

  test("SELECT DISTINCT * FROM /QueryRegion1 r1,  /QueryRegion2 r2 WHERE r1.ID = r2.ID") {
    val r = QueryParser.parseOQL("SELECT DISTINCT * FROM /QueryRegion1 r1,  /QueryRegion2 r2 WHERE r1.ID = r2.ID").get
    assert(r == "List(/QueryRegion1, /QueryRegion2)")
  }

  test("SELECT id, \"type\", positions, status FROM /obj_obj_region WHERE status = 'active'") {
    val r = QueryParser.parseOQL("SELECT id, \"type\", positions, status FROM /obj_obj_region WHERE status = 'active'").get
    println("r.type=" + r.getClass.getName + " r=" + r)
    assert(r == "List(/obj_obj_region)")
  }

  test("SELECT r.id, r.\"type\", r.positions, r.status FROM /obj_obj_region r, r.positions.values f WHERE r.status = 'active' and f.secId = 'MSFT'") {
    val r = QueryParser.parseOQL("SELECT r.id, r.\"type\", r.positions, r.status FROM /obj_obj_region r, r.positions.values f WHERE r.status = 'active' and f.secId = 'MSFT'").get
    assert(r == "List(/obj_obj_region, r.positions.values)")
  }
}
