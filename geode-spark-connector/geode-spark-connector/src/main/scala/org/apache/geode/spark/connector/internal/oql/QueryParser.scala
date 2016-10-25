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

import scala.util.parsing.combinator.RegexParsers

class QueryParser extends RegexParsers {

  def query: Parser[String] = opt(rep(IMPORT ~ PACKAGE)) ~> select ~> opt(distinct) ~> projection ~> from ~> regions <~ opt(where ~ filter) ^^ {
    _.toString
  }

  val IMPORT: Parser[String] = "[Ii][Mm][Pp][Oo][Rr][Tt]".r

  val select: Parser[String] = "[Ss][Ee][Ll][Ee][Cc][Tt]".r

  val distinct: Parser[String] = "[Dd][Ii][Ss][Tt][Ii][Nn][Cc][Tt]".r

  val from: Parser[String] = "[Ff][Rr][Oo][Mm]".r

  val where: Parser[String] = "[Ww][Hh][Ee][Rr][Ee]".r

  def PACKAGE: Parser[String] = """[\w.]+""".r

  def projection: Parser[String] = "*" | repsep("""["\w]+[.\w"]*""".r, ",") ^^ {
    _.toString
  }

  def regions: Parser[String] = repsep(region <~ opt(alias), ",") ^^ {
    _.toString
  }

  def region: Parser[String] = """/[\w.]+[/[\w.]+]*""".r | """[\w]+[.\w]*""".r

  def alias: Parser[String] = not(where) ~> """[\w]+""".r

  def filter: Parser[String] = """[\w.]+[[\s]+[<>=.'\w]+]*""".r
}

object QueryParser extends QueryParser {

  def parseOQL(expression: String) = parseAll(query, expression)

}
