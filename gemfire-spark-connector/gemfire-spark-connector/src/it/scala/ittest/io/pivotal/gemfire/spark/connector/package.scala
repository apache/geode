package ittest.io.pivotal.gemfire.spark

import org.scalatest.Tag

package object connector {

  object OnlyTest extends Tag("OnlyTest")
  object FetchDataTest extends Tag("FetchDateTest")
  object FilterTest extends Tag("FilterTest")
  object JoinTest extends Tag("JoinTest")
  object OuterJoinTest extends Tag("OuterJoinTest")  
  
}
