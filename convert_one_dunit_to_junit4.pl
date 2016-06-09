#!/usr/bin/perl -w -i -p
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

use strict;

my $filename = $ARGV;
$filename =~ s/.*\/(.*).java/$1/;

#Change constructor to a no arg constructor
s/(public.*$filename)\(String name\)/$1()/;
s/super\(name\)/super()/;

#Add @Test annotation
s/^[^\/]*public void test/  \@Test\n  public void test/;

#Extend the correct junit 4 class
s/extends CacheTestCase/extends JUnit4CacheTestCase/;
s/extends DistributedTestCase/extends JUnit4DistributedTestCase/;

#Add the DistributedTest category
s/(^public class $filename)/\@Category(DistributedTest.class)\n$1/;

#Change calls to new MyTest("string") to use the no arg constructor
s/new $filename\(\".*?\"\)/new $filename\(\)/;

#Add imports
s/^(package com.gemstone.*)/$1\n\nimport org.junit.experimental.categories.Category;\nimport org.junit.Test;\n\nimport static org.junit.Assert.*;\n\nimport com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;\nimport com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;\nimport com.gemstone.gemfire.test.junit.categories.DistributedTest;/

