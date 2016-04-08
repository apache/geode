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
package com.gemstone.gemfire.test.golden;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

/**
 * Custom GoldenComparator which returns the string of the golden file name
 * as the output for simple unit testing of the quickstart testing framework.
 * 
 */
public class GoldenStringComparator extends RegexGoldenComparator {

  protected GoldenStringComparator(final String[] expectedProblemLines) {
    super(expectedProblemLines);
  }
  
  @Override
  protected Reader readGoldenFile(final String goldenFileName) throws IOException {
    return new StringReader(goldenFileName);
  }
}
