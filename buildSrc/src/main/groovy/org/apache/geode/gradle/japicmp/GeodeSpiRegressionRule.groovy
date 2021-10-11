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

package org.apache.geode.gradle.japicmp

import groovy.json.JsonSlurper
import japicmp.model.JApiCompatibility
import me.champeau.gradle.japicmp.report.Violation
import me.champeau.gradle.japicmp.report.stdrules.AbstractRecordingSeenMembers

class GeodeSpiRegressionRule extends AbstractRecordingSeenMembers {
  private final Map<String, String> acceptedRegressions

  public GeodeSpiRegressionRule() {
    def jsonSlurper = new JsonSlurper()
    acceptedRegressions = jsonSlurper.parse(getClass().getResource('/japicmp_exceptions.json').openStream()) as Map
  }

  @Override
  Violation maybeAddViolation(final JApiCompatibility member) {
    if (!member.isSourceCompatible()) {
      def exception = acceptedRegressions[Violation.describe(member)]
      if (exception) {
        Violation.accept(member, exception)
      } else {
        println("Correct, or add exception for: [${Violation.describe(member)}]")
        Violation.error(member, "Is not source compatible")
      }
    }
  }
}
