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

import me.champeau.gradle.japicmp.report.Violation
import me.champeau.gradle.japicmp.report.stdrules.AbstractRecordingSeenMembers
import japicmp.model.JApiMethod
import japicmp.model.JApiClass
import japicmp.model.JApiSuperclass
import japicmp.model.JApiConstructor
import japicmp.model.JApiCompatibility


class ParentIsExperimental extends AbstractRecordingSeenMembers {
  @Override
  Violation maybeAddViolation(final JApiCompatibility member) {
    boolean isExperimental = true
    if (!member.isBinaryCompatible() || !member.isSourceCompatible()) {
      if (member instanceof JApiMethod || member instanceof JApiConstructor) {
        isExperimental = isClassExperimental(member.jApiClass)
      } else if (member instanceof JApiClass) {
        isExperimental = isClassExperimental(member)
      }

      if (isExperimental) {
        return Violation.accept(member, "Parent class is @Experimental")
      }
    }
  }

  boolean isClassExperimental(final JApiClass member) {
    boolean isExperimental = false
    if (!member.annotations.find {
      it.fullyQualifiedName == 'org.apache.geode.annotations.Experimental'
    }) {
      JApiSuperclass sup = member.superclass
      if (sup.getJApiClass().present) {
        isExperimental = isClassExperimental(sup.getJApiClass().get())
      } else {
      }
    } else {
      isExperimental = true
    }
    return isExperimental
  }
}
