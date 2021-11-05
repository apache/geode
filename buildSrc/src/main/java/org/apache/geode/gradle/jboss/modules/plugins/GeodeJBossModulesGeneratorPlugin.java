/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.gradle.jboss.modules.plugins;

import org.apache.commons.lang3.StringUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

import org.apache.geode.gradle.jboss.modules.plugins.task.AggregateTestModuleDescriptorsTask;
import org.apache.geode.gradle.jboss.modules.plugins.task.GenerateTestModuleDescriptorsTask;

public class GeodeJBossModulesGeneratorPlugin implements Plugin<Project> {

  @Override
  public void apply(Project project) {
    project.afterEvaluate(project1 -> {
        project1.getTasks()
            .register(getFacetTaskName("generateTestModuleDescriptors", "distributedTest"),
                    (Class) GenerateTestModuleDescriptorsTask.class);

      if (project1.getName().equals("geode-assembly")) {
        project1.getTasks()
            .register(getFacetTaskName("combineTestModuleDescriptors", "distributedTest"),
                AggregateTestModuleDescriptorsTask.class,"distributedTest");
      }
    });
  }

  private String getFacetTaskName(String baseTaskName, String facet) {
    return facet.equals("main") ? baseTaskName : facet + StringUtils.capitalize(baseTaskName);
  }
}
