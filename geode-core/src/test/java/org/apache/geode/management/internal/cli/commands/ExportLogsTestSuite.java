/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management.internal.cli.commands;

import org.apache.geode.management.internal.cli.functions.SizeExportLogsFunctionTest;
import org.apache.geode.management.internal.cli.functions.SizeExportLogsFunctionFileTest;
import org.apache.geode.management.internal.cli.util.LogExporterIntegrationTest;
import org.apache.geode.management.internal.cli.util.LogExporterTest;
import org.apache.geode.management.internal.cli.util.LogSizerTest;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * All of the JUnit, DUnit and Integration tests for gfsh command export logs.
 */

@Ignore
@Suite.SuiteClasses({ExportLogsCommandTest.class, ExportLogsDUnitTest.class,
    SizeExportLogsFunctionTest.class, SizeExportLogsFunctionFileTest.class, LogSizerTest.class,
    LogExporterTest.class, LogExporterIntegrationTest.class, ExportLogsIntegrationTest.class})
@RunWith(Suite.class)
public class ExportLogsTestSuite {
}
