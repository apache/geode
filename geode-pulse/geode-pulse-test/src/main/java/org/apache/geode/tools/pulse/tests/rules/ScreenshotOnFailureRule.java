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
 *
 */

package org.apache.geode.tools.pulse.tests.rules;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.apache.commons.io.FileUtils;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebDriver;

public class ScreenshotOnFailureRule extends TestWatcher {

  private Supplier<WebDriver> webDriverSupplier;

  public ScreenshotOnFailureRule(final Supplier<WebDriver> provider) {
    this.webDriverSupplier = provider;
  }

  @Override
  public void failed(Throwable t, Description test) {
    takeScreenshot(test.getDisplayName());
  }

  private void takeScreenshot(String screenshotName) {
    WebDriver driver = this.webDriverSupplier.get();
    if (driver instanceof TakesScreenshot) {
      File tempFile = ((TakesScreenshot) driver).getScreenshotAs(OutputType.FILE);
      try {
        File screenshot = new File("build/screenshots/" + screenshotName + ".png");
        FileUtils.copyFile(tempFile, screenshot);
        System.err.println("Screenshot saved to: " + screenshot.getCanonicalPath());
      } catch (IOException e) {
        throw new Error(e);
      }
    }
  }



}
