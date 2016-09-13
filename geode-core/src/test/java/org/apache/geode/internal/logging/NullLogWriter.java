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
package org.apache.geode.internal.logging;

import java.util.logging.Handler;

import org.apache.geode.LogWriter;
import org.apache.geode.i18n.LogWriterI18n;

public class NullLogWriter implements LogWriter {
  @Override
  public boolean severeEnabled() {
    return false;
  }
  @Override
  public void severe(String msg, Throwable ex) {
  }
  @Override
  public void severe(String msg) {
  }
  @Override
  public void severe(Throwable ex) {
  }
  @Override
  public boolean errorEnabled() {
    return false;
  }
  @Override
  public void error(String msg, Throwable ex) {
  }
  @Override
  public void error(String msg) {
  }
  @Override
  public void error(Throwable ex) {
  }
  @Override
  public boolean warningEnabled() {
    return false;
  }
  @Override
  public void warning(String msg, Throwable ex) {
  }
  @Override
  public void warning(String msg) {
  }
  @Override
  public void warning(Throwable ex) {
  }
  @Override
  public boolean infoEnabled() {
    return false;
  }
  @Override
  public void info(String msg, Throwable ex) {
  }
  @Override
  public void info(String msg) {
  }
  @Override
  public void info(Throwable ex) {
  }
  @Override
  public boolean configEnabled() {
    return false;
  }
  @Override
  public void config(String msg, Throwable ex) {
  }
  @Override
  public void config(String msg) {
  }
  @Override
  public void config(Throwable ex) {
  }
  @Override
  public boolean fineEnabled() {
    return false;
  }
  @Override
  public void fine(String msg, Throwable ex) {
  }
  @Override
  public void fine(String msg) {
  }
  @Override
  public void fine(Throwable ex) {
  }
  @Override
  public boolean finerEnabled() {
    return false;
  }
  @Override
  public void finer(String msg, Throwable ex) {
  }
  @Override
  public void finer(String msg) {
  }
  @Override
  public void finer(Throwable ex) {
  }
  @Override
  public void entering(String sourceClass, String sourceMethod) {
  }
  @Override
  public void exiting(String sourceClass, String sourceMethod) {
  }
  @Override
  public void throwing(String sourceClass, String sourceMethod, Throwable thrown) {
  }
  @Override
  public boolean finestEnabled() {
    return false;
  }
  @Override
  public void finest(String msg, Throwable ex) {
  }
  @Override
  public void finest(String msg) {
  }
  @Override
  public void finest(Throwable ex) {
  }
  @Override
  public Handler getHandler() {
    return null;
  }
  @Override
  public LogWriterI18n convertToLogWriterI18n() {
    return null;
  }
}
