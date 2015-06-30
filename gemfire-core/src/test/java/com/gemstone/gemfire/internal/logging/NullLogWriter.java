package com.gemstone.gemfire.internal.logging;

import java.util.logging.Handler;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.i18n.LogWriterI18n;

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
