/*
 * ========================================================================= (c)
 * Copyright 2002-2007, GemStone Systems, Inc. All Rights Reserved. 1260 NW
 * Waterhouse Ave., Suite 200, Beaverton, OR 97006 All Rights Reserved.
 * ========================================================================
 */
package com.gemstone.gemfire.mgmt.DataBrowser.prefs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.prefs.BackingStoreException;
import java.util.prefs.NodeChangeListener;
import java.util.prefs.PreferenceChangeListener;
import java.util.prefs.Preferences;

/**
 * This is an abstract class that extends java.util.prefs.Preferences and wraps
 * another Preferences object. This class can be extended by other Preferences
 * implementations that want to add new features to wrapped Preferences object.
 * The methods for persisting/retriving objects are added.
 * 
 * @author mjha
 */
public abstract class PreferencesAdapter extends Preferences {
  /* wrapped Preferences object */
  protected Preferences wrapped;

  /**
   * Constructor for wrapping the java.util.Preferences object
   * 
   * @param toBeWrapped
   *                java.util.Preferences object to be wrapped
   */
  public PreferencesAdapter(Preferences toBeWrapped) {
    this.wrapped = toBeWrapped;
  }

  public String absolutePath() {
    return wrapped.absolutePath();
  }

  public void addNodeChangeListener(NodeChangeListener ncl) {
    wrapped.addNodeChangeListener(ncl);
  }

  public void addPreferenceChangeListener(PreferenceChangeListener pcl) {
    wrapped.addPreferenceChangeListener(pcl);
  }

  public String[] childrenNames() throws BackingStoreException {
    return wrapped.childrenNames();
  }

  public void clear() throws BackingStoreException {
    wrapped.clear();
  }

  public void exportNode(OutputStream os) throws IOException,
      BackingStoreException {
    wrapped.exportNode(os);
  }

  public void exportSubtree(OutputStream os) throws IOException,
      BackingStoreException {
    wrapped.exportSubtree(os);
  }

  public void flush() throws BackingStoreException {
    wrapped.flush();
  }

  public String get(String key, String def) {
    return wrapped.get(key, def);
  }

  public boolean getBoolean(String key, boolean def) {
    return wrapped.getBoolean(key, def);
  }

  public byte[] getByteArray(String key, byte[] def) {
    return wrapped.getByteArray(key, def);
  }

  public double getDouble(String key, double def) {
    return wrapped.getDouble(key, def);
  }

  public float getFloat(String key, float def) {
    return wrapped.getFloat(key, def);
  }

  public int getInt(String key, int def) {
    return wrapped.getInt(key, def);
  }

  public long getLong(String key, long def) {
    return wrapped.getLong(key, def);
  }

  public boolean isUserNode() {
    return wrapped.isUserNode();
  }

  public String[] keys() throws BackingStoreException {
    return wrapped.keys();
  }

  public String name() {
    return wrapped.name();
  }

  public Preferences node(String pathName) {
    return wrapped.node(pathName);
  }

  public boolean nodeExists(String pathName) throws BackingStoreException {
    return wrapped.nodeExists(pathName);
  }

  public Preferences parent() {
    return wrapped.parent();
  }

  public void put(String key, String value) {
    wrapped.put(key, value);
  }

  public void putBoolean(String key, boolean value) {
    wrapped.putBoolean(key, value);
  }

  public void putByteArray(String key, byte[] value) {
    wrapped.putByteArray(key, value);
  }

  public void putDouble(String key, double value) {
    wrapped.putDouble(key, value);
  }

  public void putFloat(String key, float value) {
    wrapped.putFloat(key, value);
  }

  public void putInt(String key, int value) {
    wrapped.putInt(key, value);
  }

  public void putLong(String key, long value) {
    wrapped.putLong(key, value);
  }

  public void remove(String key) {
    wrapped.remove(key);
  }

  public void removeNode() throws BackingStoreException {
    wrapped.removeNode();
  }

  public void removeNodeChangeListener(NodeChangeListener ncl) {
    wrapped.removeNodeChangeListener(ncl);
  }

  public void removePreferenceChangeListener(PreferenceChangeListener pcl) {
    wrapped.removePreferenceChangeListener(pcl);
  }

  public void sync() throws BackingStoreException {
    wrapped.sync();
  }

  /**
   * Returns a string representation of the object.
   * 
   * @return a string representation of the object
   */
  public String toString() {
    return this.getClass() + " : " + wrapped.toString();
  }
}
