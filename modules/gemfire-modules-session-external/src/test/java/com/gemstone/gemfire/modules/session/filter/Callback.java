/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.gemstone.gemfire.modules.session.filter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @author jdeppe
 */
public interface Callback {
  public void call(HttpServletRequest request,
      HttpServletResponse response);
}
