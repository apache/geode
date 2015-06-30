
package com.gemstone.gemfire.modules.session;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Interface which, when implemented, can be put into a servlet context and 
 * executed by the servlet.
 */
public interface Callback {
    
    public void call(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException;
}
