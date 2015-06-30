/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.modules.session;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

/**
 *
 */
public class CommandServlet extends HttpServlet {

    private ServletContext context;

    /**
     * The standard servlet method overridden.
     * @param request
     * @param response
     * @throws IOException 
     */
    @Override
    protected void doGet(HttpServletRequest request,
            HttpServletResponse response) throws IOException, ServletException {

        QueryCommand cmd = QueryCommand.UNKNOWN;
        String param = request.getParameter("param");
        String value = request.getParameter("value");
        PrintWriter out = response.getWriter();

        String cmdStr = request.getParameter("cmd");
        if (cmdStr != null) {
            cmd = QueryCommand.valueOf(cmdStr);
        }

        HttpSession session;

        switch (cmd) {
            case SET:
                session = request.getSession();
                session.setAttribute(param, value);
                break;
            case GET:
                session = request.getSession();
                String val = (String) session.getAttribute(param);
                if (val != null) {
                    out.write(val);
                }
                break;
            case INVALIDATE:
                session = request.getSession();
                session.invalidate();
                break;
            case CALLBACK:
                Callback c = (Callback)context.getAttribute("callback");
                c.call(request, response);
                break;
        }
    }

    /**
     * Save a reference to the ServletContext for later use.
     * @param config 
     */
    @Override
    public void init(ServletConfig config) {
        this.context = config.getServletContext();
    }
}
