/*
 * Copyright (C) 2018 B3Partners B.V.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package nl.b3p.brmo.verschil.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Een servlet filter om BASIC athentication te kunnen gebruiken in een FORM
 * beveiligde webapp, bijvoorbeeld voor losses REST services.
 *
 * Voeg het filter toe in de {@code web.xml}:
 * <pre>
 * {@code
 * <filter>
 *   <display-name>BasicLogin Filter</display-name>
 *   <filter-name>BasicLoginFilter</filter-name>
 *   <filter-class>nl.b3p.brmo.verschil.util.BasicLoginFilter</filter-class>
 *   <init-param>
 *     <description>1 of meer rollen, komma gescheiden</description>
 *     <param-name>auth-role-names</param-name>
 *     <param-value>BRKMutaties</param-value>
 *   </init-param>
 * </filter>
 * <filter-mapping>
 *   <filter-name>BasicLoginFilter</filter-name>
 *   <url-pattern>/rest/*</url-pattern>
 * </filter-mapping>
 * }
 * </pre> Voeg een constraint toe voor de specifieke resource:
 * <pre>
 * {@code
 * <security-constraint>
 *   <display-name>rest endpoints</display-name>
 *   <web-resource-collection>
 *     <web-resource-name>service endpoint</web-resource-name>
 *     <url-pattern>/rest/*</url-pattern>
 *   </web-resource-collection>
 * </security-constraint>
 * }
 * </pre>
 *
 * @author mprins
 */
public class BasicLoginFilter implements Filter {

    private static final Log LOG = LogFactory.getLog(BasicLoginFilter.class);

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BASIC_PREFIX = "Basic ";
    /**
     * List of roles the user must have to authenticate
     */
    private final List<String> roleNames = new ArrayList<>();

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        String roleNamesParam = filterConfig.getInitParameter("auth-role-names");
        if (roleNamesParam != null) {
            for (String roleName : roleNamesParam.split(",")) {
                roleNames.add(roleName.trim());
            }
        }
        LOG.debug("beschikbare rollen voor basic auth: " + String.join(", ", roleNames));
    }

    @Override
    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) req;
        HttpServletResponse response = (HttpServletResponse) resp;

        // haal username and password uit de Authorization header
        String authHeader = request.getHeader(AUTHORIZATION_HEADER);
        if (authHeader == null || !authHeader.startsWith(BASIC_PREFIX)) {
            LOG.debug("BASIC Authorization header ontbreekt (401)");
            this.handle401(response);
            return;
        }

        String userPassBase64 = authHeader.substring(BASIC_PREFIX.length());
        // decode credentials base64
        String credentials = new String(Base64.getDecoder().decode(userPassBase64), StandardCharsets.UTF_8);
        if (!credentials.contains(":")) {
            LOG.debug("geen `:` in user:password combo (401)");
            this.handle401(response);
            return;
        }

        final String[] values = credentials.split(":", 2);
        String authUser = values[0];
        String authPass = values[1];
        try {
            // do login, nodig voor request.isUserInRole(...)
            request.login(authUser, authPass);
        } catch (ServletException ex) {
            this.handle403(response);
            return;
        }

        // check user rollen
        boolean hasRoles = false;
        for (String role : roleNames) {
            if (role == null) {
                continue;
            }
            if (request.isUserInRole(role)) {
                // rol gevonden
                hasRoles = true;
                break;
            }
        }

        if (hasRoles) {
            // login successful en user in rol
            chain.doFilter(request, response);
            // logout when done
            request.logout();
        } else {
            request.logout();
            handle403(response);
            return;
        }
    }

    @Override
    public void destroy() {
        // void
    }

    private void handle401(HttpServletResponse response) throws IOException {
        response.addHeader("WWW-Authenticate", "Basic realm=\"REST service Realm\", charset=\"UTF-8\"");
        response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "BASIC authentication credentials invalid.");
    }

    private void handle403(HttpServletResponse response) throws IOException {
        response.sendError(HttpServletResponse.SC_FORBIDDEN, "Basic login failed.");
    }
}
