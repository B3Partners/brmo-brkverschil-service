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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

public class ConfigUtil {

    private static final Log LOG = LogFactory.getLog(ConfigUtil.class);
    private static final String JNDI_NAME = "java:comp/env";
    private static final String JDBC_NAME_RSGB = "jdbc/brmo/rsgb";
    private static DataSource datasourceRsgb = null;

    private ConfigUtil() {
    }

    public static DataSource getDataSourceRsgb() {
        try {
            if (datasourceRsgb == null) {
                InitialContext ic = new InitialContext();
                Context xmlContext = (Context) ic.lookup(JNDI_NAME);
                datasourceRsgb = (DataSource) xmlContext.lookup(JDBC_NAME_RSGB);
            }
        } catch (NamingException e) {
            LOG.error("Fout opzoeken  rsgb db. ", e);
        }
        return datasourceRsgb;
    }
}
