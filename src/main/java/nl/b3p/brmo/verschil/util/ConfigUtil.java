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
