/*
 * Copyright (C) 2018 B3Partners B.V.
 */
package nl.b3p.brmo.verschil.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.servlet.*;
import javax.sql.DataSource;
import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.util.Locale;
import java.util.Properties;

/**
 *
 * @author mprins
 */
public class DiagnosticsServlet implements Servlet {

    private static final Log LOG = LogFactory.getLog(DiagnosticsServlet.class);

    @Override
    public void init(ServletConfig config) throws ServletException {

        try {
            // lookup db connectie en log info
            DataSource rsgb = ConfigUtil.getDataSourceRsgb();
            DatabaseMetaData metadata = rsgb.getConnection().getMetaData();
            LOG.info(String.format("\nDatabase en driver informatie\n\n  Database product: %s\n  Database version: %s\n  Database major:   %s\n  Database minor:   %s\n\n  DBdriver product: %s\n  DBdriver version: %s\n  DBdriver major:   %s\n  DBdriver minor:   %s",
                    metadata.getDatabaseProductName(),
                    metadata.getDatabaseProductVersion().replace('\n', ' '),
                    metadata.getDatabaseMajorVersion(),
                    metadata.getDatabaseMinorVersion(),
                    metadata.getDriverName(),
                    metadata.getDriverVersion(),
                    metadata.getDriverMajorVersion(),
                    metadata.getDriverMinorVersion()
            ));

            metadata.getConnection().close();
        } catch (Exception ex) {
            LOG.error(ex);
        }

        final Properties props = new Properties();
        String appVersie = "onbekend";
        try {
            props.load(DiagnosticsServlet.class.getClassLoader().getResourceAsStream("git.properties"));
            appVersie = props.getProperty("builddetails.build.version", "onbekend");
        } catch (IOException ex) {
            String name = config.getServletContext().getContextPath();
            name = name.startsWith("/") ? name.substring(1).toUpperCase(Locale.ROOT) : "ROOT";
            LOG.warn("Ophalen " + name + " applicatie versie informatie is mislukt.", ex);
        }
        LOG.info("BRMO BRK verschilservice versie is: "+ appVersie);
    }

    @Override
    public ServletConfig getServletConfig() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getServletInfo() {
        return "Diagnostics info logging servlet";
    }

    @Override
    public void destroy() {
        // nothing
    }
}
