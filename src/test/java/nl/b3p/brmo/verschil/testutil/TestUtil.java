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
package nl.b3p.brmo.verschil.testutil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.*;
import org.apache.tomcat.dbcp.dbcp.BasicDataSource;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.Properties;


/**
 * @author mprins
 */
@EnabledIfEnvironmentVariable(named = "database.properties.file", matches = "*.properties")
public abstract class TestUtil {
    private static final Log LOG = LogFactory.getLog(TestUtil.class);
    private static boolean haveSetupJNDI = false;

    /**
     * properties uit {@code <DB smaak>.properties} en
     * {@code local.<DB smaak>.properties}.
     *
     * @see #loadDBprop()
     */
    protected final Properties DBPROPS = new Properties();

    /**
     * onze test client.
     * @see #setUpHttpClient()
     */
    protected static CloseableHttpClient client;

    /**
     * the server root url. {@value}
     */
    public static final String BASE_TEST_URL = "http://localhost:9090/brmo-brkverschil-service/";

    /**
     * set up van custom http client voor de integratie tests.
     */
    @BeforeAll
    public static void setUpHttpClient() {
        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(
                new AuthScope("localhost", 9090),
                new UsernamePasswordCredentials("mutaties", "mutaties")
        );

        client = HttpClients.custom()
                .useSystemProperties()
                .setUserAgent("brmo verschil service integration test")
                .setRedirectStrategy(new LaxRedirectStrategy())
                .setDefaultCookieStore(new BasicCookieStore())
                .setDefaultCredentialsProvider(credsProvider)
                .build();
    }

    @AfterAll
    public static void closeHttpClient() throws IOException {
        client.close();
    }

    /**
     * Subklassen dienen zelf een setup te hebben. Vanwege de overerving gaat
     * deze methode af na de {@code @BeforeEach} methoden van de superklasse, bijvoorbeeld {@link #loadDBprop()}.
     *
     * @throws Exception if any
     */
    @BeforeEach
    abstract public void setUp() throws Exception;

    /**
     * initialize database props using the environment provided file.
     *
     * @throws java.io.IOException if loading the property file fails
     */
    @BeforeEach
    public void loadDBprop() throws IOException {
        // the `database.properties.file`  is set in the pom.xml or using the commandline
        DBPROPS.load(TestUtil.class.getClassLoader()
                .getResourceAsStream(System.getProperty("database.properties.file")));
        try {
            // see if a local version exists and use that to override
            DBPROPS.load(TestUtil.class.getClassLoader()
                    .getResourceAsStream("local." + System.getProperty("database.properties.file")));
        } catch (IOException | NullPointerException e) {
            // ignore this
        }

        try {
            Class driverClass = Class.forName(DBPROPS.getProperty("jdbc.driverClassName"));
        } catch (ClassNotFoundException ex) {
            LOG.error("Database driver niet gevonden.", ex);
        }
    }

    /**
     * Log de naam van de test als deze begint.
     *
     * @param testInfo current test information
     */
    @BeforeEach
    final public void startTest(TestInfo testInfo) {
        LOG.info("==== Start test methode: " + testInfo.getTestMethod());
    }

    /**
     * Log de naam van de test als deze eindigt.
     *
     * @param testInfo current test information
     */
    @AfterEach
    final public void endTest(TestInfo testInfo) {
        LOG.info("==== Einde test methode: " + testInfo.getTestMethod());
    }

    /**
     * setup jndi voor testcases.
     *
     * @param dsRsgb    rsgb databron
     * @param dsStaging staging databron
     */
    protected void setupJNDI(final BasicDataSource dsRsgb, final BasicDataSource dsStaging) {
        if (!haveSetupJNDI) {
            try {
                System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.naming.java.javaURLContextFactory");
                System.setProperty(Context.URL_PKG_PREFIXES, "org.apache.naming");
                InitialContext ic = new InitialContext();
                ic.createSubcontext("java:");
                ic.createSubcontext("java:comp");
                ic.createSubcontext("java:comp/env");
                ic.createSubcontext("java:comp/env/jdbc");
                ic.createSubcontext("java:comp/env/jdbc/brmo");
                ic.bind("java:comp/env/jdbc/brmo/rsgb", dsRsgb);
                ic.bind("java:comp/env/jdbc/brmo/staging", dsStaging);
                haveSetupJNDI = true;
            } catch (NamingException ex) {
                LOG.warn("Opzetten van datasource jndi is mislukt", ex);
            }
        }
    }
}
