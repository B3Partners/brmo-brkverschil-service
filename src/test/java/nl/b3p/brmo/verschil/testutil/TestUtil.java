/*
 * Copyright (C) 2018 B3Partners B.V.
 */
package nl.b3p.brmo.verschil.testutil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;
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
     * our test client.
     */
    protected static CloseableHttpClient client;

    /**
     * the server root url. {@value}
     */
    public static final String BASE_TEST_URL = "http://localhost:9090/brmo-brkverschil-service/";

    @BeforeAll
    public static void setUpHttpClient() {
        client = HttpClients.custom()
                .useSystemProperties()
                .setUserAgent("brmo verschil service integration test")
                .setRedirectStrategy(new LaxRedirectStrategy())
                .setDefaultCookieStore(new BasicCookieStore())
                .build();
    }

    @AfterAll
    public static void closeHttpClient() throws IOException {
        client.close();
    }

    /**
     * subklassen dienen zelf een setup te hebben; vanwege de overerving gaat
     * deze methode af na de {@code @Before} methoden van de superklasse, bijv.
     * {@link #loadDBprop()}.
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
     */
    @BeforeEach
    public void startTest(TestInfo testInfo) {
        LOG.info("==== Start test methode: " + testInfo.getTestMethod());
    }

    /**
     * Log de naam van de test als deze eindigt.
     */
    @AfterEach
    public void endTest(TestInfo testInfo) {
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
