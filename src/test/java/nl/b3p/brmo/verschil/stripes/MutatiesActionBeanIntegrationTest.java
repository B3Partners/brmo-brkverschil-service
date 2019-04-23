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
package nl.b3p.brmo.verschil.stripes;

import nl.b3p.brmo.verschil.testutil.TestUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.tomcat.dbcp.dbcp.BasicDataSource;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseDataSourceConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.ext.postgresql.PostgresqlDataTypeFactory;
import org.junit.Ignore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Draaien met:
 * {@code mvn -Dit.test=MutatiesActionBeanIntegrationTest -Dtest.skipTs=true verify -Ppostgresql > target/pgtests.log}
 *
 * @author mark
 */
public class MutatiesActionBeanIntegrationTest extends TestUtil {

    private static final Log LOG = LogFactory.getLog(MutatiesActionBeanIntegrationTest.class);
    private final Lock sequential = new ReentrantLock(true);
    private IDatabaseConnection rsgb;
    private HttpResponse response = null;

    private final Map<String, String> jsonNames = new HashMap<String, String>() {{
        put("NieuweOnroerendGoed.json", "nieuw");
        put("GekoppeldeObjecten.json", "koppeling");
        put("VervallenOnroerendGoed.json", "vervallen");
        put("GewijzigdeOpp.json", "gewijzigdeopp");
        put("Verkopen.json", "verkopen");
        put("NieuweSubjecten.json", "nieuwe_subjecten");
        put("BsnAangevuld.json", "bsnaangevuld");
    }};

    private final List<String> cvsNames = Arrays.asList(new String[]{
            "NieuweOnroerendGoed.csv",
            "GekoppeldeObjecten.csv",
            "VervallenOnroerendGoed.csv",
            "GewijzigdeOpp.csv",
            "Verkopen.csv",
            "NieuweSubjecten.csv",
            "BsnAangevuld.csv"
    });

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        BasicDataSource dsRsgb = new BasicDataSource();
        dsRsgb.setUrl(DBPROPS.getProperty("rsgb.url"));
        dsRsgb.setUsername(DBPROPS.getProperty("rsgb.username"));
        dsRsgb.setPassword(DBPROPS.getProperty("rsgb.password"));
        dsRsgb.setAccessToUnderlyingConnectionAllowed(true);
        dsRsgb.setConnectionProperties(DBPROPS.getProperty("rsgb.options", ""));

        rsgb = new DatabaseDataSourceConnection(dsRsgb);

        setupJNDI(dsRsgb, null);

        rsgb.getConfig().setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new PostgresqlDataTypeFactory());

        sequential.lock();
    }

    @AfterEach
    public void cleanup() throws Exception {
        response = null;
        rsgb.close();
        sequential.unlock();
    }

    /**
     * test of er een geldige zipfile met de genoemde json bestanden uit de service komt.
     *
     * @throws IOException if any
     */
    @Test
    public void testValidZipforJsonReturned() throws IOException {
        response = client.execute(new HttpGet(BASE_TEST_URL + "rest/mutaties?van=2018-12-01&tot=2019-01-01&f=json"));

        assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode(), "Response status is niet OK.");

        int filesInZip = 0;
        try (ZipInputStream zis = new ZipInputStream(response.getEntity().getContent())) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                LOG.debug("file: " + entry.getName() + ", compressed size: " + entry.getCompressedSize());
                assertTrue(jsonNames.containsKey(entry.getName()), "De verwachte sleutel komt niet voor");
                StringBuilder actual = new StringBuilder();
                byte[] buffer = new byte[1024];
                int read = 0;
                while (zis.available() > 0) {
                    while ((read = zis.read(buffer, 0, 1024)) >= 0) {
                        actual.append(new String(buffer, 0, read));
                    }
                }
                // TODO check inhoud van entry bijvoorbeeld {"bsnaangevuld":[]}
                //assertJsonEquals("{\"" + jsonNames.get(entry.getName()) + "\":[]}", actual.toString());

                // check dat de node begint met bijv. '{"bsnaangevuld":['
                assertTrue(actual.toString().startsWith("{\"" + jsonNames.get(entry.getName()) + "\":["));
                LOG.trace("json data dump voor: " + entry.getName() + "\n##################\n" + actual);
                filesInZip++;
            }
        }
        assertEquals(jsonNames.size(), filesInZip, "Onverwacht aantal json files in zipfile");

        // see: https://stackoverflow.com/questions/2085637/how-to-check-if-a-generated-zip-file-is-corrupted
    }

    /**
     * test of er een geldige zipfile met de genoemde csv bestanden uit de service komt.
     *
     * @throws IOException if any
     */
    @Test
    @Ignore
    public void testValidZipforCSVReturned() throws IOException {
        response = client.execute(new HttpGet(BASE_TEST_URL + "rest/mutaties?van=2018-12-01&tot=2019-02-01&f=csv"));

        assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode(), "Response status is niet OK.");

        int filesInZip = 0;
        try (ZipInputStream zis = new ZipInputStream(response.getEntity().getContent())) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                LOG.debug("file: " + entry.getName() + ", compressed size: " + entry.getCompressedSize());
                assertTrue(cvsNames.contains(entry.getName()), "De verwachte sleutel komt niet voor");
                StringBuilder actual = new StringBuilder("csv data dump voor: " + entry.getName() + "\n##################\n");
                byte[] buffer = new byte[1024];
                int read = 0;
                while (zis.available() > 0) {
                    while ((read = zis.read(buffer, 0, 1024)) >= 0) {
                        actual.append(new String(buffer, 0, read));
                    }
                }
                LOG.trace(actual);
                // TODO evt testen op kolom headers, bijv. voor NieuweSubjecten.csv
                // begin_geldigheid;soort;geslachtsnaam;voorvoegsel;voornamen;naam;woonadres;geboortedatum;overlijdensdatum;bsn;rsin;kvk_nummer;straatnaam;huisnummer;huisletter;huisnummer_toev;postcode;woonplaats
                filesInZip++;
            }
        }
        assertEquals(cvsNames.size(), filesInZip, "Onverwacht aantal csv files in zipfile");
    }
}
