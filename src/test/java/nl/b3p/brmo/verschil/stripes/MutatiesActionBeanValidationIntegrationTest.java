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

import java.io.IOException;
import nl.b3p.brmo.verschil.testutil.TestUtil;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Draaien met:
 * {@code mvn -Dit.test=MutatiesActionBeanValidationIntegrationTest -Dtest.skipTs=true verify -Ppostgresql > target/pgtests.log}.
 *
 * @author mprins
 */
public class MutatiesActionBeanValidationIntegrationTest extends TestUtil {

    private HttpResponse response;

    @BeforeEach
    @Override
    public void setUp() {
        // dummy
    }

    @Test
    public void testNullVan() throws IOException {
        response = client.execute(new HttpGet(BASE_TEST_URL + "rest/mutaties"));
        String body = EntityUtils.toString(response.getEntity());

        assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusLine().getStatusCode(), "Response status is niet Bad Request.");
        assertNotNull(body, "Response body mag niet null zijn.");
        assertTrue(body.contains("Van is verplicht"), "Response body bevat de verwachte melding niet");
    }

    @Test
    public void testOngeldigeVan() throws IOException {
        response = client.execute(new HttpGet(BASE_TEST_URL + "rest/mutaties?van=18-02-01"));
        String body = EntityUtils.toString(response.getEntity());

        assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusLine().getStatusCode(), "Response status is niet Bad Request.");
        assertNotNull(body, "Response body mag niet null zijn.");
        assertTrue(body.contains("18-02-01 is geen geldige Van"), "Response body bevat de verwachte melding niet");
    }

    @Test
    public void testTotVoorVan() throws IOException {
        response = client.execute(new HttpGet(BASE_TEST_URL + "rest/mutaties?van=2018-02-01&tot=2018-01-01"));
        String body = EntityUtils.toString(response.getEntity());

        assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusLine().getStatusCode(), "Response status is niet Bad Request.");
        assertNotNull(body, "Response body mag niet null zijn.");
        assertTrue(body.contains("`van` datum is voor `tot` datum"), "Response body bevat de verwachte melding niet");
    }

    @Test
    public void testVanTot() throws IOException {
        response = client.execute(new HttpGet(BASE_TEST_URL + "rest/mutaties?van=2018-01-01&tot=2018-01-10"));
        String body = EntityUtils.toString(response.getEntity());

        assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode(), "Response status is niet OK.");
        assertNotNull(body, "Response body mag niet null zijn.");
    }
}
