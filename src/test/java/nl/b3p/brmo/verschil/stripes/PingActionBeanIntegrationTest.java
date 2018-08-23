/*
 * Copyright (C) 2018 B3Partners B.V.
 */
package nl.b3p.brmo.verschil.stripes;

import nl.b3p.brmo.verschil.testutil.TestUtil;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class PingActionBeanIntegrationTest extends TestUtil {


    /**
     * onze test response.
     */
    private HttpResponse response;

    @BeforeEach
    public void setUp() {

    }

    @Test
    public void ping() throws IOException {
        response = client.execute(new HttpGet(BASE_TEST_URL + "rest/ping?end=2008-04-12"));

        String body = EntityUtils.toString(response.getEntity());

        assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_OK, "Response status is OK.");

        assertNotNull(body, "Response body mag niet null zijn.");
        System.out.println(body);

        // TODO gebruik jsonunit
        assertTrue(body.contains("\"get done!\""));
        assertTrue(body.contains("\"Sat Apr 12 00:00:00 CEST 2008\""));
    }
}
