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
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import static net.javacrumbs.jsonunit.JsonAssert.assertJsonEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PingActionBeanIntegrationTest extends TestUtil {
    private HttpResponse response;

    @BeforeEach
    @Override
    public void setUp() {
        // dummy
    }

    @Test
    public void ping() throws IOException {
        response = client.execute(new HttpGet(BASE_TEST_URL + "rest/ping?tot=2008-04-12"));
        String body = EntityUtils.toString(response.getEntity());

        assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_OK, "Response status is niet OK.");
        assertNotNull(body, "Response body mag niet null zijn.");
        // 1207951200000 == 2008-04-12 00:00
        assertJsonEquals("{\"pong\":1207951200000}", body);
    }
}
