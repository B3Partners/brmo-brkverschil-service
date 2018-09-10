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

import net.sourceforge.stripes.action.*;
import net.sourceforge.stripes.validation.Validate;
import javax.servlet.http.HttpServletResponse;
import java.util.Date;

/**
 * ping actionbean. {@code /rest/ping?tot=2008-04-12}.
 */
@RestActionBean
@UrlBinding("/rest/ping")
public class PingActionBean implements ActionBean {

    private ActionBeanContext context;

    @Validate
    private Date tot = new Date();

    @GET
    @DefaultHandler
    public Resolution get() {
        return new StreamingResolution("application/json") {
            @Override
            public void stream(HttpServletResponse response) throws Exception {
                response.getOutputStream().print("{\"pong\":" + tot.getTime() + "}");
            }
        }.setLastModified(tot.getTime());
    }

    public Date getTot() {
        return tot;
    }

    public void setTot(Date end) {
        this.tot = end;
    }

    public ActionBeanContext getContext() {
        return context;
    }

    public void setContext(ActionBeanContext context) {
        this.context = context;
    }
}
