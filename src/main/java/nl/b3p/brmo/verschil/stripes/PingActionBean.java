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
