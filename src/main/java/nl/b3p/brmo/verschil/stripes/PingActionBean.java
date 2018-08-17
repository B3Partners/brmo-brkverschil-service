package nl.b3p.brmo.verschil.stripes;

import java.util.Date;
import net.sourceforge.stripes.action.ActionBean;
import net.sourceforge.stripes.action.ActionBeanContext;
import net.sourceforge.stripes.action.DefaultHandler;
import net.sourceforge.stripes.action.GET;
import net.sourceforge.stripes.action.JsonResolution;
import net.sourceforge.stripes.action.Resolution;
import net.sourceforge.stripes.action.RestActionBean;
import net.sourceforge.stripes.action.UrlBinding;
import net.sourceforge.stripes.validation.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ping actionbean. {@code /rest/ping?end=2008-04-12}.
 */
@RestActionBean
@UrlBinding("/rest/ping")
public class PingActionBean implements ActionBean {
    private static final Log LOG = LogFactory.getLog(PingActionBean.class);

    private ActionBeanContext context;

    @Validate
    private Date end = new Date();

    @GET
    @DefaultHandler
    public Resolution get(){
        LOG.debug("received get, datum: " + end);
        String[] j = new String[]{"get done!", end.toString()};

        return new JsonResolution(j);
    }
    public Date getEnd() {
        return end;
    }

    public void setEnd(Date end) {
        this.end = end;
    }

    public ActionBeanContext getContext() {
        return context;
    }

    public void setContext(ActionBeanContext context) {
        this.context = context;
    }
}
