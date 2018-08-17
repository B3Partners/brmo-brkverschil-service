package nl.b3p.brmo.verschil.stripes;

import net.sourceforge.stripes.action.*;
import net.sourceforge.stripes.validation.Validate;
import nl.b3p.brmo.verschil.util.ConfigUtil;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Mutaties actionbean. Voorbeeld url: {@code /rest/mutaties?van=2018-08-01} of
 * {@code /rest/mutaties?van=2018-08-01&tot=2018-09-01} .
 */
@RestActionBean
@UrlBinding("/rest/{location}")
public class MutatiesActionBean implements ActionBean {

    private static final Log LOG = LogFactory.getLog(MutatiesActionBean.class);

    /**
     * verplichte datum begin periode. Datum in yyyy-mm-dd formaat.
     */
    @Validate
    private Date van;

    /**
     * optionele datum einde periode. Datum in yyyy-mm-dd formaat.
     */
    @Validate
    private Date tot = new Date();

    private ActionBeanContext context;
    private final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    private long copied;

    @GET
    @DefaultHandler
    public Resolution get() throws IOException {
        LOG.debug("get met params: van=" + df.format(van) + " tot=" + df.format(tot));

        // maak werkdirectory en werkbestand
        Path workPath = Files.createTempDirectory(Paths.get(System.getProperty("java.io.tmpdir")), "brkmutsvc", PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-x---")));
        File workDir = workPath.toFile();
        workDir.deleteOnExit();
        File workZip = Files.createTempFile("brkmutsvc", ".zip").toFile();
        workZip.deleteOnExit();

        // uitvoeren queries
        try {
            long nwOnrrgd = this.getNieuweOnroerendGoed(workDir);
        } catch (SQLException e) {
            LOG.error(e);
        }

        // zippen resultaat in workZip
        try (ZipOutputStream zs = new ZipOutputStream(Files.newOutputStream(workZip.toPath()))) {
            Files.walk(workPath)
                    .filter(path -> !Files.isDirectory(path))
                    .forEach(path -> {
                        ZipEntry zipEntry = new ZipEntry(workPath.relativize(path).toString());
                try {
                    LOG.debug("Putting file: " + zipEntry);
                            zs.putNextEntry(zipEntry);
                            Files.copy(path, zs);
                            zs.closeEntry();
                        } catch (IOException e) {
                            System.err.println(e);
                        }
                    });
        }

        return new StreamingResolution("application/zip") {
            @Override
            public void stream(HttpServletResponse response) throws Exception {
                copied = FileUtils.copyFile(workZip, response.getOutputStream());
                LOG.debug("copied " + copied);

                FileUtils.deleteQuietly(workDir);
                FileUtils.deleteQuietly(workZip);
            }
        }.setFilename("mutaties_" + df.format(van) + "_" + df.format(tot) + ".zip")
                .setLastModified(tot.getTime())
                .setAttachment(false)
 //.setLength(copied)
                ;
    }

    /**
     * ophalen nieuwe percelen en appartementsrechten. [2.3].
     * 
     *
     *
     * @return aantal nieuw
     * @throws SQLException als de openen van de database mislukt of er een
     * andere database fout optreedt
     * @throws FileNotFoundException als de werkdirectory niet gevonden kan
     * worden
     * @throws IOException als openen/sluiten van het werkbestand mislukt
     *
     */
    private long getNieuweOnroerendGoed(File workDir) throws SQLException, FileNotFoundException, IOException {

        ResultSet rs = null;
        long count = -1;
        try (Connection c = ConfigUtil.getDataSourceRsgb().getConnection()) {
            c.setReadOnly(true);
            // gebruik evt json functie om json uit de database te krijgen
            // zie: https://www.postgresql.org/docs/9.6/static/functions-json.html
            // en https://stackoverflow.com/questions/24006291/postgresql-return-result-set-as-json-array

            // zie: https://www.postgresql.org/docs/9.6/static/rangetypes.html
            // objecten met datum begin geldigheid in de periode "van"/"tot" inclusief, maar niet in de archief tabel met een datum voor "van".
            StringBuilder sql = new StringBuilder("select ")
                    .append("kad_identif")
                    .append(",dat_beg_geldh")
                    .append(" from kad_onrrnd_zk where '[")
                    .append(df.format(van))
                    .append(",")
                    .append(df.format(tot))
                    .append("]'::daterange @> dat_beg_geldh::date ")
                    .append(" and kad_identif not in (select kad_identif from kad_onrrnd_zk_archief where '")
                    .append(df.format(van))
                    .append("'::date < dat_beg_geldh::date)");

            try (PreparedStatement stm = c.prepareStatement(sql.toString())) {
                stm.setFetchDirection(ResultSet.FETCH_FORWARD);
                stm.setFetchSize(1000);

                LOG.trace(stm);
                try (ResultSet r = stm.executeQuery()) {
                    PrintWriter w = new PrintWriter(
                            new OutputStreamWriter(
                                    new BufferedOutputStream(
                                            new FileOutputStream(workDir + File.separator + "NieuweOnroerendGoed.csv")),
                                    "UTF-8")
                    );
                    count = 0;
                    while (r.next()) {
                        count++;
                        w.append(r.getString(1))
                                .append(",")
                                .append(r.getString(2))
                                .println();
                    }
                    w.flush();
                    w.close();
                }

            }
            LOG.debug("aantal nieuwe onroerende zaken is: " + count);
        }
        return count;

    }

    /**
     * ophalen gekoppelde objecten [2.4]
     */
    private long getGekoppeldeObjecten() {
        long count = -1;
        return count;
    }

    /**
     * ophalen vervallen percelen en appartementsrechten. [2.5]
     */
    private long getVervallenOnroerendGoed() {
        long count = -1;
        return count;
    }

    /**
     * ophalen gewijzigde oppervlakte. [2.6]
     */
    private long getGewijzigdeOpp() {
        long count = -1;
        return count;
    }

    /**
     * ophalen verkopen [2.6].
     */
    private long getVerkopen() {
        DataSource rsgb = ConfigUtil.getDataSourceRsgb();
        long count = -1;
        return count;
    }

    public ActionBeanContext getContext() {
        return context;
    }

    public void setContext(ActionBeanContext context) {
        this.context = context;
    }

    public Date getVan() {
        return van;
    }

    public void setVan(Date van) {
        this.van = van;
    }

    public Date getTot() {
        return tot;
    }

    public void setTot(Date tot) {
        this.tot = tot;
    }
}
