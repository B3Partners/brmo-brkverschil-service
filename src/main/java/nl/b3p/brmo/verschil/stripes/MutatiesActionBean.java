/*
 * Copyright (C) 2018 B3Partners B.V.
 */
package nl.b3p.brmo.verschil.stripes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.sourceforge.stripes.action.*;
import net.sourceforge.stripes.validation.Validate;
import nl.b3p.brmo.verschil.util.ConfigUtil;
import nl.b3p.brmo.verschil.util.ResultSetSerializer;
import nl.b3p.brmo.verschil.util.ResultSetSerializerException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
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
    private final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
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
        long nwOnrrgd = this.getNieuweOnroerendGoed(workDir);
        LOG.debug("aantal nieuwe onroerende zaken is: " + nwOnrrgd);


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
     * @param workDir directory waar json resultaat wordt neergezet
     * @return aantal nieuw
     */
    private long getNieuweOnroerendGoed(File workDir) {
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

        /* TODO kolommen:
         * - oppervlakte
         * - x-coordinaat
         * - y-coordinaat
         * - kadastrale aanduiding? (kadastraal object nummer)
         * - KPR nummer
         * - aandeel
         * - soort recht
         * - ontstaan uit
         * via left join app_re en kad_perceel
         * */

        return queryToJson(workDir, "NieuweOnroerendGoed.csv", sql.toString());
    }

    /**
     * @param workDir      directory waar json resultaat wordt neergezet
     * @param bestandsNaam naam van resultaat bestand
     * @param sql          query
     * @param params       optionele prepared statement params
     * @return aantal verwerkte records of -1 in geval van een fout
     */
    private long queryToJson(File workDir, String bestandsNaam, String sql, String... params) {
        long count = -1;
        try (Connection c = ConfigUtil.getDataSourceRsgb().getConnection()) {
            c.setReadOnly(true);

            try (PreparedStatement stm = c.prepareStatement(sql)) {
                int index = 0;
                for (String p : params) {
                    stm.setString(index++, p);
                }

                stm.setFetchDirection(ResultSet.FETCH_FORWARD);
                stm.setFetchSize(1000);
                LOG.trace(stm);

                SimpleModule module = new SimpleModule();
                ResultSetSerializer serializer = new ResultSetSerializer();
                ObjectMapper mapper = new ObjectMapper();

                try (ResultSet r = stm.executeQuery()) {
                    module.addSerializer(serializer);
                    mapper.registerModule(module);
                    ObjectNode objectNode = mapper.createObjectNode();
                    objectNode.putPOJO("results", r);

                    mapper.writeValue(new FileOutputStream(workDir + File.separator + bestandsNaam), objectNode);
                }
                count = serializer.getCount();
            }

        } catch (SQLException | FileNotFoundException | ResultSetSerializerException e) {
            LOG.error(
                    String.format("Fout tijdens ophalen en uitschrijven gegevens (sql: %s, bestand: %s %s",
                            sql,
                            workDir,
                            bestandsNaam)
                    , e);
        } finally {
            return count;
        }
    }

    /**
     * ophalen gekoppelde objecten [2.4]
     */
    private long getGekoppeldeObjecten() {
        long count = -1;

        /* TODO
        - lijst van nieuwe percelen
        - adres
        - bagid adresaanduiding
         */
        return count;
    }

    /**
     * ophalen vervallen percelen en appartementsrechten. [2.5]
     */
    private long getVervallenOnroerendGoed() {
        long count = -1;
        // mogelijk snelst om </empty> berichten in de periode op te zoeken in de staging? daar hant de objectref aan.

        return count;
    }

    /**
     * ophalen gewijzigde oppervlakte. [2.6]
     */
    private long getGewijzigdeOpp() {
        long count = -1;
        // alle percelen die aangepast zijn in de periode waarvan de oppervlakte van de oudste en de jongste een verschillende oppervlakte hebben
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
