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
        long verkopen = this.getVerkopen(workDir);
        LOG.debug("aantal verkopen: " + verkopen);


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
                LOG.debug("bytes copied: " + copied);

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
        StringBuilder sql = new StringBuilder("SELECT ")
                .append("distinct o.kad_identif, ")
                .append("o.dat_beg_geldh, ")
                // TODO kadastrale aanduiding? (kadastraal object nummer) evt samenvoegen
                .append("q.ka_kad_gemeentecode, ")
                .append("q.ka_perceelnummer, ")
                .append("q.ka_deelperceelnummer, ")
                .append("q.ka_sectie, ")
                .append("q.ka_appartementsindex, ")
                .append("q.grootte_perceel, ")
                .append("q.x, ")
                .append("q.y, ")
                .append("b.bsn, ")
                .append("z.ar_teller, ")
                .append("z.ar_noemer, ")
                .append("z.fk_3avr_aand, ")
                .append("avr.omschr_aard_verkregenr_recht ")
                // TODO kolom: "ontstaan uit"
                .append("from kad_onrrnd_zk o ")
                // samengestelde app_re en kad_perceel als q
                .append("left join (select  ")
                .append("ar.sc_kad_identif, ")
                .append("ar.ka_kad_gemeentecode, ")
                .append("ar.ka_perceelnummer, ")
                .append("null as ka_deelperceelnummer, ")
                .append("ar.ka_sectie, ")
                .append("ar.ka_appartementsindex, ")
                .append("null as grootte_perceel, ")
                .append("null as x, ")
                .append("null as y ")
                .append("from app_re ar ")
                .append("union all select ")
                .append("p.sc_kad_identif, ")
                .append("p.ka_kad_gemeentecode, ")
                .append("p.ka_perceelnummer, ")
                .append("p.ka_deelperceelnummer, ")
                .append("p.ka_sectie, ")
                .append("null as ka_appartementsindex, ")
                .append("p.grootte_perceel, ")
                .append("ST_X(p.plaatscoordinaten_perceel) as x, ")
                .append("ST_Y(p.plaatscoordinaten_perceel) as y ")
                .append("from kad_perceel p) q ")
                // einde samenstelling app_re en kad_perceel als q
                .append("on o.kad_identif=q.sc_kad_identif ")
                // zakelijk recht erbij
                .append("left join zak_recht z on o.kad_identif=z.fk_7koz_kad_identif ")
                .append("left join aard_verkregen_recht avr on z.fk_3avr_aand=avr.aand ")
                // BKP erbij
                .append("join belastingplichtige b on ( ")
                .append("q.ka_kad_gemeentecode=b.ka_kad_gemeentecode ")
                .append("and q.ka_sectie=b.ka_sectie ")
                .append("and q.ka_perceelnummer=b.ka_perceelnummer ")
                .append("and coalesce(q.ka_deelperceelnummer,'')=coalesce(b.ka_deelperceelnummer,'') ")
                .append("and coalesce(q.ka_appartementsindex,'')=coalesce(b.ka_appartementsindex,'') )")
                // zie: https://www.postgresql.org/docs/9.6/static/rangetypes.html
                // objecten met datum begin geldigheid in de periode "van"/"tot" inclusief, maar niet in de archief tabel met een datum voor "van".
                .append("WHERE '[")
                .append(df.format(van))
                .append(",")
                .append(df.format(tot))
                .append("]'::daterange @> dat_beg_geldh::date ")
                .append(" and kad_identif not in (select kad_identif from kad_onrrnd_zk_archief where '")
                .append(df.format(van))
                .append("'::date < dat_beg_geldh::date) ")
                .append("and z.fk_8pes_sc_identif is not null");

        return queryToJson(workDir, "NieuweOnroerendGoed.json", sql.toString());
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
     *
     * @param workDir directory waar json resultaat wordt neergezet
     * @return aantal verkopen
     */
    private long getVerkopen(File workDir) {
        StringBuilder sql = new StringBuilder("SELECT ")
                .append("distinct b.ref_id, ")
                .append("b.datum::text, ")
                //
                .append("q.ka_kad_gemeentecode, ")
                .append("q.ka_sectie, ")
                .append("q.ka_perceelnummer, ")
                .append("q.ka_deelperceelnummer, ")
                .append("q.ka_appartementsindex, ")
                //
                .append("k.bsn, ")
                .append("z.ar_teller, ")
                .append("z.ar_noemer, ")
                .append("z.fk_3avr_aand, ")
                .append("avr.omschr_aard_verkregenr_recht ")

                .append("FROM ( SELECT brondocument.ref_id, max(brondocument.datum) AS datum FROM brondocument WHERE brondocument.omschrijving = 'Akte van Koop en Verkoop' GROUP BY brondocument.ref_id) b ")

                // samengestelde app_re en kad_perceel als q
                .append("left join (select  ")
                .append("ar.sc_kad_identif, ")
                .append("ar.ka_kad_gemeentecode, ")
                .append("ar.ka_perceelnummer, ")
                .append("null as ka_deelperceelnummer, ")
                .append("ar.ka_sectie, ")
                .append("ar.ka_appartementsindex, ")
                .append("null as grootte_perceel, ")
                .append("null as x, ")
                .append("null as y ")
                .append("from app_re ar ")
                .append("union all select ")
                .append("p.sc_kad_identif, ")
                .append("p.ka_kad_gemeentecode, ")
                .append("p.ka_perceelnummer, ")
                .append("p.ka_deelperceelnummer, ")
                .append("p.ka_sectie, ")
                .append("null as ka_appartementsindex, ")
                .append("p.grootte_perceel, ")
                .append("ST_X(p.plaatscoordinaten_perceel) as x, ")
                .append("ST_Y(p.plaatscoordinaten_perceel) as y ")
                .append("from kad_perceel p) q ")
                // einde samenstelling app_re en kad_perceel als q
                .append("on b.ref_id=q.sc_kad_identif::text ")

                .append("left join zak_recht z on b.ref_id=z.fk_7koz_kad_identif::text ")
                .append("left join aard_verkregen_recht avr on z.fk_3avr_aand=avr.aand ")

                .append("join belastingplichtige k on ( ")
                .append("q.ka_kad_gemeentecode=k.ka_kad_gemeentecode ")
                .append("and q.ka_sectie=k.ka_sectie ")
                .append("and q.ka_perceelnummer=k.ka_perceelnummer ")
                .append("and coalesce(q.ka_deelperceelnummer,'')=coalesce(k.ka_deelperceelnummer,'') ")
                .append("and coalesce(q.ka_appartementsindex,'')=coalesce(k.ka_appartementsindex,'') ) ")

                .append("WHERE '[")
                .append(df.format(van))
                .append(",")
                .append(df.format(tot))
                .append("]'::daterange @> b.datum ")
                .append("and z.fk_8pes_sc_identif is not null");

        return queryToJson(workDir, "Verkopen.json", sql.toString());
    }

    /**
     * Voert de sql query uit en schrijft het resultaat in het bestand in json formaat.
     *
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
