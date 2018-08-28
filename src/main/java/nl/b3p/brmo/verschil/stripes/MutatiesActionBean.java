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
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Mutaties actionbean. Haalt mutaties uit de BRMO RSGB database voor de gegeven
 * periode.
 * <br> Voorbeeld url: {@code /rest/mutaties?van=2018-08-01} of
 * {@code /rest/mutaties?van=2018-08-01&tot=2018-09-01} .
 *
 * @author mark
 * @since 1.0
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
     * optionele datum einde periode, default is datum van aanroepen. Datum in
     * yyyy-mm-dd formaat.
     */
    @Validate
    private Date tot = new Date();

    /**
     * optionele format parameter, default is {@code json}.
     */
    @Validate
    private String f = "json";

    private ActionBeanContext context;
    private long copied;

    @GET
    @DefaultHandler
    public Resolution get() throws IOException {
        LOG.trace("get met params: van=" + van + " tot=" + tot);
        if (van == null) {
            return new ErrorResolution(500, "De verplichte parameter `van` ontbreekt.");
        }
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

        long gekoppeld = this.getGekoppeldeObjecten(workDir);
        LOG.debug("aantal gekoppelde objecten: "+gekoppeld);
        long verkopen = this.getVerkopen(workDir);
        LOG.debug("aantal verkopen: " + verkopen);

        long vervallen = this.getVervallenOnroerendGoed(workDir);
        LOG.debug("aantal vervallen: " + vervallen);

        // 2.9
        long bsn = this.getBSNAangevuld(workDir);
        LOG.debug("aantal aangepast bsn: " + bsn);

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
                //.setLength(copied)
                .setAttachment(false);
    }

    /**
     * ophalen nieuwe percelen en appartementsrechten. [2.3].
     *
     * @param workDir directory waar resultaat wordt neergezet
     * @return aantal nieuw
     */
    private long getNieuweOnroerendGoed(File workDir) {
        StringBuilder sql = new StringBuilder("SELECT DISTINCT ")
                .append("o.kad_identif, ")
                .append("o.dat_beg_geldh, ")
                .append("q.ka_kad_gemeentecode, ")
                .append("q.ka_perceelnummer, ")
                .append("q.ka_deelperceelnummer, ")
                .append("q.ka_sectie, ")
                .append("q.ka_appartementsindex, ")
                .append("q.grootte_perceel, ")
                .append("q.x, ")
                .append("q.y, ")
                .append("b.kpr_nummer, ")
                .append("z.ar_teller, ")
                .append("z.ar_noemer, ")
                .append("z.fk_3avr_aand, ")
                .append("avr.omschr_aard_verkregenr_recht, ")
                .append("h.fk_sc_rh_koz_kad_identif AS ontstaan_uit ")
                .append("from kad_onrrnd_zk o ")
                // samengestelde app_re en kad_perceel als q
                .append("LEFT JOIN (SELECT  ")
                .append("ar.sc_kad_identif, ")
                .append("ar.ka_kad_gemeentecode, ")
                .append("ar.ka_perceelnummer, ")
                .append("null AS ka_deelperceelnummer, ")
                .append("ar.ka_sectie, ")
                .append("ar.ka_appartementsindex, ")
                .append("null AS grootte_perceel, ")
                .append("null AS x, ")
                .append("null AS y ")
                .append("FROM app_re ar ")
                .append("UNION ALL SELECT ")
                .append("p.sc_kad_identif, ")
                .append("p.ka_kad_gemeentecode, ")
                .append("p.ka_perceelnummer, ")
                .append("p.ka_deelperceelnummer, ")
                .append("p.ka_sectie, ")
                .append("null AS ka_appartementsindex, ")
                .append("p.grootte_perceel, ")
                .append("ST_X(p.plaatscoordinaten_perceel) AS x, ")
                .append("ST_Y(p.plaatscoordinaten_perceel) AS y ")
                .append("FROM kad_perceel p) q ")
                // einde samenstelling app_re en kad_perceel als q
                .append("ON o.kad_identif=q.sc_kad_identif ")
                // zakelijk recht erbij
                .append("LEFT JOIN zak_recht z ON o.kad_identif=z.fk_7koz_kad_identif ")
                // soort recht omschrijving
                .append("LEFT JOIN aard_verkregen_recht avr ON z.fk_3avr_aand=avr.aand ")
                // ontstaan uit
                .append("LEFT JOIN kad_onrrnd_zk_his_rel h ON o.kad_identif=h.fk_sc_lh_koz_kad_identif ")
                // BKP erbij
                .append("JOIN belastingplichtige b ON ( ")
                .append("  q.ka_kad_gemeentecode=b.ka_kad_gemeentecode ")
                .append("  AND q.ka_sectie=b.ka_sectie ")
                .append("  AND q.ka_perceelnummer=b.ka_perceelnummer ")
                .append("  AND coalesce(q.ka_deelperceelnummer,'')=coalesce(b.ka_deelperceelnummer,'') ")
                .append("  AND coalesce(q.ka_appartementsindex,'')=coalesce(b.ka_appartementsindex,'') )")
                // objecten met datum begin geldigheid in de periode "van"/"tot" inclusief,
                // maar niet in de archief tabel met een datum voor "van".
                .append("WHERE '[")
                .append(df.format(van))
                .append(",")
                .append(df.format(tot))
                .append("]'::DATERANGE @> dat_beg_geldh::date ")
                .append("AND kad_identif NOT IN (SELECT kad_identif FROM kad_onrrnd_zk_archief WHERE '")
                .append(df.format(van))
                .append("'::date < dat_beg_geldh::date) ")
                .append("AND z.fk_8pes_sc_identif IS NOT null");

        switch (f) {
            case "csv":
                return queryToCSV(workDir, "NieuweOnroerendGoed.csv", sql.toString());
            case "json":
            default:
                return queryToJson(workDir, "NieuweOnroerendGoed.json", "nieuw", sql.toString());
        }

    }

    /**
     * ophalen gekoppelde objecten [2.4].
     *
     * @param workDir directory waar resultaat wordt neergezet
     * @return aantal gekoppeld
     */
    private long getGekoppeldeObjecten(File workDir) {
        StringBuilder sql = new StringBuilder("SELECT DISTINCT ")
                .append("o.kad_identif, ")
                .append("adr.gemeentecode, ")
                .append("adr.sectie, ")
                .append("adr.perceelnummer, ")
                .append("adr.appartementsindex, ")
                .append("o.lo_loc__omschr, ")
                .append("adr.benoemdobj_identif, ")
                .append("adr.straatnaam, ")
                .append("adr.huisnummer, ")
                .append("adr.huisletter, ")
                .append("adr.huisnummer_toev, ")
                .append("adr.woonplaats, ")
                .append("adr.postcode ")
                .append("FROM kad_onrrnd_zk o ")
                .append("LEFT JOIN v_kad_onrrnd_zk_adres adr ON adr.koz_identif = o.kad_identif ")
                .append("WHERE '[")
                .append(df.format(van))
                .append(",")
                .append(df.format(tot))
                .append("]'::DATERANGE @> dat_beg_geldh::date ")
                .append("AND kad_identif NOT IN (SELECT kad_identif FROM kad_onrrnd_zk_archief WHERE '")
                .append(df.format(van))
                .append("'::date < dat_beg_geldh::date) ");
        switch (f) {
            case "csv":
                return queryToCSV(workDir, "GekoppeldeObjecten.csv", sql.toString());
            case "json":
            default:
                return queryToJson(workDir, "GekoppeldeObjecten.json", "koppeling", sql.toString());
        }
    }

    /**
     * ophalen vervallen percelen en appartementsrechten. [2.5]
     *
     * @param workDir directory waar resultaat wordt neergezet
     * @return aantal vervallen
     */
    private long getVervallenOnroerendGoed(File workDir) {
        StringBuilder sql = new StringBuilder("SELECT DISTINCT ON (k.kad_identif)")
                .append("k.kad_identif, ")
                .append("k.datum_einde_geldh, ")
                //
                .append("q.ka_kad_gemeentecode, ")
                .append("q.ka_sectie, ")
                .append("q.ka_perceelnummer, ")
                .append("q.ka_deelperceelnummer, ")
                .append("q.ka_appartementsindex ")
                //
                .append("FROM kad_onrrnd_zk_archief k ")
                // samengestelde app_re en kad_perceel als q
                .append("LEFT JOIN (SELECT  ")
                .append("ar.sc_kad_identif, ")
                .append("ar.ka_kad_gemeentecode, ")
                .append("ar.ka_perceelnummer, ")
                .append("null AS ka_deelperceelnummer, ")
                .append("ar.ka_sectie, ")
                .append("ar.ka_appartementsindex ")
                .append("FROM app_re_archief ar ")
                .append("UNION ALL SELECT ")
                .append("p.sc_kad_identif, ")
                .append("p.ka_kad_gemeentecode, ")
                .append("p.ka_perceelnummer, ")
                .append("p.ka_deelperceelnummer, ")
                .append("p.ka_sectie, ")
                .append("null AS ka_appartementsindex ")
                .append("FROM kad_perceel_archief p) q ")
                // einde samenstelling app_re en kad_perceel als q
                .append("ON k.kad_identif=q.sc_kad_identif ")
                .append("WHERE '[")
                .append(df.format(van))
                .append(",")
                .append(df.format(tot))
                .append("]'::DATERANGE @> k.datum_einde_geldh::date ")
                .append("AND k.kad_identif NOT IN (SELECT kad_identif FROM kad_onrrnd_zk) ")
                .append("ORDER BY k.kad_identif, k.datum_einde_geldh::date DESC");

        switch (f) {
            case "csv":
                return queryToCSV(workDir, "VervallenOnroerendGoed.csv", sql.toString());
            case "json":
            default:
                return queryToJson(workDir, "VervallenOnroerendGoed.json", "vervallen", sql.toString());
        }

    }

    /**
     * ophalen gewijzigde oppervlakte. [2.7]
     */
    private long getGewijzigdeOpp(File workDir) {
        // alle percelen die aangepast zijn in de periode waarvan de oppervlakte van de oudste en de jongste een verschillende oppervlakte hebben
        return -1;
    }

    /**
     * ophalen verkopen [2.6].
     *
     * @param workDir directory waar resultaat wordt neergezet
     * @return aantal verkopen
     */
    private long getVerkopen(File workDir) {
        StringBuilder sql = new StringBuilder("SELECT ")
                .append("DISTINCT b.ref_id, ")
                .append("b.datum::text, ")
                //
                .append("q.ka_kad_gemeentecode, ")
                .append("q.ka_sectie, ")
                .append("q.ka_perceelnummer, ")
                .append("q.ka_deelperceelnummer, ")
                .append("q.ka_appartementsindex, ")
                //
                .append("k.kpr_nummer, ")
                .append("z.ar_teller, ")
                .append("z.ar_noemer, ")
                .append("z.fk_3avr_aand, ")
                .append("avr.omschr_aard_verkregenr_recht ")
                // verkoop + datum
                .append("FROM ( SELECT brondocument.ref_id, max(brondocument.datum) AS datum FROM brondocument WHERE brondocument.omschrijving = 'Akte van Koop en Verkoop' GROUP BY brondocument.ref_id) b ")
                // samengestelde app_re en kad_perceel als q
                .append("LEFT JOIN (SELECT  ")
                .append("ar.sc_kad_identif, ")
                .append("ar.ka_kad_gemeentecode, ")
                .append("ar.ka_perceelnummer, ")
                .append("null AS ka_deelperceelnummer, ")
                .append("ar.ka_sectie, ")
                .append("ar.ka_appartementsindex ")
                .append("FROM app_re ar ")
                .append("UNION ALL SELECT ")
                .append("p.sc_kad_identif, ")
                .append("p.ka_kad_gemeentecode, ")
                .append("p.ka_perceelnummer, ")
                .append("p.ka_deelperceelnummer, ")
                .append("p.ka_sectie, ")
                .append("null AS ka_appartementsindex ")
                .append("FROM kad_perceel p) q ")
                // einde samenstelling app_re en kad_perceel als q
                .append("ON b.ref_id=q.sc_kad_identif::text ")
                .append("LEFT JOIN zak_recht z ON b.ref_id=z.fk_7koz_kad_identif::text ")
                .append("LEFT JOIN aard_verkregen_recht avr ON z.fk_3avr_aand=avr.aand ")
                .append("JOIN belastingplichtige k ON ( ")
                .append("q.ka_kad_gemeentecode=k.ka_kad_gemeentecode ")
                .append("AND q.ka_sectie=k.ka_sectie ")
                .append("AND q.ka_perceelnummer=k.ka_perceelnummer ")
                .append("AND coalesce(q.ka_deelperceelnummer,'')=coalesce(k.ka_deelperceelnummer,'') ")
                .append("AND coalesce(q.ka_appartementsindex,'')=coalesce(k.ka_appartementsindex,'') ) ")
                .append("WHERE '[")
                .append(df.format(van))
                .append(",")
                .append(df.format(tot))
                .append("]'::DATERANGE @> b.datum ")
                .append("AND z.fk_8pes_sc_identif IS NOT null");

        switch (f) {
            case "csv":
                return queryToCSV(workDir, "Verkopen.csv", sql.toString());
            case "json":
            default:
                return queryToJson(workDir, "Verkopen.json", "verkopen", sql.toString());
        }
    }

    /**
     * nieuwe subjecten. [2.8].
     *
     * @param workDir directory waar resultaat wordt neergezet
     * @return aantal nieuwe subjecten
     */
    private long getNieuweSubjecten(File workDir) {
        return -1;
    }

    /**
     * [2.9].
     *
     * @param workDir directory waar resultaat wordt neergezet
     * @return aantal bsn bijgewerkt
     */
    private long getBSNAangevuld(File workDir) {
        StringBuilder sql = new StringBuilder("SELECT ")
                .append("i.bsn, ")
                .append("h.datum ")
                // TODO KPR nummer??
                .append("FROM ingeschr_nat_prs i ")
                .append("LEFT JOIN herkomst_metadata h ON ")
                .append("i.sc_identif = h.waarde ")
                .append("WHERE i.sc_identif IN (SELECT sc_identif FROM ander_nat_prs) ")
                .append("AND h.tabel='subject' ")
                .append("AND '[")
                .append(df.format(van))
                .append(",")
                .append(df.format(tot))
                .append("]'::DATERANGE @> datum::DATE ");

        switch (f) {
            case "csv":
                return queryToCSV(workDir, "BsnAangevuld.csv", sql.toString());
            case "json":
            default:
                return queryToJson(workDir, "BsnAangevuld.json", "bsnaangevuld", sql.toString());
        }

    }

    /**
     * Voert de sql query uit en schrijft het resultaat in het bestand in json
     * formaat.
     *
     * @param workDir      directory waar json resultaat wordt neergezet
     * @param bestandsNaam naam van resultaat bestand
     * @param resultName   naam van de json node met resultaten, default is
     *                     {@code results}
     * @param sql          uit te voeren query
     * @param params       optionele prepared statement params
     * @return aantal verwerkte records of -1 in geval van een fout
     */
    private long queryToJson(File workDir, String bestandsNaam, String resultName, String sql, String... params) {
        long count = -1;
        if (resultName == null) {
            resultName = "results";
        }
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
                    objectNode.putPOJO(resultName, r);
                    mapper.writeValue(new FileOutputStream(workDir + File.separator + bestandsNaam), objectNode);
                }
                count = serializer.getCount();
            }

        } catch (SQLException | FileNotFoundException | ResultSetSerializerException e) {
            LOG.error(
                    String.format("Fout tijdens ophalen en uitschrijven gegevens (sql: %s, bestand: %s %s",
                            sql,
                            workDir,
                            bestandsNaam), e);
        } finally {
            return count;
        }
    }

    private long queryToCSV(File workDir, String bestandsNaam, String sql, String... params) {
        long count = -1;

        final String NL = System.getProperty("line.separator");
        final String SEP = ";";

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

                try (ResultSet r = stm.executeQuery();
                     FileOutputStream fos = new FileOutputStream(workDir + File.separator + bestandsNaam);
                     Writer out = new OutputStreamWriter(new BufferedOutputStream(fos), "UTF-8")) {
                    ResultSetMetaData metaData = r.getMetaData();
                    int numCols = metaData.getColumnCount();


                    // schrijf kolommen
                    for (int j = 1; j < (numCols + 1); j++) {
                        out.append(metaData.getColumnName(j));
                        if (j < numCols) {
                            out.append(SEP);
                        } else {
                            out.append(NL);
                        }
                    }

                    count = 0;
                    // schrijf data
                    while (r.next()) {
                        for (int k = 1; k < (numCols + 1); k++) {
                            // het zou mooier zijn om de type specifieke getters van de resultset te gebruiken,
                            // maar uiteindelijk doen we toch een toString() dus resultaat is gelijk.
                            String o = r.getString(k);
                            out.append(o != null ? o : "");
                            if (k < numCols) {
                                out.append(SEP);
                            } else {
                                out.append(NL);
                            }
                        }
                        count++;
                    }
                }
            }
        } catch (SQLException | FileNotFoundException e) {
            LOG.error(
                    String.format("Fout tijdens ophalen en uitschrijven gegevens (sql: %s, bestand: %s %s",
                            sql,
                            workDir,
                            bestandsNaam), e);
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

    public String getF() {
        return f;
    }

    public void setF(String f) {
        this.f = f;
    }
}
