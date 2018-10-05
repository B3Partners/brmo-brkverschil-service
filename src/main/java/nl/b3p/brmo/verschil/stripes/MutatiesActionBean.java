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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.sourceforge.stripes.action.*;
import net.sourceforge.stripes.validation.*;
import nl.b3p.brmo.verschil.util.ConfigUtil;
import nl.b3p.brmo.verschil.util.ResultSetJSONSerializer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
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
public class MutatiesActionBean implements ActionBean, ValidationErrorHandler {

    private static final Log LOG = LogFactory.getLog(MutatiesActionBean.class);
    private final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    /**
     * verplichte datum begin periode. Datum in yyyy-mm-dd formaat, de
     * begindatum is deel van de periode.
     */
    @Validate(required = true, mask = "\\d{4}-\\d{2}-\\d{2}")
    private Date van;
    /**
     * optionele datum einde periode, default is datum van aanroepen. Datum in
     * yyyy-mm-dd formaat, de einddatum is deel van de periode, dwz. tot-en-met.
     */
    @Validate(mask = "\\d{4}-\\d{2}-\\d{2}")
    private Date tot = new Date();

    /**
     * optionele format parameter, default is {@code json}.
     */
    @Validate
    private String f = "json";

    private ActionBeanContext context;
    private long copied;
    private boolean errorCondition = false;

    // als gekoppeld wordt met een table
    private static final String TAX_JOIN_CLAUSE_TBL = new StringBuilder()
            .append("tax.belastingplichtige tax ON ( ")
            .append("      q.ka_kad_gemeentecode=trim(LEADING '0' from tax.gemeentecode) ")
            .append("  AND q.ka_sectie=tax.sectie ")
            .append("  AND q.ka_perceelnummer=trim(LEADING '0' from tax.perceelnummer) ")
            // deelperceel nummer wordt niet gevuld vanuit BRK want dat bestaat niet meer, dus ook niet in rsgb
            //.append("  AND coalesce(q.ka_deelperceelnummer,'')=coalesce(trim(LEADING '0' from tax.deelperceelnummer),'') ")
            .append("  AND coalesce(q.ka_appartementsindex,'')=coalesce(trim(LEADING '0' from tax.appartementsindex),'') )").toString();

    // als gekoppeld wordt met een view
    private static final String TAX_JOIN_CLAUSE_VW = new StringBuilder()
            .append("tax.belastingplichtige tax ON ( ")
            .append("      q.gemeentecode=trim(LEADING '0' from tax.gemeentecode) ")
            .append("  AND q.sectie=tax.sectie ")
            .append("  AND q.perceelnummer=trim(LEADING '0' from tax.perceelnummer) ")
            // deelperceel nummer wordt niet gevuld vanuit BRK want dat bestaat niet meer, dus ook niet in rsgb
            //.append("  AND coalesce(q.deelperceelnummer,'')=coalesce(trim(LEADING '0' from tax.deelperceelnummer),'') ")
            //.append("  AND coalesce(q.deelperceelnummer,'')=coalesce(trim(LEADING '0' from tax.deelperceelnummer),'') ")
            .append("  AND coalesce(q.appartementsindex,'')=coalesce(trim(LEADING '0' from tax.appartementsindex),'') )").toString();

    /**
     * context param voor view vb_koz_rechth.
     *
     * @see #initParams()
     */
    private String VIEW_KOZ_RECHTHEBBENDE = "vb_koz_rechth";
    /**
     * context param voor view vb_kad_onrrnd_zk_adres.
     *
     * @see #initParams()
     */
    private String VIEW_KAD_ONRRND_ZK_ADRES = "vb_kad_onrrnd_zk_adres";
    /**
     * context param voor view vb_kad_onrrnd_zk_archief.
     *
     * @see #initParams()
     */
    private String VIEW_KAD_ONRRND_ZK_ARCHIEF = "vb_kad_onrrnd_zk_archief";
    /**
     * context param voor view JDBC_FETCH_SIZE.
     *
     * @see #initParams()
     */
    private int JDBC_FETCH_SIZE = 1000;

    @ValidationMethod(when = ValidationState.NO_ERRORS)
    public void validateVanBeforeTot(ValidationErrors errors) {
        if (tot.before(van)) {
            errors.addGlobalError(new SimpleError("`van` datum is voor `tot` datum"));
        }
    }

    @Override
    public Resolution handleValidationErrors(ValidationErrors errors) throws Exception {
        StringBuilder msg = new StringBuilder("Validatiefout(en): \n");
        if (errors.hasFieldErrors()) {
            errors.entrySet().stream().forEach((entry) -> {
                entry.getValue().stream().map((e) -> {
                    if (LOG.isDebugEnabled()) {
                        msg.append("veld: ").append(entry.getKey()).append(", waarde: ");
                        msg.append(e.getFieldValue()).append(", melding: ");
                    }
                    return e;
                }).forEach((e) -> {
                    msg.append(e.getMessage(Locale.ROOT)).append(" \n");
                });
            });
        }
        if (errors.get(ValidationErrors.GLOBAL_ERROR) != null) {
            errors.get(ValidationErrors.GLOBAL_ERROR).stream().forEach((e) -> {
                msg.append(e.getMessage(Locale.ROOT));
            });
        }

        return new ErrorResolution(HttpServletResponse.SC_BAD_REQUEST, msg.toString());
    }

    @GET
    @DefaultHandler
    public Resolution get() throws IOException {
        errorCondition = false;
        LOG.trace("`get` met params: van=" + van + " tot=" + tot + ", format: " + f);
        LOG.info("Uitvoeren opdracht met params: van=" + df.format(van) + " tot=" + df.format(tot));
        this.initParams();
        // maak werkdirectory en werkbestand
        Path workPath = Files.createTempDirectory(
                Paths.get(System.getProperty("java.io.tmpdir")),
                "brkmutsvc"
        );
        File workDir = workPath.toFile();
        workDir.deleteOnExit();
        File workZip = Files.createTempFile("brkmutsvc", ".zip").toFile();
        workZip.deleteOnExit();

        // uitvoeren queries
        // 2.3
        LOG.debug("Ophalen nieuwe onroerende zaken");
        long nwOnrrgd = this.getNieuweOnroerendGoed(workDir);
        LOG.info("Aantal nieuwe onroerende zaken is: " + nwOnrrgd);
        // 2.4
        LOG.debug("Ophalen nieuwe onroerende zaken");
        long gekoppeld = this.getGekoppeldeObjecten(workDir);
        LOG.info("Aantal gekoppelde objecten: " + gekoppeld);
        // 2.5
        LOG.debug("Ophalen vervallen objecten");
        long vervallen = this.getVervallenOnroerendGoed(workDir);
        LOG.info("Aantal vervallen: " + vervallen);
        // 2.6
        LOG.debug("Ophalen object verkopen");
        long verkopen = this.getVerkopen(workDir);
        LOG.info("Aantal verkopen: " + verkopen);
        // 2.7
        LOG.debug("Ophalen oppervlakte veranderd objecten");
        long oppVeranderd = this.getGewijzigdeOpp(workDir);
        LOG.info("Aantal oppervlakte veranderd: " + oppVeranderd);
        // 2.8
        LOG.debug("Ophalen nieuwe subjecten");
        long nwSubject = this.getNieuweSubjecten(workDir);
        LOG.info("Aantal nieuwe subjecten: " + nwSubject);
        // 2.9
        LOG.debug("Ophalen BSN aangepast");
        long bsn = this.getBSNAangevuld(workDir);
        LOG.info("Aantal aangepast bsn: " + bsn);

        if (nwOnrrgd < 0 || gekoppeld < 0 || vervallen < 0 || verkopen < 0 || oppVeranderd < 0 || nwSubject < 0 || bsn < 0) {
            errorCondition = true;
            LOG.trace("Een van de queries heeft een onverwacht resultaat gegeven, errorCondition="+errorCondition);
        }
        // zippen resultaat in workZip
        try (ZipOutputStream zs = new ZipOutputStream(Files.newOutputStream(workZip.toPath()))) {
            LOG.debug("Aanmaken van zip bestand: " + workZip);
            Files.walk(workPath)
                    .filter(path -> !Files.isDirectory(path))
                    .forEach(path -> {
                        ZipEntry zipEntry = new ZipEntry(workPath.relativize(path).toString());
                        try {
                            LOG.debug("Toevoegen van bestand: " + zipEntry);
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
                if (errorCondition) {
                    response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                }
                FileUtils.deleteQuietly(workDir);
                FileUtils.deleteQuietly(workZip);
            }
        }.setFilename("mutaties_" + df.format(van) + "_" + df.format(tot) + ".zip")
                // 0! .setLength(copied)
                .setAttachment(true)
                .setLastModified(tot.getTime());
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
                .append("tax.gemeentecode, ")
                .append("tax.perceelnummer, ")
                .append("tax.deelperceelnummer, ")
                .append("tax.sectie, ")
                .append("tax.appartementsindex, ")
                .append("tax.kpr_nummer, ")
                .append("q.grootte_perceel, ")
                .append("q.x, ")
                .append("q.y, ")
                .append("z.ar_teller AS aandeel_teller, ")
                .append("z.ar_noemer AS aandeel_noemer, ")
                .append("z.fk_3avr_aand AS rechtcode, ")
                .append("avr.omschr_aard_verkregenr_recht AS rechtomschrijving, ")
                .append("h.fk_sc_rh_koz_kad_identif AS ontstaan_uit ")
                // TODO evt opzoeken kadastrale aanduiding in perceel/app_re archief
                // .append("arch.ka_kad_gemeentecode AS ontstaan_uit_gemeentecode, ")
                // .append("arch.ka_perceelnummer AS ontstaan_uit_perceelnummer, ")
                // .append("arch.ka_deelperceelnummer AS ontstaan_uit_deelperceelnummer, ")
                // .append("arch.ka_sectie AS ontstaan_uit_sectie, ")
                // .append("arch.ka_appartementsindex AS ontstaan_uit_appartementsindex, ")
                // of misschien samengesteld?
                // .append("h.fk_sc_lh_koz_kad_identif AS overgegaan_in ")
                .append("FROM kad_onrrnd_zk o ")
                // samengestelde app_re en kad_perceel als q
                .append("LEFT JOIN (SELECT  ")
                .append("  ar.sc_kad_identif, ")
                .append("  ar.ka_kad_gemeentecode, ")
                .append("  ar.ka_perceelnummer, ")
                .append("  null AS ka_deelperceelnummer, ")
                .append("  ar.ka_sectie, ")
                .append("  ar.ka_appartementsindex, ")
                .append("  null AS grootte_perceel, ")
                .append("  null AS x, ")
                .append("  null AS y ")
                .append("FROM app_re ar ")
                .append("UNION ALL SELECT ")
                .append("  p.sc_kad_identif, ")
                .append("  p.ka_kad_gemeentecode, ")
                .append("  p.ka_perceelnummer, ")
                .append("  p.ka_deelperceelnummer, ")
                .append("  p.ka_sectie, ")
                .append("  null AS ka_appartementsindex, ")
                .append("  p.grootte_perceel, ")
                .append("  ST_X(p.plaatscoordinaten_perceel) AS x, ")
                .append("  ST_Y(p.plaatscoordinaten_perceel) AS y ")
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
                .append("JOIN ")
                .append(TAX_JOIN_CLAUSE_TBL)
                // objecten met datum begin geldigheid in de periode "van"/"tot" inclusief,
                // maar niet in de archief tabel met een datum voor "van".
                .append("WHERE '[")
                .append(df.format(van)).append(",").append(df.format(tot))
                .append("]'::DATERANGE @> o.dat_beg_geldh::date ")
                .append("AND o.kad_identif NOT IN (SELECT kad_identif FROM kad_onrrnd_zk_archief WHERE '")
                .append(df.format(van)).append("'::date < dat_beg_geldh::date) ")
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
     * ophalen gekoppelde objecten [2.4]. Nieuwe objecten met bijbehoren adres
     * en/of adresbeschrijving.
     *
     * @param workDir directory waar resultaat wordt neergezet
     * @return aantal gekoppelde objecten
     */
    private long getGekoppeldeObjecten(File workDir) {
        StringBuilder sql = new StringBuilder("SELECT DISTINCT ")
                .append("adr.koz_identif, ")
                .append("adr.gemeentecode, ")
                .append("adr.sectie, ")
                .append("adr.perceelnummer, ")
                .append("adr.appartementsindex, ")
                .append("adr.loc_omschr, ")
                .append("adr.benoemdobj_identif, ")
                .append("adr.straatnaam, ")
                .append("adr.huisnummer, ")
                .append("adr.huisletter, ")
                .append("adr.huisnummer_toev, ")
                .append("adr.woonplaats, ")
                .append("adr.postcode ")
                .append("FROM ").append(VIEW_KAD_ONRRND_ZK_ADRES).append(" adr ")
                .append(" WHERE '[")
                .append(df.format(van)).append(",").append(df.format(tot)).append("]'::DATERANGE @> adr.begin_geldigheid::date ")
                .append("AND adr.koz_identif NOT IN (SELECT kad_identif FROM kad_onrrnd_zk_archief WHERE '")
                .append(df.format(van))
                .append("'::date < dat_beg_geldh::date) ORDER BY adr.koz_identif");
        switch (f) {
            case "csv":
                return queryToCSV(workDir, "GekoppeldeObjecten.csv", sql.toString());
            case "json":
            default:
                return queryToJson(workDir, "GekoppeldeObjecten.json", "koppeling", sql.toString());
        }
    }

    /**
     * ophalen vervallen percelen en appartementsrechten. [2.5] Het jongste
     * archief record van een object dat niet meer in de actuele tabel voorkomt.
     *
     * @param workDir directory waar resultaat wordt neergezet
     * @return aantal vervallen
     */
    private long getVervallenOnroerendGoed(File workDir) {
        StringBuilder sql = new StringBuilder("SELECT DISTINCT ON (arch.koz_identif) ")
                // TODO data uit RSGB heeft geen 0-padding
                .append("arch.koz_identif, ")
                .append("arch.eind_geldigheid, ")
                .append("arch.gemeentecode, ")
                .append("arch.sectie, ")
                .append("arch.perceelnummer, ")
                .append("arch.deelperceelnummer, ")
                .append("arch.appartementsindex ")
                // 0-padding
                .append("FROM ")
                .append(VIEW_KAD_ONRRND_ZK_ARCHIEF)
                .append(" arch ")
                // object heeft archief record in gevraagde periode
                .append("WHERE '[")
                .append(df.format(van)).append(",").append(df.format(tot))
                .append("]'::DATERANGE @> arch.eind_geldigheid::date ")
                // object niet meer in actuele tabel
                .append("AND arch.koz_identif NOT IN (SELECT kad_identif FROM kad_onrrnd_zk) ")
                // alleen de jongste archief record
                .append("ORDER BY arch.koz_identif, arch.eind_geldigheid::date DESC");

        switch (f) {
            case "csv":
                return queryToCSV(workDir, "VervallenOnroerendGoed.csv", sql.toString());
            case "json":
            default:
                return queryToJson(workDir, "VervallenOnroerendGoed.json", "vervallen", sql.toString());
        }
    }

    /**
     * Ophalen gewijzigde oppervlakte [2.7]. Jongste archief perceel die in de
     * periode waarvan de oppervlakte anders is dan het actuele perceel.
     */
    private long getGewijzigdeOpp(File workDir) {
        StringBuilder sql = new StringBuilder("SELECT DISTINCT ON (za.kad_identif) ")
                .append("za.kad_identif, ")
                // TODO data uit RSGB heeft geen 0-padding
                .append("k.ka_kad_gemeentecode AS gemeentecode, ")
                .append("k.ka_sectie AS sectie, ")
                .append("k.ka_perceelnummer AS perceelnummer, ")
                .append("k.ka_deelperceelnummer AS deelperceelnummer, ")
                // 0-padding
                .append("za.dat_beg_geldh, ")
                .append("pa.grootte_perceel AS opp_oud, ")
                .append("k.grootte_perceel  AS opp_actueel ")
                .append("FROM kad_onrrnd_zk_archief za, kad_perceel_archief pa, kad_perceel k ")
                // perceel moet in de archief zitten in gevraagde periode
                .append("WHERE '[")
                .append(df.format(van)).append(",").append(df.format(tot))
                .append("]'::DATERANGE @> za.dat_beg_geldh::DATE ")
                .append("AND za.dat_beg_geldh    = pa.sc_dat_beg_geldh ")
                .append("AND za.kad_identif      = pa.sc_kad_identif ")
                .append("AND za.kad_identif      = k.sc_kad_identif ")
                .append("AND pa.grootte_perceel != k.grootte_perceel ")
                // dubbelop vanwege grootte_perceel check
                // .append("AND za.clazz            = 'KADASTRAAL PERCEEL' ")
                // perceel moet in de actueel zitten in de periode, anders vervallen of datafout
                .append("AND za.kad_identif IN ( SELECT kad_identif FROM kad_onrrnd_zk ")
                .append("WHERE '[")
                .append(df.format(van)).append(",").append(df.format(tot))
                .append("]'::DATERANGE @> dat_beg_geldh::DATE ) ")
                .append("ORDER BY za.kad_identif, za.dat_beg_geldh DESC");

        switch (f) {
            case "csv":
                return queryToCSV(workDir, "GewijzigdeOpp.csv", sql.toString());
            case "json":
            default:
                return queryToJson(workDir, "GewijzigdeOpp.json", "gewijzigdeopp", sql.toString());
        }
    }

    /**
     * ophalen verkopen [2.6].
     *
     * @param workDir directory waar resultaat wordt neergezet
     * @return aantal verkopen
     */
    private long getVerkopen(File workDir) {
        StringBuilder sql = new StringBuilder("SELECT DISTINCT ")
                .append("bron.ref_id, ")
                .append("bron.datum::text as verkoopdatum, ")
                .append("tax.gemeentecode, ")
                .append("tax.sectie, ")
                .append("tax.perceelnummer, ")
                .append("tax.deelperceelnummer, ")
                .append("tax.appartementsindex, ")
                .append("tax.kpr_nummer, ")
                .append("z.ar_teller AS aandeel_teller, ")
                .append("z.ar_noemer AS aandeel_noemer, ")
                .append("z.fk_3avr_aand AS rechtcode, ")
                .append("avr.omschr_aard_verkregenr_recht AS rechtomschrijving ")
                // verkoop + datum
                .append("FROM ( ")
                .append("  SELECT brondocument.ref_id, max(brondocument.datum) AS datum FROM brondocument WHERE brondocument.omschrijving = 'Akte van Koop en Verkoop' GROUP BY brondocument.ref_id) bron ")
                // samengestelde app_re en kad_perceel als q
                .append("LEFT JOIN (SELECT  ")
                .append("  ar.sc_kad_identif, ")
                .append("  ar.ka_kad_gemeentecode, ")
                .append("  ar.ka_perceelnummer, ")
                .append("  null AS ka_deelperceelnummer, ")
                .append("  ar.ka_sectie, ")
                .append("  ar.ka_appartementsindex ")
                .append("FROM app_re ar ")
                .append("UNION ALL SELECT ")
                .append("  p.sc_kad_identif, ")
                .append("  p.ka_kad_gemeentecode, ")
                .append("  p.ka_perceelnummer, ")
                .append("  p.ka_deelperceelnummer, ")
                .append("  p.ka_sectie, ")
                .append("  null AS ka_appartementsindex ")
                .append("FROM kad_perceel p) q ")
                // einde samenstelling app_re en kad_perceel als q
                .append("ON bron.ref_id=q.sc_kad_identif::text ")
                .append("LEFT JOIN zak_recht z ON bron.ref_id = z.fk_7koz_kad_identif::text ")
                .append("LEFT JOIN aard_verkregen_recht avr ON z.fk_3avr_aand = avr.aand ")
                .append("JOIN ")
                // levert b
                .append(TAX_JOIN_CLAUSE_TBL)
                .append("WHERE '[")
                .append(df.format(van)).append(",").append(df.format(tot))
                .append("]'::DATERANGE @> bron.datum ")
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
     * Nieuwe subjecten [2.8]. De voor het systeem nieuwe subjecten zijn de
     * subjecten van nieuwe kadastrale objecten die niet aan de
     * belastingplichtige kunnen worden gekoppeld.
     *
     * @param workDir directory waar resultaat wordt neergezet
     * @return aantal nieuwe subjecten
     */
    private long getNieuweSubjecten(File workDir) {
        StringBuilder sql = new StringBuilder("SELECT DISTINCT ON (q.naam) ")
                .append("q.begin_geldigheid, ")
                .append("q.soort, ")
                .append("q.geslachtsnaam, ")
                .append("q.voorvoegsel, ")
                .append("q.voornamen, ")
                .append("q.naam, ")
                .append("q.woonadres, ")
                .append("q.geboortedatum, ")
                .append("q.overlijdensdatum, ")
                .append("q.bsn, ")
                .append("q.rsin, ")
                .append("q.kvk_nummer, ")
                .append("q.straatnaam, ")
                .append("q.huisnummer, ")
                .append("q.huisletter, ")
                .append("q.huisnummer_toev, ")
                .append("q.postcode, ")
                .append("q.woonplaats ")
                // altijd null: tax.kpr_nummer
                .append("FROM ").append(VIEW_KOZ_RECHTHEBBENDE).append(" q ")
                .append("LEFT JOIN ")
                .append(TAX_JOIN_CLAUSE_VW)
                // objecten met datum begin geldigheid in de periode "van"/"tot" inclusief,
                // maar niet in de archief tabel met een datum voor "van".
                .append("WHERE '[")
                .append(df.format(van)).append(",").append(df.format(tot))
                .append("]'::DATERANGE @> q.begin_geldigheid::date ")
                .append("AND q.koz_identif NOT IN (SELECT kad_identif FROM kad_onrrnd_zk_archief WHERE '")
                .append(df.format(van)).append("'::date < dat_beg_geldh::date) ")
                // die niet gekoppeld kunnen worden
                .append("AND tax.kpr_nummer IS NULL ")
                // alleen de eerste naam met de oudste datum
                .append("ORDER BY q.naam, q.begin_geldigheid ASC");

        switch (f) {
            case "csv":
                return queryToCSV(workDir, "NieuweSubjecten.csv", sql.toString());
            case "json":
            default:
                return queryToJson(workDir, "NieuweSubjecten.json", "nieuwe_subjecten", sql.toString());
        }
    }

    /**
     * Nieuwe subjecten in de gevraagde periode [2.9]. Nieuwe subjecten hebben
     * een record in de herkomst_metadata tabel met verwijzing naar subject en
     * met een datum binnen de gevraagde periode en zitten ook in de
     * ander_nat_prs tabel (want die zijn niet ingeschreven/geen bsn).
     *
     * @param workDir directory waar resultaat wordt neergezet
     * @return aantal bsn bijgewerkt
     */
    private long getBSNAangevuld(File workDir) {
        StringBuilder sql = new StringBuilder("SELECT ")
                .append("inp.bsn, ")
                .append("hm.datum::TEXT ")
                // (?)KPR nummer kan niet want er was toch geen bsn bekend dus waar komt dat dan vandaan?
                .append("FROM ingeschr_nat_prs inp ")
                .append("LEFT JOIN herkomst_metadata hm ON ")
                .append("inp.sc_identif = hm.waarde ")
                .append("WHERE inp.sc_identif IN (SELECT sc_identif FROM ander_nat_prs) ")
                .append("AND hm.tabel='subject' ")
                .append("AND '[")
                .append(df.format(van)).append(",").append(df.format(tot))
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
     * @param workDir directory waar json resultaat wordt neergezet
     * @param bestandsNaam naam van resultaat bestand
     * @param resultName naam van de json node met resultaten, default is
     * {@code results}
     * @param sql uit te voeren query
     * @param params optionele prepared statement params
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
                stm.setFetchSize(JDBC_FETCH_SIZE);
                LOG.debug(stm);

                SimpleModule module = new SimpleModule();
                ResultSetJSONSerializer serializer = new ResultSetJSONSerializer();
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

        } catch (SQLException | IOException e) {
            LOG.error(
                    String.format("Fout tijdens ophalen en uitschrijven gegevens (sql: %s, bestand: %s %s",
                            sql,
                            workDir,
                            bestandsNaam), e);
        }
        return count;
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
                stm.setFetchSize(JDBC_FETCH_SIZE);
                LOG.debug(stm);

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
        } catch (SQLException | IOException e) {
            LOG.error(
                    String.format("Fout tijdens ophalen en uitschrijven gegevens (sql: %s, bestand: %s %s",
                            sql,
                            workDir,
                            bestandsNaam), e);
        }
        return count;
    }

    private void initParams() {
        LOG.debug("laden van context params");
        boolean use_mv = Boolean.parseBoolean(getContext().getServletContext().getInitParameter("use_mv"));
        if (use_mv) {
            LOG.info("Gebruik materialized views in de queries.");
            VIEW_KOZ_RECHTHEBBENDE = VIEW_KOZ_RECHTHEBBENDE.replaceFirst("vb_", "mb_");
            VIEW_KAD_ONRRND_ZK_ADRES = VIEW_KAD_ONRRND_ZK_ADRES.replaceFirst("vb_", "mb_");
            VIEW_KAD_ONRRND_ZK_ARCHIEF = VIEW_KAD_ONRRND_ZK_ARCHIEF.replaceFirst("vb_", "mb_");
        }

        try {
            JDBC_FETCH_SIZE = Integer.parseInt(getContext().getServletContext().getInitParameter("jdbc_fetch_size"));
            LOG.debug("Gebruik fetch size van " + JDBC_FETCH_SIZE);
        } catch (Exception e) {
            // ignore
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
