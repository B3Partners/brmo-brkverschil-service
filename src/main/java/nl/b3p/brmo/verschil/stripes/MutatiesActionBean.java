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
            .append("      q.ka_kad_gemeentecode = trim(LEADING '0' from tax.gemeentecode) ")
            .append("  AND q.ka_sectie           = trim(tax.sectie) ")
            .append("  AND q.ka_perceelnummer    = trim(LEADING '0' from tax.perceelnummer) ")
            // deelperceel nummer wordt niet gevuld vanuit BRK want dat bestaat niet meer, dus ook niet in rsgb
            //.append("  AND coalesce(q.ka_deelperceelnummer,'')=coalesce(trim(LEADING '0' from tax.deelperceelnummer),'') ")
            .append("  AND coalesce(q.ka_appartementsindex,'') = coalesce(trim(LEADING '0' from tax.appartementsindex),'') )").toString();

    // als gekoppeld wordt met een view
    private static final String TAX_JOIN_CLAUSE_VW = new StringBuilder()
            .append("tax.belastingplichtige tax ON ( ")
            .append("      q.gemeentecode  = trim(LEADING '0' from tax.gemeentecode) ")
            .append("  AND q.sectie        = trim(tax.sectie) ")
            .append("  AND q.perceelnummer = trim(LEADING '0' from tax.perceelnummer) ")
            // deelperceel nummer wordt niet gevuld vanuit BRK want dat bestaat niet meer, dus ook niet in rsgb
            //.append("  AND coalesce(q.deelperceelnummer,'')=coalesce(trim(LEADING '0' from tax.deelperceelnummer),'') ")
            .append("  AND coalesce(q.appartementsindex,'') = coalesce(trim(LEADING '0' from tax.appartementsindex),'') )").toString();

    private static final String TAX_JOIN_CLAUSE_TBL_AANDUIDING2 = new StringBuilder()
            .append("tax.belastingplichtige tax ON q.aanduiding2 = tax.aanduiding2 ").toString();

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
     * context param voor sql JDBC_FETCH_SIZE.
     *
     * @see #initParams()
     */
    private int JDBC_FETCH_SIZE = 0;
    /**
     * context param voor sql timeout.
     *
     * @see #initParams()
     */
    private int QRY_TIMEOUT = 600;
    /**
     * CSV separator character.
     *
     * @see #initParams()
     */
    private String SEP = ";";
    /**
     * CSV quote character.
     *
     * @see #initParams()
     */
    private String QUOTE = "";
    /**
     * newline voor CSV output.
     */
    private final String NL = System.getProperty("line.separator");

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
        LOG.info(String.format("Uitvoeren opdracht met params: van=%s tot=%s", df.format(van), df.format(tot)));
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

        this.checkTaxAanduiding2();
        // uitvoeren queries
        // 2.3
        LOG.debug("Ophalen nieuwe onroerende zaken");
        long nwOnrrgd = this.getNieuweOnroerendGoed(workDir);
        LOG.info("Aantal nieuwe onroerende zaken is: " + nwOnrrgd);
        // 2.4
        LOG.debug("Ophalen gekoppelde objecten");
        long gekoppeld = this.getGekoppeldeObjecten(workDir);
        LOG.info("Aantal gekoppeld objecten: " + gekoppeld);
        // 2.5
        LOG.debug("Ophalen vervallen objecten");
        long vervallen = this.getVervallenOnroerendGoed(workDir);
        LOG.info("Aantal vervallen objecten: " + vervallen);
        // 2.6
        LOG.debug("Ophalen object verkopen");
        long verkopen = this.getVerkopen(workDir);
        LOG.info("Aantal object verkopen: " + verkopen);
        // 2.7
        LOG.debug("Ophalen oppervlakte veranderd objecten");
        long oppVeranderd = this.getGewijzigdeOpp(workDir);
        LOG.info("Aantal oppervlakte veranderd objecten: " + oppVeranderd);
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
            LOG.trace("Een van de queries heeft een onverwacht resultaat gegeven, errorCondition=" + errorCondition);
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
                            LOG.error("Probleem tijdens aanmaken van zipfile", e);
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
                .setAttachment(true)
                .setLastModified(tot.getTime());
    }

    /**
     * bijwerken van kolom aanduiding2 van tabel tax.belastingplichtige indien
     * nodig.
     */
    private void checkTaxAanduiding2() {
        final String checkSQL = "SELECT COUNT(*) FROM tax.belastingplichtige WHERE aanduiding2 IS NULL";
        final String updateSQL = "UPDATE tax.belastingplichtige SET aanduiding2 = TRIM(LEADING '0' FROM gemeentecode)  || ' ' || TRIM(sectie) || ' ' || TRIM(LEADING '0' FROM perceelnummer) || ' ' || COALESCE(TRIM(LEADING '0' FROM appartementsindex), '')";

        LOG.debug("Controle kolom aanduiding2 van tabel tax.belastingplichtige");
        try (Connection c = ConfigUtil.getDataSourceRsgb().getConnection()) {
            int count = 0;
            try (PreparedStatement stm = c.prepareStatement(checkSQL)) {
                stm.setQueryTimeout(QRY_TIMEOUT);
                LOG.debug(stm);

                ResultSet rs = stm.executeQuery();
                while (rs.next()) {
                    count = rs.getInt(1);
                }
                LOG.debug("Aantal niet gevuld in kolom aanduiding2 van tabel tax.belastingplichtige is: " + count);
                rs.close();
            }
            if (count > 1) {
                LOG.info("Vullen/bijwerken van kolom `aanduiding2` van tabel `tax.belastingplichtige`");
                try (PreparedStatement stm = c.prepareStatement(updateSQL)) {
                    stm.setQueryTimeout(QRY_TIMEOUT);
                    LOG.debug(stm);

                    int updated = stm.executeUpdate();
                    LOG.debug(updated + " rijen bijgewerkt in kolom aanduiding2 van tabel tax.belastingplichtige");
                }
            }
        } catch (SQLException e) {
            LOG.fatal(String.format("Fout tijdens bijwerken kolom `aanduiding2` van tabel `tax.belastingplichtige`. \n\tVoer met de hand de update `%s` uit.", updateSQL), e);
        }
    }

    /**
     * ophalen nieuwe percelen en appartementsrechten. [2.3].
     *
     * @param workDir directory waar resultaat wordt neergezet
     * @return aantal nieuw
     */
    private long getNieuweOnroerendGoed(File workDir) {
//        StringBuilder sql = new StringBuilder("SELECT DISTINCT ")
//                // de ON (o.kad_identif) zorgt ervoor dat we niet voor ieder bron
//                //  object van een samenvoeging een record krijgen, maar dat is een uitdrukkelijke wens (mail dd.18-12-18)
//                // .append("ON (o.kad_identif) ")
//                .append("o.kad_identif, ")
//                .append("o.dat_beg_geldh, ")
//                .append("q.ka_kad_gemeentecode  AS gemeentecode, ")
//                .append("q.ka_perceelnummer     AS perceelnummer, ")
//                .append("q.ka_deelperceelnummer AS deelperceelnummer, ")
//                .append("q.ka_sectie            AS sectie, ")
//                .append("q.ka_appartementsindex AS appartementsindex, ")
//                .append("tax.kpr_nummer, ")
//                .append("bel.bpl_identif, ")
//                .append("bel.naam_zakelijk_gerechtigde, ")
//                .append("q.grootte_perceel, ")
//                .append("q.x, ")
//                .append("q.y, ")
//                .append("z.ar_teller            AS aandeel_teller, ")
//                .append("z.ar_noemer            AS aandeel_noemer, ")
//                .append("z.fk_3avr_aand         AS rechtcode, ")
//                .append("avr.omschr_aard_verkregenr_recht AS rechtomschrijving, ")
//                .append("h.fk_sc_rh_koz_kad_identif AS ontstaan_uit, ")
//                .append("h.aard, ")
//                .append("arch.gemeentecode             AS ontstaan_uit_gemeentecode,  ")
//                .append("arch.perceelnummer            AS ontstaan_uit_perceelnummer, ")
//                .append("arch.deelperceelnummer        AS ontstaan_uit_deelperceelnummer, ")
//                .append("arch.sectie                   AS ontstaan_uit_sectie,  ")
//                .append("arch.appartementsindex        AS ontstaan_uit_appartementsindex ")
//                .append("FROM kad_onrrnd_zk o ")
//                // samengestelde app_re en kad_perceel als q
//                .append("LEFT JOIN (SELECT  ")
//                .append("  ar.sc_kad_identif, ")
//                .append("  ar.ka_kad_gemeentecode, ")
//                .append("  ar.ka_perceelnummer, ")
//                .append("  null AS ka_deelperceelnummer, ")
//                .append("  ar.ka_sectie, ")
//                .append("  ar.ka_appartementsindex, ")
//                .append("  null AS grootte_perceel, ")
//                .append("  null AS x, ")
//                .append("  null AS y ")
//                .append("FROM app_re ar ")
//                .append("UNION ALL SELECT ")
//                .append("  p.sc_kad_identif, ")
//                .append("  p.ka_kad_gemeentecode, ")
//                .append("  p.ka_perceelnummer, ")
//                .append("  p.ka_deelperceelnummer, ")
//                .append("  p.ka_sectie, ")
//                .append("  null AS ka_appartementsindex, ")
//                .append("  p.grootte_perceel, ")
//                .append("  ST_X(p.plaatscoordinaten_perceel) AS x, ")
//                .append("  ST_Y(p.plaatscoordinaten_perceel) AS y ")
//                .append("FROM kad_perceel p) q ")
//                // einde samenstelling app_re en kad_perceel als q
//                .append("ON o.kad_identif = q.sc_kad_identif ")
//                // zakelijk recht erbij
//                .append("LEFT JOIN zak_recht z ON o.kad_identif = z.fk_7koz_kad_identif ")
//                // soort recht omschrijving
//                .append("LEFT JOIN aard_verkregen_recht avr ON z.fk_3avr_aand = avr.aand ")
//                // ontstaan uit (NB. alleen eerste ontstaan uit, object kan uit meer dan 2 ontstaan)
//                .append("LEFT JOIN kad_onrrnd_zk_his_rel h ON o.kad_identif = h.fk_sc_lh_koz_kad_identif ")
//                // ophalen aanduiding van 'onstaan uit'
//                .append("LEFT JOIN mb_kad_onrrnd_zk_archief arch ON h.fk_sc_rh_koz_kad_identif = arch.koz_identif ")
//                // belastingplichtige
//                .append("LEFT JOIN wdd.kad_zak_recht bel ON o.kad_identif = bel.sc_kad_identif ")
//                // BKP erbij
//                .append("LEFT JOIN ")
//                .append(TAX_JOIN_CLAUSE_TBL)
//                // Eigendom (recht van) (2), Erfpacht (recht van) (3), Gebruik en bewoning (recht van) (4), Vruchtgebruik (recht van) (12)
//                .append(" WHERE z.fk_3avr_aand IN ( '2', '4', '3', '12')  AND '[")
//                // objecten met datum begin geldigheid in de periode "van"/"tot" inclusief,
//                .append(df.format(van)).append(",").append(df.format(tot))
//                .append("]'::DATERANGE @> o.dat_beg_geldh::date ")
//                // maar niet in de archief tabel
//                .append(" AND o.kad_identif NOT IN (")
//                .append("     SELECT kad_identif FROM kad_onrrnd_zk_archief ")
//                // met een datum voor "van" == geen archief record voorafgaand aan gevraagde periode
//                //.append("     WHERE dat_beg_geldh::date < '")
//                //.append(df.format(van)).append("'::date  ")
//                //
//                // gerechtigde is niet null
//                .append(") AND z.fk_8pes_sc_identif IS NOT NULL ")
//                // kpr nummer/gibs onbekend
//                .append(" AND tax.kpr_nummer IS NULL");

// optimalisaties:
//   - gebruik mb_kad_onrrnd_zk_adres
//   - gebruik extra kolom in tax.belastingplichtige zodat we aanduiding2 kunnen gebruiken
//   - gebruik een NOT EXISTS voor uitsluiten van bekende percelen/subjecten
        StringBuilder sql = new StringBuilder("SELECT DISTINCT ")
                .append("o.koz_identif, ")
                .append("o.begin_geldigheid, ")
                .append("o.gemeentecode, ")
                .append("o.perceelnummer, ")
                .append("o.deelperceelnummer, ")
                .append("o.sectie, ")
                .append("o.appartementsindex, ")
                //.append("-- tax.kpr_nummer, ")
                .append("bel.bpl_identif, ")
                .append("bel.naam_zakelijk_gerechtigde, ")
                .append("o.grootte_perceel, ")
                .append("ST_X(ST_Transform(ST_SetSRID(ST_MakePoint(o.lon, o.lat),4326),28992)) AS x, ")
                .append("ST_Y(ST_Transform(ST_SetSRID(ST_MakePoint(o.lon, o.lat),4326),28992)) AS y, ")
                .append("z.ar_teller AS aandeel_teller, ")
                .append("z.ar_noemer AS aandeel_noemer, ")
                .append("z.fk_3avr_aand AS rechtcode, ")
                .append("avr.omschr_aard_verkregenr_recht AS rechtomschrijving, ")
                .append("h.fk_sc_rh_koz_kad_identif AS ontstaan_uit, ")
                .append("h.aard, ")
                .append("arch.gemeentecode AS ontstaan_uit_gemeentecode, ")
                .append("arch.perceelnummer AS ontstaan_uit_perceelnummer, ")
                .append("arch.deelperceelnummer AS ontstaan_uit_deelperceelnummer, ")
                .append("arch.sectie AS ontstaan_uit_sectie, ")
                .append("arch.appartementsindex AS ontstaan_uit_appartementsindex ")
                .append("FROM ").append(VIEW_KAD_ONRRND_ZK_ADRES).append(" o ")
                // zakelijk recht erbij
                .append("LEFT JOIN zak_recht z ON o.koz_identif = z.fk_7koz_kad_identif ")
                // soort recht omschrijving
                .append("LEFT JOIN aard_verkregen_recht avr ON z.fk_3avr_aand = avr.aand ")
                // ontstaan uit
                .append("LEFT JOIN kad_onrrnd_zk_his_rel h ON o.koz_identif = h.fk_sc_lh_koz_kad_identif ")
                // ophalen aanduiding van 'onstaan uit'
                .append("LEFT JOIN mb_kad_onrrnd_zk_archief arch ON h.fk_sc_rh_koz_kad_identif = arch.koz_identif ")
                // belastingplichtige
                .append("LEFT JOIN wdd.kad_zak_recht bel ON o.koz_identif = bel.sc_kad_identif ")
                //.append("-- LEFT JOIN tax.belastingplichtige tax ON o.aanduiding2 = tax.aanduiding2 ")
                .append("WHERE '[")
                // objecten met datum begin geldigheid in de periode "van"/"tot" inclusief,
                .append(df.format(van)).append(",").append(df.format(tot))
                .append("]'::DATERANGE @> o.begin_geldigheid::date ")
                // Eigendom (recht van) (2), Erfpacht (recht van) (3), Gebruik en bewoning (recht van) (4), Vruchtgebruik (recht van) (12)
                .append("AND z.fk_3avr_aand IN ('2','4','3','12') ")
                // gerechtigde is niet null
                .append("AND z.fk_8pes_sc_identif IS NOT NULL ")
                // maar niet in de archief tabel met een datum voor "van" == geen archief record voorafgaand aan gevraagde periode
                .append("AND NOT EXISTS (  SELECT kad_identif FROM kad_onrrnd_zk_archief WHERE dat_beg_geldh::date < '2019-01-01'::date and o.koz_identif=kad_identif ) ")
                //-- als we dit aanzetten komen we er niet meer uit...
                //.append("-- AND tax.kpr_nummer IS NULL
                // -- vervangen door een not exist op aanduiding2
                // in gibs onbekend
                .append("AND NOT EXISTS ( SELECT aanduiding2 FROM tax.belastingplichtige WHERE o.aanduiding2 = aanduiding2 )");

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
// herschrijf de NOT IN naar NOT EXISTS tbv. PG 9.6
//        StringBuilder sql = new StringBuilder("SELECT DISTINCT ")
//                .append("adr.koz_identif, ")
//                .append("adr.gemeentecode, ")
//                .append("adr.sectie, ")
//                .append("adr.perceelnummer, ")
//                .append("adr.appartementsindex, ")
//                .append("adr.loc_omschr, ")
//                .append("adr.benoemdobj_identif, ")
//                .append("adr.straatnaam, ")
//                .append("adr.huisnummer, ")
//                .append("adr.huisletter, ")
//                .append("adr.huisnummer_toev, ")
//                .append("adr.woonplaats, ")
//                .append("adr.postcode ")
//                .append("FROM ").append(VIEW_KAD_ONRRND_ZK_ADRES).append(" adr ")
//                .append(" WHERE '[")
//                .append(df.format(van)).append(",").append(df.format(tot)).append("]'::DATERANGE @> adr.begin_geldigheid::date ")
//                .append("AND adr.koz_identif NOT IN (SELECT kad_identif FROM kad_onrrnd_zk_archief WHERE '")
//                .append(df.format(van))
//                .append("'::date < dat_beg_geldh::date) ORDER BY adr.koz_identif");
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
                .append("AND NOT EXISTS (SELECT kad_identif FROM kad_onrrnd_zk_archief WHERE kad_identif = adr.koz_identif AND '")
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
// PG 9.6 -itt 10.9- heeft nog steeds problemen met "not in"-subquery; daarom herschreven naar "LEFT OUTER JOIN"
//        StringBuilder sql = new StringBuilder("SELECT DISTINCT ON (arch.koz_identif) ")
//                .append("arch.koz_identif, ")
//                .append("arch.eind_geldigheid, ")
//                .append("arch.gemeentecode, ")
//                .append("arch.sectie, ")
//                .append("arch.perceelnummer, ")
//                .append("arch.deelperceelnummer, ")
//                .append("arch.appartementsindex ")
//                // 0-padding
//                .append("FROM ")
//                .append(VIEW_KAD_ONRRND_ZK_ARCHIEF)
//                .append(" arch ")
//                // object heeft archief record in gevraagde periode
//                .append("WHERE '[")
//                .append(df.format(van)).append(",").append(df.format(tot))
//                .append("]'::DATERANGE @> arch.eind_geldigheid::date ")
//                // object niet meer in actuele tabel
//                .append("AND arch.koz_identif NOT IN (SELECT kad_identif FROM kad_onrrnd_zk) ")
//                // alleen de jongste archief record
//                .append("ORDER BY arch.koz_identif, arch.eind_geldigheid::date DESC");
        StringBuilder sql = new StringBuilder("SELECT DISTINCT ON (arch.koz_identif) ")
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
                .append("LEFT OUTER JOIN kad_onrrnd_zk koz ON arch.koz_identif = koz.kad_identif ")
                // object heeft archief record in gevraagde periode
                .append("WHERE '[")
                .append(df.format(van)).append(",").append(df.format(tot))
                .append("]'::DATERANGE @> arch.eind_geldigheid::date ")
                // object niet meer in actuele tabel
                .append("AND koz.kad_identif IS NULL ")
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
                .append("q.gemeentecode, ")
                .append("q.sectie, ")
                .append("q.perceelnummer, ")
                .append("q.deelperceelnummer, ")
                .append("q.appartementsindex, ")
                // altijd leeg/null want alleen als belastingplichtige onbekend is is verkoop relevant
                // .append("kpr_nummer, ")
                .append("z.ar_teller AS aandeel_teller, ")
                .append("z.ar_noemer AS aandeel_noemer, ")
                .append("z.fk_3avr_aand AS rechtcode, ")
                .append("avr.omschr_aard_verkregenr_recht AS rechtomschrijving ")
                // verkoop + datum
                .append("FROM ( SELECT brondocument.ref_id, max(brondocument.datum) AS datum FROM brondocument ")
                // NB "Akte van Koop en Verkoop" zit altijd in BRONDOCUMENT tabel
                .append("    WHERE brondocument.tabel='BRONDOCUMENT' AND brondocument.omschrijving = 'Akte van Koop en Verkoop' ")
                .append("         AND '[").append(df.format(van)).append(",").append(df.format(tot)).append("]'::DATERANGE @> brondocument.datum ")
                .append(" GROUP BY brondocument.ref_id) bron ")
                // samengestelde app_re en kad_perceel als q
                // zou evt ook "VIEW_KAD_ONRRND_ZK_ADRES" kunnen gebruuken, maar dat blijkt niet sneller
//                .append("LEFT JOIN (SELECT  ")
//                .append("  ar.sc_kad_identif, ")
//                .append("  ar.ka_kad_gemeentecode AS gemeentecode, ")
//                .append("  ar.ka_perceelnummer AS perceelnummer, ")
//                .append("  null AS deelperceelnummer, ")
//                .append("  ar.ka_sectie AS sectie, ")
//                .append("  ar.ka_appartementsindex AS appartementsindex ")
//                .append("FROM app_re ar ")
//                .append("UNION ALL SELECT ")
//                .append("  p.sc_kad_identif, ")
//                .append("  p.ka_kad_gemeentecode AS gemeentecode, ")
//                .append("  p.ka_perceelnummer AS perceelnummer, ")
//                .append("  p.ka_deelperceelnummer AS deelperceelnummer, ")
//                .append("  p.ka_sectie AS sectie, ")
//                .append("  null AS appartementsindex ")
//                .append("FROM kad_perceel p) q ")
                // einde samenstelling app_re en kad_perceel als q
//                .append("ON bron.ref_id::BIGINT=q.sc_kad_identif ")
                .append("LEFT JOIN ").append(VIEW_KAD_ONRRND_ZK_ADRES)
                .append(" q ON bron.ref_id::BIGINT=q.koz_identif ")
                .append("LEFT JOIN zak_recht z ON bron.ref_id::BIGINT = z.fk_7koz_kad_identif ")
                .append("LEFT JOIN aard_verkregen_recht avr ON z.fk_3avr_aand = avr.aand ")
                // gebruik left join ipv join; mail Dimitri dd.23 april 2019
                .append("LEFT JOIN ")
                .append(TAX_JOIN_CLAUSE_TBL_AANDUIDING2)
                .append("WHERE ")
                // met een rechthebbende
                .append("z.fk_8pes_sc_identif IS NOT null ")
                // eigendommen
                .append("AND z.fk_3avr_aand IN ('2','4','3','12') ")
                // alleen verkochte percelen waar belastingplichtige onbekend is
                .append("AND tax.kpr_nummer IS null");

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
        StringBuilder sql = new StringBuilder("SELECT DISTINCT ON (q.subject_identif) ")
                .append("q.subject_identif, ")
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
                .append("LEFT OUTER JOIN ").append(TAX_JOIN_CLAUSE_TBL_AANDUIDING2)

                .append("WHERE '[")
                // objecten met datum begin geldigheid in de periode "van"/"tot" inclusief,
                .append(df.format(van)).append(",").append(df.format(tot))
                .append("]'::DATERANGE @> q.begin_geldigheid::date ")
                // maar niet in de archief tabel met een datum voor "van".
                // .append("AND q.koz_identif NOT IN (SELECT kad_identif FROM kad_onrrnd_zk_archief WHERE '")
                //.append(df.format(van)).append("'::date < dat_beg_geldh::date) ")
                //
                //.append("AND NOT EXISTS (SELECT kad_identif FROM kad_onrrnd_zk_archief WHERE dat_beg_geldh::date < '")
                //.append(df.format(van))
                //.append("'::date AND q.koz_identif = kad_identif ) ")

                // onbekend in GIBS
                //.append("AND tax.kpr_nummer IS NULL ")
                .append("AND tax.aanduiding2 IS NULL ")
                // alleen de eerste naam met de oudste datum
                .append("ORDER BY q.subject_identif, q.begin_geldigheid ASC");

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
                .append("inp.sc_identif, ")
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

                SimpleModule module = new SimpleModule();
                ResultSetJSONSerializer serializer = new ResultSetJSONSerializer();
                ObjectMapper mapper = new ObjectMapper();

                stm.setQueryTimeout(QRY_TIMEOUT);
                stm.setFetchDirection(ResultSet.FETCH_FORWARD);
                stm.setFetchSize(JDBC_FETCH_SIZE);
                LOG.debug(stm);

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

        try (Connection c = ConfigUtil.getDataSourceRsgb().getConnection()) {
            c.setReadOnly(true);

            try (PreparedStatement stm = c.prepareStatement(sql)) {
                int index = 0;
                for (String p : params) {
                    stm.setString(index++, p);
                }

                stm.setQueryTimeout(QRY_TIMEOUT);
                stm.setFetchDirection(ResultSet.FETCH_FORWARD);
                stm.setFetchSize(JDBC_FETCH_SIZE);
                LOG.debug(stm);

                try (ResultSet r = stm.executeQuery();
                        FileOutputStream fos = new FileOutputStream(workDir + File.separator + bestandsNaam);
                        Writer out = new OutputStreamWriter(new BufferedOutputStream(fos), "UTF-8")) {
                    ResultSetMetaData metaData = r.getMetaData();
                    int numCols = metaData.getColumnCount();
                    LOG.trace("uitlezen query resultaat metadata");
                    // schrijf kolommen
                    for (int j = 1; j < (numCols + 1); j++) {
                        out.append(QUOTE);
                        out.append(metaData.getColumnName(j));
                        if (j < numCols) {
                            out.append(QUOTE);
                            out.append(SEP);
                        } else {
                            out.append(QUOTE);
                            out.append(NL);
                        }
                    }

                    count = 0;
                    LOG.trace("uitlezen en uitschrijven query resultaat");
                    while (r.next()) {
                        for (int k = 1; k < (numCols + 1); k++) {
                            // het zou mooier zijn om de type specifieke getters van de resultset te gebruiken,
                            // maar uiteindelijk doen we toch een toString() dus resultaat is gelijk.
                            String o = r.getString(k);
                            out.append(QUOTE);
                            out.append(o != null ? o : "");
                            if (k < numCols) {
                                out.append(QUOTE);
                                out.append(SEP);
                            } else {
                                out.append(QUOTE);
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
        boolean use_mv = Boolean.parseBoolean(getContext().getServletContext().getInitParameter("use_mv"));
        if (use_mv) {
            LOG.info("Gebruik materialized views in de queries.");
            VIEW_KOZ_RECHTHEBBENDE = VIEW_KOZ_RECHTHEBBENDE.replaceFirst("vb_", "mb_");
            VIEW_KAD_ONRRND_ZK_ADRES = VIEW_KAD_ONRRND_ZK_ADRES.replaceFirst("vb_", "mb_");
            VIEW_KAD_ONRRND_ZK_ARCHIEF = VIEW_KAD_ONRRND_ZK_ARCHIEF.replaceFirst("vb_", "mb_");
        } else {
            LOG.warn("Gebruik reguliere views in de queries (zeer langzaam).");
        }

        try {
            JDBC_FETCH_SIZE = Integer.parseInt(getContext().getServletContext().getInitParameter("jdbc_fetch_size"));
            LOG.info(String.format("Gebruik fetch size van: %s records", JDBC_FETCH_SIZE));
        } catch (Exception e) {
            // ignore
        }

        final String sep = getContext().getServletContext().getInitParameter("csv_separator_char");
        if (sep != null && !sep.isEmpty()) {
            SEP = sep;
            LOG.info(String.format("Gebruik '%s' als scheidingsteken in CSV", SEP));
        }

        final String quote = getContext().getServletContext().getInitParameter("csv_quote_char");
        if (quote != null && !quote.isEmpty()) {
            QUOTE = quote;
            LOG.info(String.format("Gebruik '%s' als aanhalingsteken in CSV", QUOTE));
        }

        try {
            QRY_TIMEOUT = Integer.parseInt(getContext().getServletContext().getInitParameter("jdbc_query_timeout"));
            LOG.info(String.format("Gebruik query timout van: %s seconden", QRY_TIMEOUT));
        } catch (Exception e) {
            // ignore
        }

    }

    @Override
    public ActionBeanContext getContext() {
        return context;
    }

    @Override
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
