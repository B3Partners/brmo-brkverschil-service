-- Ophalen nieuwe onroerende zaken
SELECT DISTINCT
    o.kad_identif,
    o.dat_beg_geldh,
    tax.gemeentecode,
    tax.perceelnummer,
    tax.deelperceelnummer,
    tax.sectie,
    tax.appartementsindex,
    tax.kpr_nummer,
    q.grootte_perceel,
    q.x,
    q.y,
    z.ar_teller                      AS aandeel_teller,
    z.ar_noemer                      AS aandeel_noemer,
    z.fk_3avr_aand                   AS rechtcode,
    avr.omschr_aard_verkregenr_recht AS rechtomschrijving,
    h.fk_sc_rh_koz_kad_identif       AS ontstaan_uit
FROM
    kad_onrrnd_zk o
LEFT JOIN
    (
        SELECT
            ar.sc_kad_identif,
            ar.ka_kad_gemeentecode,
            ar.ka_perceelnummer,
            NULL AS ka_deelperceelnummer,
            ar.ka_sectie,
            ar.ka_appartementsindex,
            NULL AS grootte_perceel,
            NULL AS x,
            NULL AS y
        FROM
            app_re ar
        UNION ALL
        SELECT
            p.sc_kad_identif,
            p.ka_kad_gemeentecode,
            p.ka_perceelnummer,
            p.ka_deelperceelnummer,
            p.ka_sectie,
            NULL AS ka_appartementsindex,
            p.grootte_perceel,
            ST_X(p.plaatscoordinaten_perceel) AS x,
            ST_Y(p.plaatscoordinaten_perceel) AS y
        FROM
            kad_perceel p) q
ON
    o.kad_identif = q.sc_kad_identif
LEFT JOIN
    zak_recht z
ON
    o.kad_identif = z.fk_7koz_kad_identif
LEFT JOIN
    aard_verkregen_recht avr
ON
    z.fk_3avr_aand = avr.aand
LEFT JOIN
    kad_onrrnd_zk_his_rel h
ON
    o.kad_identif = h.fk_sc_lh_koz_kad_identif
JOIN
    tax.belastingplichtige tax
ON
    (
        q.ka_kad_gemeentecode = trim(LEADING '0' FROM tax.gemeentecode)
    AND q.ka_sectie = tax.sectie
    AND q.ka_perceelnummer = trim(LEADING '0' FROM tax.perceelnummer)
    AND COALESCE(q.ka_appartementsindex, '') = COALESCE(trim(LEADING '0' FROM tax.appartementsindex), ''))
WHERE
    '[2017-08-01,2018-10-02]'::DATERANGE @> o.dat_beg_geldh::DATE
AND o.kad_identif NOT IN
    (
        SELECT
            kad_identif
        FROM
            kad_onrrnd_zk_archief
        WHERE
            '2017-08-01'::DATE < dat_beg_geldh::DATE)
AND z.fk_8pes_sc_identif IS NOT NULL;

-- Ophalen nieuwe onroerende zaken
SELECT DISTINCT
    adr.koz_identif,
    adr.gemeentecode,
    adr.sectie,
    adr.perceelnummer,
    adr.appartementsindex,
    adr.loc_omschr,
    adr.benoemdobj_identif,
    adr.straatnaam,
    adr.huisnummer,
    adr.huisletter,
    adr.huisnummer_toev,
    adr.woonplaats,
    adr.postcode
FROM
    vb_kad_onrrnd_zk_adres adr
WHERE
    '[2017-08-01,2018-10-02]'::DATERANGE @> adr.begin_geldigheid::DATE
AND adr.koz_identif NOT IN
    (
        SELECT
            kad_identif
        FROM
            kad_onrrnd_zk_archief
        WHERE
            '2017-08-01'::DATE < dat_beg_geldh::DATE);

-- Ophalen vervallen objecten
SELECT DISTINCT
ON
    (
        arch.koz_identif) arch.koz_identif,
    arch.eind_geldigheid,
    arch.gemeentecode,
    arch.sectie,
    arch.perceelnummer,
    arch.deelperceelnummer,
    arch.appartementsindex
FROM
    vb_kad_onrrnd_zk_archief arch
WHERE
    '[2017-08-01,2018-10-02]'::DATERANGE @> arch.eind_geldigheid::DATE
AND arch.koz_identif NOT IN
    (
        SELECT
            kad_identif
        FROM
            kad_onrrnd_zk)
ORDER BY
    arch.koz_identif,
    arch.eind_geldigheid::DATE DESC;

-- Ophalen object verkopen
SELECT DISTINCT
    bron.ref_id,
    bron.datum::text AS verkoopdatum,
    tax.gemeentecode,
    tax.sectie,
    tax.perceelnummer,
    tax.deelperceelnummer,
    tax.appartementsindex,
    tax.kpr_nummer,
    z.ar_teller                      AS aandeel_teller,
    z.ar_noemer                      AS aandeel_noemer,
    z.fk_3avr_aand                   AS rechtcode,
    avr.omschr_aard_verkregenr_recht AS rechtomschrijving
FROM
    (
        SELECT
            brondocument.ref_id,
            MAX(brondocument.datum) AS datum
        FROM
            brondocument
        WHERE
            brondocument.omschrijving = 'Akte van Koop en Verkoop'
        GROUP BY
            brondocument.ref_id) bron
LEFT JOIN
    (
        SELECT
            ar.sc_kad_identif,
            ar.ka_kad_gemeentecode,
            ar.ka_perceelnummer,
            NULL AS ka_deelperceelnummer,
            ar.ka_sectie,
            ar.ka_appartementsindex
        FROM
            app_re ar
        UNION ALL
        SELECT
            p.sc_kad_identif,
            p.ka_kad_gemeentecode,
            p.ka_perceelnummer,
            p.ka_deelperceelnummer,
            p.ka_sectie,
            NULL AS ka_appartementsindex
        FROM
            kad_perceel p) q
ON
    bron.ref_id = q.sc_kad_identif::text
LEFT JOIN
    zak_recht z
ON
    bron.ref_id = z.fk_7koz_kad_identif::text
LEFT JOIN
    aard_verkregen_recht avr
ON
    z.fk_3avr_aand = avr.aand
JOIN
    tax.belastingplichtige tax
ON
    (
        q.ka_kad_gemeentecode = trim(LEADING '0' FROM tax.gemeentecode)
    AND q.ka_sectie = tax.sectie
    AND q.ka_perceelnummer = trim(LEADING '0' FROM tax.perceelnummer)
    AND COALESCE(q.ka_appartementsindex, '') = COALESCE(trim(LEADING '0' FROM tax.appartementsindex), ''))
WHERE
    '[2017-08-01,2018-10-02]'::DATERANGE @> bron.datum
AND z.fk_8pes_sc_identif IS NOT NULL;

-- Ophalen oppervlakte veranderd objecten
SELECT DISTINCT
ON
    (
        za.kad_identif) za.kad_identif,
    k.ka_kad_gemeentecode  AS gemeentecode,
    k.ka_sectie            AS sectie,
    k.ka_perceelnummer     AS perceelnummer,
    k.ka_deelperceelnummer AS deelperceelnummer,
    za.dat_beg_geldh,
    pa.grootte_perceel AS opp_oud,
    k.grootte_perceel  AS opp_actueel
FROM
    kad_onrrnd_zk_archief za,
    kad_perceel_archief pa,
    kad_perceel k
WHERE
    '[2017-08-01,2018-10-02]'::DATERANGE @> za.dat_beg_geldh::DATE
AND za.dat_beg_geldh = pa.sc_dat_beg_geldh
AND za.kad_identif = pa.sc_kad_identif
AND za.kad_identif = k.sc_kad_identif
AND pa.grootte_perceel != k.grootte_perceel
AND za.kad_identif IN
    (
        SELECT
            kad_identif
        FROM
            kad_onrrnd_zk
        WHERE
            '[2017-08-01,2018-10-02]'::DATERANGE @> dat_beg_geldh::DATE)
ORDER BY
    za.kad_identif,
    za.dat_beg_geldh DESC;

-- Ophalen nieuwe subjecten
SELECT DISTINCT
ON
    (
        q.naam) q.begin_geldigheid,
    q.soort,
    q.geslachtsnaam,
    q.voorvoegsel,
    q.voornamen,
    q.naam,
    q.woonadres,
    q.geboortedatum,
    q.overlijdensdatum,
    q.bsn,
    q.rsin,
    q.kvk_nummer,
    q.straatnaam,
    q.huisnummer,
    q.huisletter,
    q.huisnummer_toev,
    q.postcode,
    q.woonplaats
FROM
    vb_koz_rechth q
LEFT JOIN
    tax.belastingplichtige tax
ON
    (
        q.gemeentecode = trim(LEADING '0' FROM tax.gemeentecode)
    AND q.sectie = tax.sectie
    AND q.perceelnummer = trim(LEADING '0' FROM tax.perceelnummer)
    AND COALESCE(q.appartementsindex, '') = COALESCE(trim(LEADING '0' FROM tax.appartementsindex), ''))
WHERE
    '[2017-08-01,2018-10-02]'::DATERANGE @> q.begin_geldigheid::DATE
AND q.koz_identif NOT IN
    (
        SELECT
            kad_identif
        FROM
            kad_onrrnd_zk_archief
        WHERE
            '2017-08-01'::DATE < dat_beg_geldh::DATE)
AND tax.kpr_nummer IS NULL
ORDER BY
    q.naam,
    q.begin_geldigheid ASC;

-- Ophalen BSN aangepast
SELECT
    inp.bsn,
    hm.datum::TEXT
FROM
    ingeschr_nat_prs inp
LEFT JOIN
    herkomst_metadata hm
ON
    inp.sc_identif = hm.waarde
WHERE
    inp.sc_identif IN
    (
        SELECT
            sc_identif
        FROM
            ander_nat_prs)
AND hm.tabel = 'subject'
AND '[2017-08-01,2018-10-02]'::DATERANGE @> datum::DATE;