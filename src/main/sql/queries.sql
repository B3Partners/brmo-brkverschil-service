-- nieuwe app_re en kad_perceel
SELECT DISTINCT
    o.kad_identif,
    o.dat_beg_geldh,
    q.ka_kad_gemeentecode,
    q.ka_perceelnummer,
    q.ka_deelperceelnummer,
    q.ka_sectie,
    q.ka_appartementsindex,
    q.grootte_perceel,
    q.x,
    q.y,
    b.kpr_nummer,
    z.ar_teller,
    z.ar_noemer,
    z.fk_3avr_aand,
    avr.omschr_aard_verkregenr_recht,
    h.fk_sc_rh_koz_kad_identif AS ontstaan_uit
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
    o.kad_identif=q.sc_kad_identif
LEFT JOIN
    zak_recht z
ON
    o.kad_identif=z.fk_7koz_kad_identif
LEFT JOIN
    aard_verkregen_recht avr
ON
    z.fk_3avr_aand=avr.aand
LEFT JOIN 
    kad_onrrnd_zk_his_rel h
ON
    o.kad_identif=h.fk_sc_lh_koz_kad_identif
JOIN
    belastingplichtige b
ON
    (
        q.ka_kad_gemeentecode=b.ka_kad_gemeentecode
    AND q.ka_sectie=b.ka_sectie
    AND q.ka_perceelnummer=b.ka_perceelnummer
    AND COALESCE(q.ka_deelperceelnummer,'')=COALESCE(b.ka_deelperceelnummer,'')
    AND COALESCE(q.ka_appartementsindex,'')=COALESCE(b.ka_appartementsindex,'') )
WHERE
    '[2018-08-01,2018-08-27]'::daterange @> dat_beg_geldh::DATE
AND kad_identif NOT IN
    (
        SELECT
            kad_identif
        FROM
            kad_onrrnd_zk_archief
        WHERE
            '2018-08-01'::DATE < dat_beg_geldh::DATE)
AND z.fk_8pes_sc_identif IS NOT NULL



-- verkopen
SELECT DISTINCT
    b.ref_id,
    b.datum::text,
    q.ka_kad_gemeentecode,
    q.ka_sectie,
    q.ka_perceelnummer,
    q.ka_deelperceelnummer,
    q.ka_appartementsindex,
    k.kpr_nummer,
    z.ar_teller,
    z.ar_noemer,
    z.fk_3avr_aand,
    avr.omschr_aard_verkregenr_recht
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
            brondocument.ref_id) b
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
    b.ref_id=q.sc_kad_identif::text
LEFT JOIN
    zak_recht z
ON
    b.ref_id=z.fk_7koz_kad_identif::text
LEFT JOIN
    aard_verkregen_recht avr
ON
    z.fk_3avr_aand=avr.aand
JOIN
    belastingplichtige k
ON
    (
        q.ka_kad_gemeentecode=k.ka_kad_gemeentecode
    AND q.ka_sectie=k.ka_sectie
    AND q.ka_perceelnummer=k.ka_perceelnummer
    AND COALESCE(q.ka_deelperceelnummer,'')=COALESCE(k.ka_deelperceelnummer,'')
    AND COALESCE(q.ka_appartementsindex,'')=COALESCE(k.ka_appartementsindex,'') )
WHERE
    '[2018-08-01,2018-08-27]'::daterange @> b.datum
AND z.fk_8pes_sc_identif IS NOT NULL;

-- nieuwe zaken met adres
SELECT DISTINCT
    o.kad_identif,
    adr.gemeentecode,
    adr.sectie,
    adr.perceelnummer,
    adr.appartementsindex,
    o.lo_loc__omschr,
    adr.benoemdobj_identif,
    adr.straatnaam,
    adr.huisnummer,
    adr.huisletter,
    adr.huisnummer_toev,
    adr.woonplaats,
    adr.postcode
FROM
    kad_onrrnd_zk o
LEFT JOIN
    v_kad_onrrnd_zk_adres adr
ON
    adr.koz_identif = o.kad_identif
WHERE
    '[2018-08-01,2018-09-01]'::DATERANGE @> dat_beg_geldh::DATE
AND kad_identif NOT IN (SELECT kad_identif FROM kad_onrrnd_zk_archief WHERE '2018-08-01'::DATE < dat_beg_geldh::DATE);


-- vervallen objecten
SELECT DISTINCT ON (k.kad_identif)
    k.kad_identif,
    k.datum_einde_geldh,
    q.ka_kad_gemeentecode,
    q.ka_sectie,
    q.ka_perceelnummer,
    q.ka_deelperceelnummer,
    q.ka_appartementsindex
FROM
    kad_onrrnd_zk_archief k
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
            app_re_archief ar
        UNION ALL
        SELECT
            p.sc_kad_identif,
            p.ka_kad_gemeentecode,
            p.ka_perceelnummer,
            p.ka_deelperceelnummer,
            p.ka_sectie,
            NULL AS ka_appartementsindex
        FROM
            kad_perceel_archief p) q
ON
    k.kad_identif=q.sc_kad_identif
WHERE
    '[2018-08-01,2018-08-27]'::DATERANGE @> k.datum_einde_geldh::DATE
AND k.kad_identif NOT IN
    (
        SELECT
            kad_identif
        FROM
            kad_onrrnd_zk)
ORDER BY
    k.kad_identif,
    k.datum_einde_geldh::DATE DESC
    
    
    
-- gewijzigde subjecten (van ander_nat_pers (zonder bsn) naar ingeschr_nat_prs (met bsn) overgegaan
SELECT
    i.bsn,
    h.datum
FROM
    ingeschr_nat_prs i
LEFT JOIN
    herkomst_metadata h
ON
    i.sc_identif = h.waarde
WHERE
    i.sc_identif IN
    (
        SELECT
            sc_identif
        FROM
            ander_nat_prs)
AND h.tabel='subject'
AND '[2018-08-01,2018-08-27]'::daterange @> h.datum::DATE





-- oudste versie van object in archief in de periode die ook in actueel zitten met perceel_grootte
-- TODO check of datum logica klopt!
/*SELECT DISTINCT ON (za.kad_identif)
        za.kad_identif,
        za.dat_beg_geldh,
        za.datum_einde_geldh,
        pa.grootte_perceel 
FROM kad_onrrnd_zk_archief za, kad_perceel_archief pa
WHERE '[2017-12-01,2018-09-01]'::DATERANGE @> dat_beg_geldh::DATE 
AND za.clazz = 'KADASTRAAL PERCEEL'
AND za.kad_identif = 57590619170000
AND za.dat_beg_geldh = pa.sc_dat_beg_geldh
AND za.kad_identif = pa.sc_kad_identif
AND za.kad_identif IN (
        SELECT kad_identif FROM kad_onrrnd_zk WHERE '[2017-12-01,2018-09-01]'::DATERANGE @> dat_beg_geldh::DATE 
)
ORDER BY
    za.kad_identif,
    za.datum_einde_geldh::DATE ASC*/

-- percelen met aangepaste oppervlakte
-- TODO check of datum logica klopt!
SELECT DISTINCT ON (za.kad_identif) 
    za.kad_identif,
    za.dat_beg_geldh,
    pa.grootte_perceel AS opp_oud,
    k.grootte_perceel  AS opp_actueel
FROM
    kad_onrrnd_zk_archief za,
    kad_perceel_archief pa,
    kad_perceel k
WHERE
    '[2018-01-01,2018-09-01]'::DATERANGE @> za.dat_beg_geldh::DATE
AND za.dat_beg_geldh = pa.sc_dat_beg_geldh
AND za.kad_identif = pa.sc_kad_identif
AND za.kad_identif = k.sc_kad_identif
AND za.kad_identif IN (
        SELECT kad_identif FROM kad_onrrnd_zk
                WHERE '[2018-01-01,2018-09-01]'::DATERANGE @> dat_beg_geldh::DATE 
)
--AND za.clazz = 'KADASTRAAL PERCEEL'
AND pa.grootte_perceel != k.grootte_perceel
    --AND za.kad_identif = 57590619170000
ORDER BY
    za.kad_identif,
    za.dat_beg_geldh DESC








-- nieuwe subjecten
SELECT DISTINCT ON (o.naam)
    --o.koz_identif,
    o.begin_geldigheid,
    /*
    o.gemeentecode,
    o.perceelnummer,
    o.deelperceelnummer,
    o.sectie,
    o.appartementsindex,
    o.aandeel,
    o.omschr_aard_verkregenr_recht,
    */
    o.soort,
    o.geslachtsnaam,
    o.voorvoegsel,
    o.voornamen,
    o.naam,
    o.woonadres,
    o.geboortedatum,
    o.overlijdensdatum,
    o.bsn,
    o.rsin,
    o.kvk_nummer,
    o.straatnaam,
    o.huisnummer,
    o.huisletter,
    o.huisnummer_toev,
    o.postcode,
    o.woonplaats
    --b.kpr_nummer
FROM
    vb_koz_rechth o
    --mb_koz_rechth o
LEFT JOIN
    belastingplichtige b
ON (
        o.gemeentecode=b.ka_kad_gemeentecode
    AND o.sectie=b.ka_sectie
    AND o.perceelnummer=b.ka_perceelnummer
    AND COALESCE(o.deelperceelnummer,'')=COALESCE(b.ka_deelperceelnummer,'')
    AND COALESCE(o.appartementsindex,'')=COALESCE(b.ka_appartementsindex,'') 
   )
-- nieuwe objecten
WHERE
    '[2018-08-01,2018-08-27]'::daterange @> o.begin_geldigheid::DATE
AND o.koz_identif NOT IN
    (
        SELECT
            kad_identif
        FROM
            kad_onrrnd_zk_archief
        WHERE
            '2018-08-01'::DATE < dat_beg_geldh::DATE)
-- die niet gekoppeld kunnen worden
AND b.kpr_nummer IS NULL
-- alleen de eerste naam
ORDER BY
    o.naam,
    o.begin_geldigheid ASC


