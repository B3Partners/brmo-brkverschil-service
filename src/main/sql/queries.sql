-- Uitvoeren opdracht met parameters: van=2019-01-01 tot=2019-07-04

-- Ophalen nieuwe onroerende zaken
SELECT DISTINCT
    o.koz_identif,
    o.begin_geldigheid,
    o.gemeentecode,
    o.perceelnummer,
    o.deelperceelnummer,
    o.sectie,
    o.appartementsindex,
    bel.bpl_identif,
    bel.naam_zakelijk_gerechtigde,
    o.grootte_perceel,
    ST_X(ST_Transform(ST_SetSRID(ST_MakePoint(o.lon, o.lat), 4326), 28992)) AS x,
    ST_Y(ST_Transform(ST_SetSRID(ST_MakePoint(o.lon, o.lat), 4326), 28992)) AS y,
    z.ar_teller AS aandeel_teller,
    z.ar_noemer AS aandeel_noemer,
    z.fk_3avr_aand AS rechtcode,
    avr.omschr_aard_verkregenr_recht AS rechtomschrijving,
    h.fk_sc_rh_koz_kad_identif AS ontstaan_uit,
    h.aard,
    arch.gemeentecode AS ontstaan_uit_gemeentecode,
    arch.perceelnummer AS ontstaan_uit_perceelnummer,
    arch.deelperceelnummer AS ontstaan_uit_deelperceelnummer,
    arch.sectie AS ontstaan_uit_sectie,
    arch.appartementsindex AS ontstaan_uit_appartementsindex
FROM
    mb_kad_onrrnd_zk_adres o
    LEFT JOIN
        zak_recht z
        ON o.koz_identif = z.fk_7koz_kad_identif
    LEFT JOIN
        aard_verkregen_recht avr
        ON z.fk_3avr_aand = avr.aand
    LEFT JOIN
        kad_onrrnd_zk_his_rel h
        ON o.koz_identif = h.fk_sc_lh_koz_kad_identif
    LEFT JOIN
        mb_kad_onrrnd_zk_archief arch
        ON h.fk_sc_rh_koz_kad_identif = arch.koz_identif
    LEFT JOIN
        wdd.kad_zak_recht bel
        ON o.koz_identif = bel.sc_kad_identif
WHERE
    '[2019-01-01,2019-07-04]'::DATERANGE @> o.begin_geldigheid::date
    AND z.fk_3avr_aand IN
    (
        '2',
        '4',
        '3',
        '12'
    )
    AND z.fk_8pes_sc_identif IS NOT NULL
    AND NOT EXISTS
    (
        SELECT
            kad_identif
        FROM
            kad_onrrnd_zk_archief
        WHERE
            dat_beg_geldh::date < '2019-01-01'::date
            and o.koz_identif = kad_identif
    )
    AND NOT EXISTS
    (
        SELECT
            aanduiding2
        FROM
            tax.belastingplichtige
        WHERE
            o.aanduiding2 = aanduiding2
    );

-- Ophalen gekoppelde objecten
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
    mb_kad_onrrnd_zk_adres adr
WHERE
    '[2019-01-01,2019-07-04]'::DATERANGE @> adr.begin_geldigheid::date
    AND NOT EXISTS
    (
        SELECT
            kad_identif
        FROM
            kad_onrrnd_zk_archief
        WHERE
            kad_identif = adr.koz_identif
            AND '2019-01-01'::date < dat_beg_geldh::date
    )
ORDER BY
    adr.koz_identif;

-- Ophalen vervallen objecten
SELECT DISTINCT
    ON (arch.koz_identif) arch.koz_identif,
    arch.eind_geldigheid,
    arch.gemeentecode,
    arch.sectie,
    arch.perceelnummer,
    arch.deelperceelnummer,
    arch.appartementsindex
FROM
    mb_kad_onrrnd_zk_archief arch
    LEFT OUTER JOIN
        kad_onrrnd_zk koz
        ON arch.koz_identif = koz.kad_identif
WHERE
    '[2019-01-01,2019-07-04]'::DATERANGE @> arch.eind_geldigheid::date
    AND koz.kad_identif IS NULL
ORDER BY
    arch.koz_identif,
    arch.eind_geldigheid::date DESC;

-- Ophalen object verkopen
SELECT DISTINCT
    bron.ref_id,
    bron.datum::text as verkoopdatum,
    q.gemeentecode,
    q.sectie,
    q.perceelnummer,
    q.deelperceelnummer,
    q.appartementsindex,
    z.ar_teller AS aandeel_teller,
    z.ar_noemer AS aandeel_noemer,
    z.fk_3avr_aand AS rechtcode,
    avr.omschr_aard_verkregenr_recht AS rechtomschrijving
FROM
    (
        SELECT
            brondocument.ref_id,
            max(brondocument.datum) AS datum
        FROM
            brondocument
        WHERE
            brondocument.tabel = 'BRONDOCUMENT'
            AND brondocument.omschrijving = 'Akte van Koop en Verkoop'
            AND '[2019-01-01,2019-07-04]'::DATERANGE @> brondocument.datum
        GROUP BY
            brondocument.ref_id
    )
    bron
    LEFT JOIN
        mb_kad_onrrnd_zk_adres q
        ON bron.ref_id::BIGINT = q.koz_identif
    LEFT JOIN
        zak_recht z
        ON bron.ref_id::BIGINT = z.fk_7koz_kad_identif
    LEFT JOIN
        aard_verkregen_recht avr
        ON z.fk_3avr_aand = avr.aand
    LEFT JOIN
        tax.belastingplichtige tax
        ON q.aanduiding2 = tax.aanduiding2
WHERE
    z.fk_8pes_sc_identif IS NOT null
    AND z.fk_3avr_aand IN
    (
        '2',
        '4',
        '3',
        '12'
    )
    AND tax.kpr_nummer IS null;

-- Ophalen oppervlakte veranderd objecten
SELECT DISTINCT
    ON (za.kad_identif) za.kad_identif,
    k.ka_kad_gemeentecode AS gemeentecode,
    k.ka_sectie AS sectie,
    k.ka_perceelnummer AS perceelnummer,
    k.ka_deelperceelnummer AS deelperceelnummer,
    za.dat_beg_geldh,
    pa.grootte_perceel AS opp_oud,
    k.grootte_perceel AS opp_actueel
FROM
    kad_onrrnd_zk_archief za,
    kad_perceel_archief pa,
    kad_perceel k
WHERE
    '[2019-01-01,2019-07-04]'::DATERANGE @> za.dat_beg_geldh::DATE
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
            '[2019-01-01,2019-07-04]'::DATERANGE @> dat_beg_geldh::DATE
    )
ORDER BY
    za.kad_identif,
    za.dat_beg_geldh DESC;

-- Ophalen nieuwe subjecten
SELECT DISTINCT
    ON (q.subject_identif)
    q.subject_identif,
    q.begin_geldigheid,
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
    mb_koz_rechth q
    LEFT OUTER JOIN
        tax.belastingplichtige tax
        ON q.aanduiding2 = tax.aanduiding2
WHERE
    '[2019-01-01,2019-07-08]'::DATERANGE @> q.begin_geldigheid::date
    AND tax.aanduiding2 IS NULL
ORDER BY
    q.subject_identif,
    q.begin_geldigheid ASC;

-- Ophalen BSN aangepast
SELECT
    inp.bsn,
    inp.sc_identif,
    hm.datum::TEXT
FROM
    ingeschr_nat_prs inp
    LEFT JOIN
        herkomst_metadata hm
        ON inp.sc_identif = hm.waarde
WHERE
    inp.sc_identif IN
    (
        SELECT
            sc_identif
        FROM
            ander_nat_prs
    )
    AND hm.tabel = 'subject'
    AND '[2019-01-01,2019-07-04]'::DATERANGE @> datum::DATE;
