# Ontwerp

## gemaakte keuzen

Omdat de data vanuit GIBS is afgeleid van de ouderwetse MO-levering van Kadaster
zijn er een aantal mismatches te verwachten. Tevens zijn we daardoor gewongen
om een eigen `belastingplichtigen` tabel met GIBS export te gebruiken.
De data van GIBS wordt gebruikt as-is, dat heeft wel een performance impact omdat
de meeste waarden zijn voorzien van voorloop nullen, mogelijk kan dat met schaduw
kolommen (waarin voorloop nullen zijn verwijderd) opgelost worden.

### geen gebruik rsgb datamodel

In het RSGB model is een kolom `fk_10pes_sc_identif` in de tabel `kad_onrrnd_zk`
welke in het model is beschreven als "heeft als voornaamste zakelijk gerechtigde",
echter deze dient gevuld te worden met een identifyer die in de "subject"
tabel voorkomt (dus incl. namespace). Vanuit GIBS komt slechts een uit cijfers
bestaande string waarvan het niet duidelijk is of het om een BSN dan wel een RSIN
of iets anders gaat, bovendien worden vanuit de BRK subjecten met een BRK-eigen
identifier geleverd welke (kennelijk) niet bekend is in GIBS.
Zou deze kolom wel gebruikt worden dan zou dit ook in het archief van de onroerende
zaak tabel mee worden genomen.

### negeer deelperceelnummer

In de BRK bestaat het concept "deelperceel" niet en deze waarde word dus vanuit
de BRK Levering niet gevuld (controle bij prov. Noord Brabant en andere klanten
heeft dat ook aangetoond), derhalve wordt het deelperceelnummer genegeerd in
het matchen op kadastrale aanduiding.