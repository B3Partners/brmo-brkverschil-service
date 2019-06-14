# Installatie

## Database

In de **staging** database dient een gebruiker te worden toegevoegd, gebruik hiervoor het script `insert_user.sql`.
Indien gewenst kan het wachtwoord worden aangepast, in de database staat de SHA1 hash van het wachtwoord.
In de **rsgb** database dient een schema `tax`  met daarin een tabel `belastingplichtige` te worden aangemaakt, gebruik hiervoor
het script `create_belastingplichtige.sql`, na het aanmaken van deze tabel kan deze worden gevuld vanuit een CSV bestand of via ETL.

```sql
-- pas het onderstaande pad aan naar het bestand met de CSV dump uit gibs in het formaat
-- AAL01;A ;00001;G;0000;000003615455240
TRUNCATE TABLE tax.belastingplichtige;
COPY tax.belastingplichtige(gemeentecode, sectie, perceelnummer, deelperceelnummer, appartementsindex, kpr_nummer)
FROM '/home/mark/dev/projects/brmo-brkverschil-service/data/b3eigwb.csv' DELIMITER ';' CSV;
```

## Webapplicatie

Zet de webapplicatie in dezelfde webroot waarin ook de `brmo-service` staat, herstart daarna eventueel tomcat.
De applicatie gebruikt dezelfde JNDI databronnen als de `brmo-service`.
De GUI van de applicatie is te breiken op [http://server:poort/brmo-brkverschil-service/].
Aanmelden met (default) `mutaties` / `mutaties`

De REST services zijn met BASIC authenticatie beveiligd, de webapplicatie zelf
is met FORM based authenticatie beveiligd.

### configuratie parameters

De webapplicatie kan met een aantal context parameters worden getuned, in onderstaande tabel een overzicht.

| parameter         | default | omschrijving                                            |
| ----------------- | --------|---------------------------------------------------------|
|use_mv             |true     |`true` om materialized views te gebruiken in de queries, `false` om reguliere views te gebruiken |
|jdbc_fetch_size    |1000     | aantal records om in 1 keer op te halen uit de database |
|jdbc_query_timeout |600      | aantal seconden voordat een query timeout fout optreedt |
|csv_separator_char | ;       | csv kolom scheidingsteken                               |
|csv_quote_char     | <blanco>| csv waarde/aanhalingsteken                              |
