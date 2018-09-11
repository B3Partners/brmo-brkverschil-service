# Installatie

## Database

In de **staging** database dient een gebruiker te worden toegevoegd, gebruik hiervoor het script `insert_user.sql`.
Indien gewenst kan het wachtwoord worden aangepast, in de database staat de SHA1 hash van het wachtwoord.
In de **rsgb** database dient een tabel `belastingplichtige` te worden aangemaakt, gebruik hiervoor
het script `create_belastingplichtige.sql`, na het aanmaken van deze tabel kan deze worden gevuld vanuit een CSV bestand of via ETL.

```sql
TRUNCATE TABLE belastingplichtige;
COPY belastingplichtige(ka_kad_gemeentecode,ka_sectie,ka_perceelnummer,ka_deelperceelnummer,ka_appartementsindex,kpr_nummer)
FROM '../brmo-brkverschil-service/data/b3eigwb.csv' DELIMITER ';' CSV;
```


## Webapplicatie

Zet de webapplicatie in dezelfde webroot waarin ook de `brmo-service` staat, herstart daarna eventueel tomcat.
De applicatie gebruikt dezelfde JNDI databronnen als de `brmo-service`.
De GUI van de applicatie is te breiken op [http://server:poort/brmo-brkverschil-service/].
Aanmelden met (default) `mutaties` / `mutaties`

