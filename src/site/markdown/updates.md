# Update procedures

## versie 1.0.2 naar 1.0.3

### database

De tabel `belastingplichtige` in schema `tax` is aangepast met een extra kolom. 
Voer de volgende SQL statements uit om de tabel bij te werken:

```sql
ALTER TABLE tax.belastingplichtige ADD COLUMN aanduiding2 CHARACTER VARYING(24);
COMMENT ON COLUMN tax.belastingplichtige.aanduiding2 IS 'Koppelsleutel, door software gevuld';
CREATE INDEX belastingplichtige_aanduiding2_idx ON tax.belastingplichtige (aanduiding2);
UPDATE tax.belastingplichtige SET aanduiding2 = TRIM(LEADING '0' from gemeentecode)  || ' ' || TRIM(sectie) || ' ' || trim(LEADING '0' from perceelnummer) || ' ' || coalesce(trim(LEADING '0' from appartementsindex), '');
 
```

Er is een index nodig op kolom `sc_kad_identif` van tabel `kad_zak_recht` in schema `wdd`:

```sql
CREATE INDEX kad_zak_recht_sc_kad_identif_idx ON wdd.kad_zak_recht (sc_kad_identif);
```