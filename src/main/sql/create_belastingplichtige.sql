-- maak schema
CREATE SCHEMA IF NOT EXISTS tax AUTHORIZATION rsgb;
COMMENT ON SCHEMA tax IS 'schema voor brmo-brkverschil-service koppel data';

-- drop en maak de belastingplichtige tabel nieuw aan
DROP TABLE IF EXISTS tax.belastingplichtige cascade;

CREATE TABLE tax.belastingplichtige (
  gemeentecode      character varying(6) NOT NULL,
  sectie            character varying(3) NOT NULL,
  perceelnummer     character varying(6) NOT NULL,
  deelperceelnummer character varying(6),
  appartementsindex character varying(6),
  kpr_nummer        character varying(18),
  aanduiding2       character varying(24),
  id                bigserial NOT NULL,
  CONSTRAINT belastingplichtige_pkey PRIMARY KEY (id)
)
WITH (
  OIDS = FALSE
);

CREATE INDEX belastingplichtige_gemeentecode_sectie_perceelnummer_idx ON tax.belastingplichtige (gemeentecode, sectie, perceelnummer);
CREATE INDEX belastingplichtige_aanduiding2_idx ON tax.belastingplichtige (aanduiding2);

COMMENT ON TABLE tax.belastingplichtige IS 'Belastingplichtigen uit GIBS - niet-RSGB tabel';
COMMENT ON COLUMN tax.belastingplichtige.gemeentecode IS 'Groepsattribuut Kadastrale aanduiding APPARTEMENTSRECHT.Kadastrale gemeentecode - Kadastrale gemeentecode';
COMMENT ON COLUMN tax.belastingplichtige.sectie IS 'Groepsattribuut Kadastrale aanduiding APPARTEMENTSRECHT.Sectie - Sectie met voorloop nullen';
COMMENT ON COLUMN tax.belastingplichtige.perceelnummer IS 'Groepsattribuut Kadastrale aanduiding APPARTEMENTSRECHT.Perceelnummer - Perceelnummer met voorloop nullen';
COMMENT ON COLUMN tax.belastingplichtige.deelperceelnummer IS 'Groepsattribuut Kadastrale aanduiding KADASTRAAL PERCEEL.Deelperceelnummer - Deelperceelnummer met voorloop nullen';
COMMENT ON COLUMN tax.belastingplichtige.appartementsindex IS 'Groepsattribuut Kadastrale aanduiding APPARTEMENTSRECHT.Appartementsindex - Appartementsindex met voorloop nullen';
COMMENT ON COLUMN tax.belastingplichtige.kpr_nummer IS 'KPR nummer uit GIBS';
COMMENT ON COLUMN tax.belastingplichtige.aanduiding2 IS 'Koppelsleutel, door software gevuld.';
