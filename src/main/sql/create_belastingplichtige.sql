DROP TABLE public.belastingplichtige;

CREATE TABLE public.belastingplichtige (
  ka_kad_gemeentecode character varying(5), -- Groepsattribuut Kadastrale aanduiding APPARTEMENTSRECHT.Kadastrale gemeentecode - Kadastrale gemeentecode
  ka_sectie character varying(255), -- Groepsattribuut Kadastrale aanduiding APPARTEMENTSRECHT.Sectie - Sectie
  ka_perceelnummer character varying(15), -- Groepsattribuut Kadastrale aanduiding APPARTEMENTSRECHT.Perceelnummer - Perceelnummer
  ka_deelperceelnummer character varying(4), -- Groepsattribuut Kadastrale aanduiding KADASTRAAL PERCEEL.Deelperceelnummer - Deelperceelnummer
  ka_appartementsindex character varying(4), -- Groepsattribuut Kadastrale aanduiding APPARTEMENTSRECHT.Appartementsindex - Appartementsindex
  bsn numeric(9,0), -- N9 - Burgerservicenummer
  id bigserial NOT NULL
)
WITH (
  OIDS = FALSE
);


ALTER TABLE public.belastingplichtige ADD PRIMARY KEY (id);

COMMENT ON COLUMN public.belastingplichtige.ka_kad_gemeentecode IS 'Groepsattribuut Kadastrale aanduiding APPARTEMENTSRECHT.Kadastrale gemeentecode - Kadastrale gemeentecode';
COMMENT ON COLUMN public.belastingplichtige.ka_sectie IS 'Groepsattribuut Kadastrale aanduiding APPARTEMENTSRECHT.Sectie - Sectie';
COMMENT ON COLUMN public.belastingplichtige.ka_perceelnummer IS 'Groepsattribuut Kadastrale aanduiding APPARTEMENTSRECHT.Perceelnummer - Perceelnummer';
COMMENT ON COLUMN public.belastingplichtige.ka_deelperceelnummer IS 'Groepsattribuut Kadastrale aanduiding KADASTRAAL PERCEEL.Deelperceelnummer - Deelperceelnummer';
COMMENT ON COLUMN public.belastingplichtige.ka_appartementsindex IS 'Groepsattribuut Kadastrale aanduiding APPARTEMENTSRECHT.Appartementsindex - Appartementsindex';
COMMENT ON COLUMN public.belastingplichtige.bsn IS 'N9 - Burgerservicenummer';