-- drop en maak de belastingplichtige tabel aan
DROP TABLE public.belastingplichtige cascade;

CREATE TABLE public.belastingplichtige (
  ka_kad_gemeentecode  character varying(6) NOT NULL,
  ka_sectie            character varying(3) NOT NULL,
  ka_perceelnummer     character varying(6) NOT NULL,
  ka_deelperceelnummer character varying(6),
  ka_appartementsindex character varying(6),
  kpr_nummer           character varying(18),
  id                   bigserial NOT NULL,
  CONSTRAINT belastingplichtige_pkey PRIMARY KEY (id)
)
WITH (
  OIDS = FALSE
);

CREATE INDEX ON public.belastingplichtige (ka_kad_gemeentecode, ka_sectie, ka_perceelnummer);

COMMENT ON TABLE public.belastingplichtige IS 'Belastingplichtigen uit GIBS - niet-RSGB tabel';
COMMENT ON COLUMN public.belastingplichtige.ka_kad_gemeentecode IS 'Groepsattribuut Kadastrale aanduiding APPARTEMENTSRECHT.Kadastrale gemeentecode - Kadastrale gemeentecode';
COMMENT ON COLUMN public.belastingplichtige.ka_sectie IS 'Groepsattribuut Kadastrale aanduiding APPARTEMENTSRECHT.Sectie - Sectie met voorloop nullen';
COMMENT ON COLUMN public.belastingplichtige.ka_perceelnummer IS 'Groepsattribuut Kadastrale aanduiding APPARTEMENTSRECHT.Perceelnummer - Perceelnummer met voorloop nullen';
COMMENT ON COLUMN public.belastingplichtige.ka_deelperceelnummer IS 'Groepsattribuut Kadastrale aanduiding KADASTRAAL PERCEEL.Deelperceelnummer - Deelperceelnummer met voorloop nullen';
COMMENT ON COLUMN public.belastingplichtige.ka_appartementsindex IS 'Groepsattribuut Kadastrale aanduiding APPARTEMENTSRECHT.Appartementsindex - Appartementsindex met voorloop nullen';
COMMENT ON COLUMN public.belastingplichtige.kpr_nummer IS 'KPR nummer uit GIBS';
