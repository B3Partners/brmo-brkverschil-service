DROP TABLE public.belastingplichtige;

CREATE TABLE public.belastingplichtige (
  ka_kad_gemeentecode character varying(6),
  ka_sectie character varying(3),
  ka_perceelnummer character varying(5),
  ka_deelperceelnummer character varying(4),
  ka_appartementsindex character varying(4),
  kpr_nummer character varying(16),
  id bigserial NOT NULL,
  CONSTRAINT belastingplichtige_pkey PRIMARY KEY (id)
)
WITH (
  OIDS = FALSE
);

COMMENT ON TABLE public.belastingplichtige IS 'Belastingplichtigen uit GIBS';
COMMENT ON COLUMN public.belastingplichtige.ka_kad_gemeentecode IS 'Groepsattribuut Kadastrale aanduiding APPARTEMENTSRECHT.Kadastrale gemeentecode - Kadastrale gemeentecode';
COMMENT ON COLUMN public.belastingplichtige.ka_sectie IS 'Groepsattribuut Kadastrale aanduiding APPARTEMENTSRECHT.Sectie - Sectie met voorloop nullen';
COMMENT ON COLUMN public.belastingplichtige.ka_perceelnummer IS 'Groepsattribuut Kadastrale aanduiding APPARTEMENTSRECHT.Perceelnummer - Perceelnummer met voorloop nullen';
COMMENT ON COLUMN public.belastingplichtige.ka_deelperceelnummer IS 'Groepsattribuut Kadastrale aanduiding KADASTRAAL PERCEEL.Deelperceelnummer - Deelperceelnummer met voorloop nullen';
COMMENT ON COLUMN public.belastingplichtige.ka_appartementsindex IS 'Groepsattribuut Kadastrale aanduiding APPARTEMENTSRECHT.Appartementsindex - Appartementsindex met voorloop nullen';
COMMENT ON COLUMN public.belastingplichtige.kpr_nummer IS 'KPR nummer uit GIBS';
