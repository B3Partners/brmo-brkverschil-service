# Developer notes


## release maken

Een release bouwen begint met het uitvoeren van het commando `mvn clean release:prepare`
daarbij wordt voor de verschillende artifacten om versienummers gevraagd,
zowel voor voor de release als de volgende ontwikkel versie.
Tevens wordt er om een naam voor een tag gevraagd. In principe kan alle informatie op de
commandline worden meegegeven, bijvoorbeeld:

```
mvn release:prepare -l rel-prepare.log -DdevelopmentVersion=1.6.1-SNAPSHOT -DreleaseVersion=1.6.0 -Dtag=v1.6.0 -T1
mvn release:perform -l rel-perform.log
```

Met het commando `mvn release:perform` wordt daarna, op basis van de tag uit de
stap hierboven, de release gebouwd en gedeployed naar de repository uit de
pom file. De release bestaat uit jar en war files met daarbij oa. ook de javadoc.
Voor het hele project kan dit even duren, oa. omdat de javadoc gebouwd wordt.

### Maven site bouwen en online brengen

De Maven site voor deze applicatie leeft in de `gh-pages` branch van de repository, met onderstaande commando's kan de site worden bijgwerkt en online gebracht.

- `cd target/checkout` (als je dit direct na een release doet, dan staat de getaggede versie daarin, anders gewoon vanuit de root)
- `mvn -T1 site site:stage`
- `mvn scm-publish:publish-scm`

## Integratie en unit tests

Er is een Maven profiel (postgresql) voor de ondersteunde database gedefinieerd,
het profiel zorgen ervoor dat de juist JDBC driver beschikbaar komt in de test suites,
tevens kan daarmee het juiste configuratie bestande worden geladen.

| unit tests | integratie tests |
| ---------- | -----------------|
|Naamgeving conventie `<Mijn>Test.java`  |Naamgeving conventie `<Mijn>IntegrationTest.java`  |
|Zelfstandige tests, zonder runtime omgeving benodigdheden, eventueel voorzien van een data bestand, maar zonder verdere afhankelijkheden.  |Tests die een database omgeving en/of servlet container nodig hebben.  |
|Unit tests worden onafhankelijk van het gebruikte Maven profiel uitgevoerd, in principe tijdens iedere full build, tenzij er een `skip` optie voor het overslaan van de tests wordt meegegeven.  |Unit tests worden afhankelijk van het gebruikte Maven profiel uitgevoerd.  |

Met het commando `mvn clean test tomcat7:run -Ppostgresql` kan een tomcat instantie in de lucht worden gebracht om tegen te testen, zie verder het `postgresql` profiel in de `pom.xml` file.

### database configuratie

De te gebruiken database smaak wordt middels de `database.properties.file` property in de pom.xml van de
module of via commandline ingesteld.

Voor gebruik van de propertyfile in een integratie test kun je overerven van een
abstracte klasse in verschillende modules.

De applicatie gebruikt het `rsbg` schema van de BRMO voor bevraging en het `staging` schema voor de authenticatie.
