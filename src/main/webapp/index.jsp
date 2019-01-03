<%@include file="/WEB-INF/taglibs.jsp" %>
<%@page contentType="text/html" pageEncoding="UTF-8"%>
<stripes:layout-render name="/WEB-INF/jsp/default-layout.jsp">
    <stripes:layout-component name="contents">
        <h2>BRMO BRK verschil service</h2>
        <p>Het rest endpoint <code>${contextPath}/rest/mutaties</code> kan met verschillende parameters 
            aangeroepen worden. De parameters staan in onderstaande lijst beschreven.</p>
        <dl>
            <dt><strong>van</strong> (verplicht)</dt><dd>datum vanaf in yyyy-mm-dd formaat, de periode is inclusief deze datum</dd>
            <dt><strong>tot</strong> (optioneel)</dt><dd>datum tot in yyyy-mm-dd formaat, de periode is inclusief deze datum, default is de actuele datum</dd>
            <dt><strong>f</strong> (optioneel)</dt><dd>formaat, <code>json</code> of <code>csv</code>, adnere formaten zijn niet beschikbaar</dd>
        </dl>
        <h3>Voorbeelden van de REST API URLs</h3>
        <ul>
            <li><a href="${contextPath}/rest/mutaties?van=2018-08-01">mutaties vanaf 2018-08-01</a></li>
            <li><a href="${contextPath}/rest/mutaties?van=2018-08-01&f=csv">mutaties vanaf 2018-08-01</a> in csv formaat</li>
            <li><a href="${contextPath}/rest/mutaties?van=2018-08-01&tot=2018-09-01">mutaties vanaf 2018-08-01 tot 2018-09-01</a></li>
            <li><a href="${contextPath}/rest/mutaties"><strong>foutief verzoek</strong></a> (geen vanaf datum)</li>
            <li><a href="${contextPath}/rest/mutaties?van=18-08-01&tot=18-09-01"><strong>foutief verzoek</strong></a> (ongeldige datums)</li>
            <li><a href="${contextPath}/rest/mutaties?van=2018-09-01&tot=2018-08-01"><strong>foutief verzoek</strong></a> (vanaf datum voor tot datum)</li>
            <li><a href="${contextPath}/rest/mutaties?van=2018-08-01&f=text"><strong>foutief verzoek</strong></a> (geen geldig formaat) levert default formaat</li>
            <li><a href="${contextPath}/rest/ping" target="_blank">ping endpoint</a> geeft de datum terug in json</li>
            <li><a href="${contextPath}/rest/ping?tot=2018-09-01" target="_blank">ping endpoint</a> met "tot 2018-09-01"</li>
        </ul>
        <p><strong>NB: </strong>Omdat de REST service een ander authenticatie schema gebruikt dan de webpagina zal kilkken op
            bovenstaande links niet direct werken. Open de link in een incognito venster zodat er een nieuwe (basic)
            authenticatie challenge wordt gestuurd voor de REST call of gebruik "wget" of "curl".</p>
        <p>Hieronder zijn een aantal voorbeelden voor gebruik van `wget` en `curl` te vinden.
            Een voorbeeld met java code is te vinden in de <a href="https://github.com/B3Partners/brmo-brkverschil-service/blob/master/src/test/java/nl/b3p/brmo/verschil/stripes/PingActionBeanIntegrationTest.java">PingActionBeanIntegrationTest</a></p>
        <ul>
            <li><code>curl --user mutaties:mutaties "${contextService}${contextPath}/rest/mutaties?van=2018-08-01" --verbose --output test.zip</code></li>
            <li><code>wget --http-user=mutaties --http-password=mutaties "${contextService}${contextPath}/rest/mutaties?van=2018-08-01" -O test.zip</code></li>
            <li><code>wget --http-user=mutaties --http-password=mutaties "${contextService}${contextPath}/rest/mutaties?van=2018-08-01" -O test.zip;unzip -oq test.zip</code> Om bestanden meteen met 7-zip uit te pakken</li>
        </ul>
        <h3>Beveiliging</h3>
        <p>De REST endpoints zijn afgeschermd met BASIC authenticatie, in tegenstelling tot de web applicatie / GUI die middels
            FORM authenticatie is beschermd.</p>
    </stripes:layout-component>
</stripes:layout-render>