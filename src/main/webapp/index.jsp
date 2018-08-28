<%@include file="/WEB-INF/taglibs.jsp" %>
<%@page contentType="text/html" pageEncoding="UTF-8"%>
<stripes:layout-render name="/WEB-INF/jsp/default-layout.jsp">
    <stripes:layout-component name="contents">
        <h2>BRMO BRK verschil service</h2>
        <p>Het rest endpoint <code>${contextPath}/rest/mutaties</code> kan met verschillende parameters 
            aangeroepen worden. De parameters staan in onderstaande lijst beschreven.</p>
        <dl>
            <dt><strong>van</strong> (verplicht)</dt><dd>datum vanaf in yyyy-mm-dd formaat</dd>
            <dt><strong>tot</strong> (optioneel)</dt><dd>datum tot in yyyy-mm-dd formaat</dd>
            <dt><strong>f</strong> (optioneel)</dt><dd>formaat, <code>json</code> of <code>csv</code></dd>
        </dl>
        <h3>Voorbeelden</h3>
        <ul>
            <li><a href="${contextPath}/rest/mutaties?van=2018-08-01">mutaties vanaf 2018-08-01</li>
            <li><a href="${contextPath}/rest/mutaties?van=2018-08-01&f=csv">mutaties vanaf 2018-08-01 in csv formaat</li>
            <li><a href="${contextPath}/rest/mutaties?van=2018-08-01&tot=2018-09-01">mutaties vanaf 2018-08-01 tot 2018-09-01</li>
            <li><a href="${contextPath}/rest/mutaties"><strong>foutief verzoek</strong> (geen vanaf datum)</li>
            <li><a href="${contextPath}/rest/ping" target="_blank">ping endpoint</a> geeft de datum terug in json</li>
            <li><a href="${contextPath}/rest/ping?tot=2018-09-01" target="_blank">ping endpoint</a> met "tot 2018-09-01"</li>
        </ul>
    </stripes:layout-component>
</stripes:layout-render>