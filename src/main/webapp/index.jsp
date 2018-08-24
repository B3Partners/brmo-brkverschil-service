<%@include file="/WEB-INF/taglibs.jsp" %>
<%@page contentType="text/html" pageEncoding="UTF-8"%>
<stripes:layout-render name="/WEB-INF/jsp/default-layout.jsp">
    <stripes:layout-component name="contents">
        <p>BRMO BRK verschil service</p>
        <ul>
            <li><a href="${contextPath}/rest/mutaties?van=2018-08-01">mutaties vanaf 2018-08-01</li>
        </ul>
    </stripes:layout-component>
</stripes:layout-render>