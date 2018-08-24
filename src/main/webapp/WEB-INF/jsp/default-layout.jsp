<%@ page contentType="text/html" pageEncoding="UTF-8"%>
<%@include file="/WEB-INF/taglibs.jsp" %>
<stripes:layout-definition>
    <!DOCTYPE html>
    <html>
        <head>
            <title><stripes:layout-component name="title">BRMO BRK verschil service</stripes:layout-component></title>
            <meta charset="utf-8">
            <meta http-equiv="X-UA-Compatible" content="IE=edge">
            <link href="${contextPath}/styles/main.css" rel="stylesheet">
            <link href="${contextPath}/styles/ext-theme-crisp-all.css" rel="stylesheet">
            <stripes:layout-component name="html_head"/>
        </head>
        <body class="x-body">
            <div class="header">
                <h1>B3Partners BRMO BRK verschil service</h1>
            </div>
            <div class="content">
                <stripes:layout-component name="contents"/>
            </div>
            <div class="footer"></div>
        </body>
    </html>
</stripes:layout-definition>