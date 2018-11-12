<?xml version="1.0" ?>
<xsl:stylesheet version="1.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:wix="http://schemas.microsoft.com/wix/2006/wi">

  <!-- Copy all attributes and elements to the output. -->
  <xsl:template match="@*|*">
    <xsl:copy>
      <xsl:apply-templates select="@*" />
      <xsl:apply-templates select="*" />
    </xsl:copy>
  </xsl:template>

  <xsl:output method="xml" indent="yes" />

  <!--
    This section includes all the files that need not be included
    in the setup. For instance, all local log files.
    -->
  
  <xsl:key name="log-search" match="wix:Component[contains(wix:File/@Source, '.log')]" use="@Id" />
  <xsl:template match="wix:Component[key('log-search', @Id)]" />
  <xsl:template match="wix:ComponentRef[key('log-search', @Id)]" />
</xsl:stylesheet>