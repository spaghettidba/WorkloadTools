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
    This section includes all the files that MUST not be included
    in the setup, because they are already included by other projects.
    In this list we will include the libraries (nuget packages) that are
    already included in the SqlWorkload project, in order to avoid collisions.
    -->

  <xsl:key name="commandline-search" match="wix:Component[contains(wix:File/@Source, 'CommandLine.')]" use="@Id" />
  <xsl:template match="wix:Component[key('commandline-search', @Id)]" />
  <xsl:template match="wix:ComponentRef[key('commandline-search', @Id)]" />

  <xsl:key name="nlog-search" match="wix:Component[contains(wix:File/@Source, 'NLog.')]" use="@Id" />
  <xsl:template match="wix:Component[key('nlog-search', @Id)]" />
  <xsl:template match="wix:ComponentRef[key('nlog-search', @Id)]" />
</xsl:stylesheet>