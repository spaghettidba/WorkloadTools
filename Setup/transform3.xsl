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

    ##########################################################################
    REMEMBER THAT THE MATCH IS CASE-SENSITIVE!!! 
    If your setup is not building, please double check the case of the items
    that you want to exclude
    ##########################################################################
    
    -->

  <xsl:key name="unwanted-search" match="wix:Component[not(contains(wix:File/@Source, '\ConvertWorkload.'))]" use="@Id" />
  <xsl:template match="wix:Component[key('unwanted-search', @Id)]" />
  <xsl:template match="wix:ComponentRef[key('unwanted-search', @Id)]" />

</xsl:stylesheet>