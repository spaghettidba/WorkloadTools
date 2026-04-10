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
    Remove Fragment elements that only contain DirectoryRef children (i.e. the
    directory-structure fragments that heat.exe generates for each subdirectory).
    Those directories are already declared in harvest.wxs (SqlWorkload) and
    re-declaring them in secondary harvest files causes LGHT0091 "Duplicate symbol"
    errors during linking. The ComponentGroup fragment is unaffected because it
    does not contain a DirectoryRef as a direct child.
    -->
  <xsl:template match="wix:Fragment[wix:DirectoryRef and not(wix:ComponentGroup)]" />

</xsl:stylesheet>
