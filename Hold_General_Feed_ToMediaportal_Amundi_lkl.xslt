<?xml version="1.0" encoding="ISO-8859-1"?>
<xsl:stylesheet version="1.0"
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
	xmlns:msxsl="urn:schemas-microsoft-com:xslt" 
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
	xsi:noNamespaceSchemaLocation="P:\dokumentation\ifr_dataportal\xml filer\beholdninger.xsd">
	
	<xsl:output method="xml" version="1.0" encoding="ISO-8859-1" indent="yes" cdata-section-elements="PapirNavn"/>


  <xsl:output method="xml" indent="yes"/>
  <xsl:strip-space elements="*"/>

  <xsl:key name="a-group" match="Row" use="Cell[@name = 'FondISIN']"/>

  <xsl:template match="/">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()"/>
    </xsl:copy>
  </xsl:template>

  <xsl:template match="Sheet">
    <Beholdninger>
	  <IndsenderID>Amundi</IndsenderID>
	  <FremsendelsesDatoTid><xsl:value-of select="//Row[@srcidx = 2]/Cell[@name = 'FremsendelsesDatoTid']"/></FremsendelsesDatoTid>
      <AntalPoster>
        <xsl:value-of select="count(Row[generate-id() = generate-id(key('a-group', Cell[@name = 'FondISIN'])[1])])"/>
      </AntalPoster>
      <AllePoster>
        <xsl:for-each select="Row[generate-id() = generate-id(key('a-group', Cell[@name = 'FondISIN'])[1])]">
          <EnPost>
		  
            <FondISIN><xsl:value-of select="Cell[@name = 'FondISIN']"/></FondISIN>
			<BeholdningsDato><xsl:variable name="dt" select="Cell[@name = 'BeholdningsDato']"/>
			<xsl:value-of select="concat(substring($dt, 1, 4),'-',substring($dt, 6, 2),'-',substring($dt, 9, 2))"/>
			</BeholdningsDato>
			<xsl:if test="string-length(Cell[@name = 'PubliceringsDato']) &gt; 0 ">
				<PubliceringsDato><xsl:variable name="dt2" select="Cell[@name = 'PubliceringsDato']"/>
					<xsl:value-of select="concat(substring($dt2, 1, 4),'-',substring($dt2, 6, 2),'-',substring($dt2, 9, 2))"/>
				</PubliceringsDato>
			</xsl:if>
			<xsl:if test="string-length(Cell[@name = 'Formue']) &gt; 0 ">
				<Formue><xsl:value-of select="Cell[@name = 'Formue']"/></Formue>
			</xsl:if>
			<xsl:if test="string-length(Cell[@name = 'BeholdningsValuta']) &gt; 0 ">
				<BeholdningsValuta>
					<xsl:value-of select="Cell[@name = 'BeholdningsValuta']"/>
				</BeholdningsValuta>
			</xsl:if>
		
            <Papirer>
              <xsl:for-each select="key('a-group', Cell[@name = 'FondISIN'])">
                <Papir>
                  <xsl:apply-templates select="."/>
                </Papir>
              </xsl:for-each>
            </Papirer>
          </EnPost>
        </xsl:for-each>
      </AllePoster>
    </Beholdninger>
  </xsl:template>

  <xsl:template match="Row">
	<Fondskode>
		<xsl:choose>
			<xsl:when test="string-length(Cell[@name = 'ISIN']) &gt; 0 ">
				<xsl:apply-templates select="Cell[@name = 'ISIN']"/>
				<xsl:if test="string-length(Cell[@name = 'SEDOL']) &gt; 0 ">
					<xsl:apply-templates select="Cell[@name = 'SEDOL']"/>
				</xsl:if>
			</xsl:when>
			<xsl:otherwise>
				<xsl:apply-templates select="Cell[@name = 'Egen']"/>
				<xsl:if test="string-length(Cell[@name = 'SEDOL']) &gt; 0 ">
					<xsl:apply-templates select="Cell[@name = 'SEDOL']"/>
				</xsl:if>
			</xsl:otherwise>
		</xsl:choose>
	</Fondskode>
	
    <xsl:apply-templates select="Cell[@name = 'PapirNavn']"/>
	<xsl:apply-templates select="Cell[@name = 'Antal_STK']"/>
	<Andel>
		<xsl:apply-templates select="Cell[@name = 'Procent']"/>
		<xsl:apply-templates select="Cell[@name = 'Vaerdi']"/>
	</Andel>
	<xsl:apply-templates select="Cell[@name = 'Valuta']"/>
	<xsl:apply-templates select="Cell[@name = 'Land']"/>
	<xsl:apply-templates select="Cell[@name = 'Kupon']"/>
	<xsl:apply-templates select="Cell[@name = 'UdloebsDato']"/>
	<xsl:apply-templates select="Cell[@name = 'CFIKode']"/>

  </xsl:template>

  <xsl:template match="Cell[@name = 'ISIN']" name="ISIN">
	<xsl:if test="string-length(.) &gt; 0 ">
					<ISIN>
						<xsl:value-of select="."/>
                    </ISIN>	
	</xsl:if>
  </xsl:template>

  <xsl:template match="Cell[@name = 'Egen']" name="Egen">
					<Egen>
						<xsl:value-of select="."/>
                    </Egen>							
  </xsl:template>

  <xsl:template match="Cell[@name = 'SEDOL']" name="SEDOL">
	<xsl:if test="string-length(.) &gt; 0 ">
					<SEDOL>
						<xsl:value-of select="."/>
                    </SEDOL>
	</xsl:if>
  </xsl:template>

  <xsl:template match="Cell[@name = 'PapirNavn']" name="PapirNavn">
	<xsl:if test="string-length(.) &gt; 0 ">
					<PapirNavn>
						<xsl:value-of select="."/>
                    </PapirNavn>
	</xsl:if>
  </xsl:template>

  <xsl:template match="Cell[@name = 'Antal_STK']" name="Antal_STK">
	<xsl:if test="string-length(.) &gt; 0 ">
					<Antal_STK>
						<xsl:value-of select="."/>
                    </Antal_STK>
	</xsl:if>
  </xsl:template>

  <xsl:template match="Cell[@name = 'Procent']" name="Procent">
	<xsl:if test="string-length(.) &gt; 0 ">
					<Procent>
						<xsl:value-of select="."/>
                    </Procent>	
	</xsl:if>					
  </xsl:template>

  <xsl:template match="Cell[@name = 'Vaerdi']" name="Vaerdi">
	<xsl:if test="string-length(.) &gt; 0 ">
					<Vaerdi>
						<xsl:value-of select="."/>
                    </Vaerdi>
	</xsl:if>
  </xsl:template>  

  <xsl:template match="Cell[@name = 'Valuta']" name="Valuta">
	<xsl:if test="string-length(.) &gt; 0 ">
					<Valuta>
						<xsl:value-of select="."/>
                    </Valuta>
	</xsl:if>
  </xsl:template>  

  <xsl:template match="Cell[@name = 'Land']" name="Land">
	<xsl:if test="string-length(.) &gt; 0 ">
					<Land>
						<xsl:value-of select="."/>
                    </Land>
	</xsl:if>
  </xsl:template>  

  <xsl:template match="Cell[@name = 'Kupon']" name="Kupon">
	<xsl:if test="string-length(.) &gt; 0 ">
					<Kupon>
						<xsl:value-of select="."/>
                    </Kupon>
	</xsl:if>
  </xsl:template> 

  <xsl:template match="Cell[@name = 'UdloebsDato']" name="UdloebsDato">
	<xsl:if test="string-length(.) &gt; 0 ">
					<UdloebsDato>
						<xsl:variable name="dt2" select="."/>
						<xsl:value-of select="concat(substring($dt2, 1, 4),'-',substring($dt2, 6, 2),'-',substring($dt2, 9, 2))"/>
                    </UdloebsDato>
	</xsl:if>
  </xsl:template>   
  <xsl:template match="Cell[@name = 'CFIKode']" name="CFIKode">
    <xsl:if test="string-length(.) &gt; 0 ">
                    <CFIKode>
                        <xsl:value-of select="."/>
                    </CFIKode> 	
	</xsl:if>
  </xsl:template> 
</xsl:stylesheet>