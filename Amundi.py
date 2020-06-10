import io
import urllib.parse
from datetime import datetime
import pandas as pd
import boto3
import os
import numpy as np

s3 = boto3.client('s3')

def transform(fileRequest, bucketResponse, keyResponse):

        message = ''
        result = []
           
        bucket = fileRequest['bucket']
        key = urllib.parse.unquote_plus(fileRequest["key"], encoding='utf-8')
        s3File = s3.get_object(Bucket=bucket, Key=key)
        file_content = s3File["Body"].read()
        readExcelData = io.BytesIO(file_content)
        df = pd.read_csv(readExcelData, sep=';',encoding='latin-1')

        di = {
                     'Disponibilites a terme':'MRC',	'Disponibilites immediates':'MRC',	'Frais de gestion':'MRC',	'Change a terme, swaps cambistes (jambe recue)':'',	'Swap sur risque de defaut':'',	'Change a terme, swaps cambistes  (jambe versee)':'',	'MUTUAL FUND M M':'EU',	'MUTUAL FUND INTL BOND':'EU',	'MUTUAL FUND EURO BOND':'EU',	'Obligation classique':'DB',	'Swaps de taux (jambe recue)':'',	'Swaps de taux (jambe versee)':'',	"C.D.N / C.D's":'MRC',	'Obligation demembree':'DB',	'Billet de tresorerie / comercial paper':'MRC',	'FCC':'EU',	'Futures de taux':'F',	'Appels de marge, depots de garantie':'MRC',	'MUTUAL FUND INTERNATIONAL EQUITIES':'EU',	'Futures sur indices':'F',	'Option':'O',	'Coupons et dividendes':'DB',	'Options de change':'O',	'Action ordinaire':'ES',	'Swaps de taux':'',	'Options sur indices':'O',	'ETF EQUITIES':'EU',	'HEDGE FUNDS':'EU',	'MUTUAL FUND BALANCED':'EU',	'M M diversifié':'EU',	'MUTUAL FUND CEE EQUITIES':'EU',	'Options sur actions':'O',	'Obligation convertible':'DB',	'B.T.F / T. BILLS':'DB',	'Prises en pensions':'',	"Certiicat. d'investissement":'ES',	'Autres swaps':'',	'B.M.T.N / M.T.N / Notes':'DB',	"Bon de souscription d'action, warrant sur action":'O',	"Droit / bon d'echange":'O',	'Futures de change':'F',	'Action preferentielle':'ES',	'Mises en pension':'',	'Options de taux':'O',	'Futures sur actions':'F',	'FCPI':'EU',	'ETF & OTHERS':'EU',	'Obligation Adossée':'DB',	'Action a bon de souscription':'ES',	'ETF BONDS':'DB',	'Option sur devise':'O',	'Futur sur devise':'F',	"Droit d'attribution":'ES'
              }
        
        funds = {
                    '050726I_C': 'FR0010829697',	'052286_C': 'FR0011088657',	'916_C': 'FR0007038138',	'050648_C': 'FR0007032990',	'050016_C': 'FR0010319996',	'050330_C': 'FR0010188383',	'555_C': 'FR0007435920',	'E142_C': 'LU1121646423',	'PF60013_C': 'LU1121647827',	'PF60035_C': 'LU1121647157',	'PF61066_C': 'LU1431872925',	'S409326IE_C': 'LU1622150198',	'PF71070_C': 'LU1882436733',	'42290C_C': 'LU0119085271',	'S409322AE_C': 'LU1386074295',	'S409290OU_C': 'LU0945149838',	'S409271IE_C': 'LU0568620560',	'S409272IU_C': 'LU0568621618',	'PF73075_C': 'LU1882445569',	'57514C_C': 'LU0347595026',	'S409245IU_C': 'LU0568608276',	'S409246AU_C': 'LU0568611817',	'57509I_C': 'LU0347594136',	'PF71246_C': 'LU1882447425',	'S409312IE_C': 'LU1161086159',	'56041I_C': 'LU0319685342',	'57497I_C': 'LU0347592197',	'55015C_C': 'LU0297165101',	'S409293IU_C': 'LU0945153863',	'S409253IJ_C': 'LU0568583008',	'S409257IU_C': 'LU0568613946',	'S409214IE_C': 'LU0616241476',	'PF71258_C': 'LU1894677027',	'37785C_C': 'LU0119099819',	'S409291IE_C': 'LU0945151578',	'73205C_C': 'LU0518421895',	'92683C_C': 'LU0201576401',	'PF71068_C': 'LU1882475392',	'PF72347_C': 'LU1882475988',	'PF72339_C': 'LU1883303635',	'S409328IE_C': 'LU1691800830',	'S409319IE_C': 'LU1328850448',	'S409237IE_C': 'LU0568607385',	'S409226IE_C': 'LU0568615057',	'S409275IE_C': 'LU0755949681',	'S409329IE_C': 'LU1691801218',	'S409324IE_C': 'LU1579337525',	'S409327IE_C': 'LU1691800160',	'PF72364_C': 'LU1883306497',	'PF72349_C': 'LU1883311224',	'PF72354_C': 'LU1883316298',	'56055I_C': 'LU0319688015',	'6705C_C': 'LU0119133931',	'39974C_C': 'LU0119108826',	'56059I4_C': 'LU0319688791',	'PF72356_C': 'LU1883318740',	'S409330IU_C': 'LU1691802026',	'PF72358_C': 'LU1883320993',	'67251I_C': 'LU0442405998',	'S409296IE_C': 'LU0996172093',	'93755C_C': 'LU0210817283',	'S409200IE_C': 'LU0568619638',	'PF71204_C': 'LU1883329432',	'PF71198_C': 'LU1883330521',	'S409285IE_C': 'LU1049757476',	'PF71798_C': 'LU1883334275',	'97185C_C': 'LU0248702192',	'92679C_C': 'LU0201575346',	'S409315AE_C': 'LU1253540170',	'PF77764IE_C': 'LU1941681956',	'PF73059_C': 'LU1883335165',	'PF73073_C': 'LU1883340678',	'PF73065_C': 'LU1883342377',	'PF73069_C': 'LU1894680757',	'PF73071_C': 'LU1883841022',	'PF72362_C': 'LU1883848977',	'PF72066_C': 'LU1883854199',	'PF72063_C': 'LU1883856723',	'PF72061_C': 'LU1883859230',	'PF72056_C': 'LU1894682704',	'PF72052_C': 'LU1882441816',	'S409323_C': 'LU1433245245',	'S409325AU_C': 'LU1579338093',	'PF72050_C': 'LU1883866441',	'PF72025_C': 'LU1883867761',	'95579C_C': 'LU0236502588',	'S409321IEYD_C': 'LU1386074709',	'PF71066_C': 'LU1883868819',	'S409320IE_C': 'LU1328848970',	'PF70704_C': 'LU1883872332',	'S409259IU_C': 'LU0568602667',	'050914_C': 'FR0010194902',	'23399POOL_C': 'LU0290108116',	'86532_C': 'LU0832972474',	'60887_C': 'LU0399639060',	'60770_C': 'LU0399639573',	'60883_C': 'LU0399640407',	'050036_C': 'FR0010032573',	'PF61295_C': 'LU1681511959',	'7386EE_C': 'LU1499628912',	'E156_C': 'LU1272945004',	'E183_C': 'LU1467375017',	'7217EE_C': 'LU1322782373',	'EEC_C': 'LU0271695388',	'PF59271_C': 'LU0271690744',	'PF60137_C': 'LU0271691981',	'PF60042_C': 'LU0271691478',	'PF59506_C': 'LU0551376923',	'PF59669_C': 'LU0271693920',	'PF59372_C': 'LU0380935170',	'PF76730A_C': 'LU1920533400',	'S452048_C': 'LU1744900314',	'91555_C': 'LU1739251079',	'051_C': 'FR0007061379',	'811_C': 'FR0007493549',	'PF60235_C': 'LU0393241723',	'PF60157_C': 'LU0330699363',	'74309_C': 'LU0562498773',	'79510_C': 'LU0619623449',	'S409001AU_C': 'LU1095740236',	'S409000A_C': 'LU0068578508',	'E225_C': 'LU1650523076',	'PF59707_C': 'LU1599403737',	'PF59719_C': 'LU1599402929',	'PF59717_C': 'LU1599403067',	'PF59808_C': 'LU1599403224',	'136EEH_C': 'LU1437672725',	'E208_C': 'LU1437673293',	'E207_C': 'LU1437673020',	'S300158_C': 'FR0000448870',	'21656_C': 'LU1534919763',	'10628POOL_C': 'LU1350002991',	'050379_C': 'FR0011307107',	'050980_C': 'FR0011220342',	'PF71084_C': 'LU1882439240'

            }
        
        country = ["AD",	"AE",	"AF",	"AG",	"AI",	"AL",	"AM",	"AN",	"AO",	"AQ",	"AR",	"AS",	"AT",	"AU",	"AW",	"AZ",	"BA",	"BB",	"BD",	"BE",	"BF",	"BG",	"BH",	"BI",	"BJ",	"BM",	"BN",	"BO",	"BR",	"BS",	"BT",	"BU",	"BV",	"BW",	"BY",	"BZ",	"CA",	"CC",	"CF",	"CG",	"CH",	"CI",	"CK",	"CL",	"CM",	"CN",	"CO",	"CR",	"CS",	"CU",	"CV",	"CX",	"CY",	"CZ",	"DD",	"DE",	"DJ",	"DK",	"DM",	"DO",	"DZ",	"EC",	"EE",	"EG",	"EH",	"ER",	"ES",	"ET",	"EU",	"FI",	"FJ",	"FK",	"FM",	"FO",	"FR",	"FX",	"GA",	"GB",	"GD",	"GE",	"GF",	"GG",	"GH",	"GI",	"GL",	"GM",	"GN",	"GP",	"GQ",	"GR",	"GS",	"GT",	"GU",	"GW",	"GY",	"HK",	"HM",	"HN",	"HR",	"HT",	"HU",	"ID",	"IE",	"IL",	"IN",	"IO",	"IQ",	"IR",	"IS",	"IT",	"JM",	"JO",	"JP",	"KE",	"KG",	"KH",	"KI",	"KM",	"KN",	"KP",	"KR",	"KW",	"KY",	"KZ",	"LA",	"LB",	"LC",	"LI",	"LK",	"LR",	"LS",	"LT",	"LU",	"LV",	"LY",	"MA",	"MC",	"MD",	"MG",	"MH",	"MK",	"ML",	"MM",	"MN",	"MO",	"MP",	"MQ",	"MR",	"MS",	"MT",	"MU",	"MV",	"MW",	"MX",	"MY",	"MZ",	"NA",	"NC",	"NE",	"NF",	"NG",	"NI",	"NL",	"NO",	"NP",	"NR",	"NT",	"NU",	"NZ",	"OM",	"PA",	"PE",	"PF",	"PG",	"PH",	"PK",	"PL",	"PM",	"PN",	"PR",	"PT",	"PW",	"PY",	"QA",	"RE",	"RO",	"RU",	"RW",	"SA",	"SB",	"SC",	"SD",	"SE",	"SG",	"SH",	"SI",	"SJ",	"SK",	"SL",	"SM",	"SN",	"SO",	"SR",	"ST",	"SU",	"SV",	"SY",	"SZ",	"TC",	"TD",	"TF",	"TG",	"TH",	"TJ",	"TK",	"TM",	"TN",	"TO",	"TP",	"TR",	"TT",	"TV",	"TW",	"TZ",	"UA",	"UG",	"UM",	"US",	"UY",	"UZ",	"VA",	"VC",	"VE",	"VG",	"VI",	"VN",	"VU",	"WF",	"WS",	"YD",	"YE",	"YT",	"YU",	"ZA",	"ZM",	"ZR",	"ZW",	"JE",	"LE",	"IM",	"CD",	"RS",	"ME",	"CW"]
        currency = ["ADP",	"AED",	"AFA",	"ALL",	"AMD",	"ANG",	"AON",	"AOR",	"ARS",	"ATS",	"AUD",	"AWG",	"AZM",	"BAD",	"BBD",	"BDT",	"BEF",	"BGN",	"BHD",	"BIF",	"BMD",	"BND",	"BOB",	"BOV",	"BRL",	"BSD",	"BTN",	"BWP",	"BYB",	"BZD",	"CAD",	"CHF",	"CLF",	"CLP",	"CNY",	"COP",	"CRC",	"CUP",	"CVE",	"CYP",	"CZK",	"DEM",	"DJF",	"DKK",	"DOP",	"DZD",	"ECS",	"ECV",	"EEK",	"EGP",	"ESP",	"ETB",	"EUR",	"FIM",	"FJD",	"FKP",	"FRF",	"GBP",	"GEL",	"GHC",	"GIP",	"GMD",	"GNF",	"GRD",	"GTQ",	"GWP",	"GYD",	"HKD",	"HNL",	"HRK",	"HTG",	"HUF",	"IDR",	"IEP",	"ILS",	"INR",	"IQD",	"IRR",	"ISK",	"ITL",	"JMD",	"JOD",	"JPY",	"KES",	"KGS",	"KHR",	"KMF",	"KPW",	"KRW",	"KWD",	"KYD",	"KZT",	"LAK",	"LBP",	"LKR",	"LRD",	"LSL",	"LTL",	"LUF",	"LVL",	"LYD",	"MAD",	"MDL",	"MGF",	"MKD",	"MMK",	"MNT",	"MOP",	"MRO",	"MTL",	"MUR",	"MVR",	"MWK",	"MXN",	"MYR",	"MZM",	"NAD",	"NGN",	"NIO",	"NLG",	"NOK",	"NPR",	"NZD",	"OMR",	"PAB",	"PEN",	"PGK",	"PHP",	"PKR",	"PLN",	"PLZ",	"PTE",	"PYG",	"QAR",	"ROL",	"RUR",	"RWF",	"SAR",	"SBD",	"SCR",	"SDD",	"SEK",	"SGD",	"SHP",	"SIT",	"SKK",	"SLL",	"SOS",	"SRG",	"STD",	"SVC",	"SYP",	"SZL",	"THB",	"TJR",	"TMM",	"TND",	"TOP",	"TPE",	"TRL",	"TTD",	"TWD",	"TZS",	"UAG",	"UAK",	"UGX",	"USD",	"UYU",	"UZS",	"VEB",	"VND",	"VUV",	"WST",	"XAF",	"XCD",	"XEU",	"XOF",	"XPF",	"YER",	"YUM",	"ZAL",	"ZAR",	"ZMK",	"ZRN",	"ZWD",	"RUB",	"TRY",	"UAH",	"RON",	"CSD",	"RSD",	"GHS",	"AZN",	"MXV",	"BYR",	"ZMW",	"CNH"]
        
        df['TNACash']=''
        df['SecuritySEDOL']=''
        df['FundISIN']=''
        df['Percent']=''
        df['PublicationDate']=''
        #df['SecurityCFI']=''
        
        columns={
            'Date':'PortfolioDate',
            'ID Fond':'FundOWN',
            'ISIN HISTO': 'SecurityISIN',
            'Libéllé valeur':'SecurityName',
            'Pos': 'NumberOfShares',
            'Val. boursiere': 'MarketValue',
            'Cpn couru': 'SecurityCoupon',
            'Echeance': 'SecurityMaturity',
            'Valo': 'FundCurrency',
            'Valeur': 'SecurityCurrency',
            'Pays': 'SecurityCountry',
            'Type Instrument': 'SecurityCFI'        
            }
        df.columns = df.columns.to_series().replace(columns)

        column_list = [
                'PortfolioDate',
                'SecurityCFI',
                'SecuritySEDOL',
                'SecurityISIN',
                'SecurityName',
                'FundISIN',
                'SecurityCurrency',
                'SecurityCountry',
                'Percent',
                'FundCurrency',
                'SecuritySEDOL'
                ]
        column_list_2 = []   
        for column in column_list:
            if column not in df.columns:
                column_list_2.append(column) 
        
        x1 = len(column_list_2) == 0
        

            
        if x1 == True:
            
            
            df['FundISIN'] = df['FundISIN'].str.strip()
            df['SecurityISIN'] = df['SecurityISIN'].str.strip()
            df['SecuritySEDOL'] = df['SecuritySEDOL'].astype(str).str.strip()
            
            
            df['FundISIN'] = df['FundOWN'].map(funds).fillna('')            
            df['CFICode'] = df['SecurityCFI'].map(di).fillna('')
            
            df = df.replace(np.nan, '', regex=True) 
                    
            df['SecurityCountry'] = df['SecurityCountry'].apply(lambda i: i if i in country else '')
            df['SecurityCurrency'] = df['SecurityCurrency'].apply(lambda i: i if i in currency else '')
            df['FundCurrency'] = df['FundCurrency'].apply(lambda i: i if i in currency else '')
                    
            #df['Percent'] = round(df['Percent'].astype(float), 5)
            df['MarketValue'] = round(df['MarketValue'], 2)
               
            df['SecurityISIN'] = df['SecurityISIN'].apply(lambda x: x if len(str(x)) == 12 else '')
            df['SecuritySEDOL'] = df['SecuritySEDOL'].apply(lambda x: x if len(str(x)) == 7 else '')
            df['FundCurrency'] = df['FundCurrency'].apply(lambda x: x if len(str(x)) == 3 else '')
            df['SecurityCurrency'] = df['SecurityCurrency'].apply(lambda x: x if len(str(x)) == 3 else '')
            df['SecurityCountry'] = df['SecurityCountry'].apply(lambda x: x if len(str(x)) == 2 else '')
                    
            df["CurrentDate"] = datetime.today().strftime('%Y-%m-%dT%H:%M:%S+01:00')
            
            df['Egen']=df.apply(lambda x: (x.SecurityName+x.SecurityCurrency+x.SecurityCountry).replace(' ', '') if len(str(x.SecurityISIN)) == 0 else '', axis=1)
            df['Egen']=df['Egen'].apply(lambda x: x.replace('%','/'))
            
            df['SecurityName']=df['SecurityName'].apply(lambda x: x.replace('%','% '))
            df['SecurityName']=df['SecurityName'].apply(lambda x: x.replace('  ',' '))

            x2 = all(df['FundISIN'].str.len() == 12)
            x3 = all(df['FundISIN'].str[0:2].str.isalpha()==True)
            x4 = all(df['FundCurrency'].str.len() == 3)
            x5 = all(df['SecurityName'].str.len() < 81)
            x6 = all(df['PortfolioDate'].astype(str).str.len() != 0)
            
            variables = [x2, x3, x4, x5, x6]
            errors = [
                      '- Wrong ISIN length or missing ISIN', 
                      '- ISIN doesn'+"'"+'t start with two letters', 
                      '- Missing fund currency', 
                      '- Security ISIN > 80 characters', 
                      '- No portfolio date'
                      ]
                
            falses = []
            for i,j in enumerate(variables):
                if j == False:
                    falses.append(errors[i])
                    
            fileName = keyResponse + os.path.basename(key) + " (Transformation)" + ".csv"
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, sep=';', header=True, index = False, float_format='%f')
            s3_resource = boto3.resource('s3')
            s3_resource.Object(bucketResponse, fileName).put(Body=csv_buffer.getvalue())   
             
            if all(x == True for x in variables):            
                            
                            fileResponse = {'bucket': bucketResponse, 'key': fileName}
                            fileResult = {
                                    'message': message,
                                    'operationStatus': 'Success',
                                    'file': fileResponse,
                                    'fileRequestName': os.path.basename(key)
                                    }
                            result.append(fileResult)
            else:
                
                fileResponse = {'bucket': bucketResponse, 'key': fileName}
                fileResult = {
                        'message': '\n' + '\n'.join(str(p) for p in falses) + '\n',
                        'operationStatus': '\n'+'Failed'+'\n',
                        'file': fileResponse                 
                        }
                result.append(fileResult)

        else:  
          
            fileResponse = {'bucket': bucketResponse, 'key': fileName}
            fileResult = {
                    'message': '\n' + '- Missing columns: ' + str(column_list_2) + '\n',
                    'operationStatus': '\n'+'Failed'+'\n',
                    'file': fileResponse                
                    }
            result.append(fileResult)

        return result

