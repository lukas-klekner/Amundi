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
        df = pd.read_csv(readExcelData, sep=';')

        di = {
                     'Fixed Income':'DB',
                     'Cash':'MRC',
                     'Other (OTC)':'MRC',
                     'Deposit': 'MRC', 
                     'Fund': 'EU', 
                     'Equity': 'ES',
                     'Forwards':'F'
              }
        country = ["AD",	"AE",	"AF",	"AG",	"AI",	"AL",	"AM",	"AN",	"AO",	"AQ",	"AR",	"AS",	"AT",	"AU",	"AW",	"AZ",	"BA",	"BB",	"BD",	"BE",	"BF",	"BG",	"BH",	"BI",	"BJ",	"BM",	"BN",	"BO",	"BR",	"BS",	"BT",	"BU",	"BV",	"BW",	"BY",	"BZ",	"CA",	"CC",	"CF",	"CG",	"CH",	"CI",	"CK",	"CL",	"CM",	"CN",	"CO",	"CR",	"CS",	"CU",	"CV",	"CX",	"CY",	"CZ",	"DD",	"DE",	"DJ",	"DK",	"DM",	"DO",	"DZ",	"EC",	"EE",	"EG",	"EH",	"ER",	"ES",	"ET",	"EU",	"FI",	"FJ",	"FK",	"FM",	"FO",	"FR",	"FX",	"GA",	"GB",	"GD",	"GE",	"GF",	"GG",	"GH",	"GI",	"GL",	"GM",	"GN",	"GP",	"GQ",	"GR",	"GS",	"GT",	"GU",	"GW",	"GY",	"HK",	"HM",	"HN",	"HR",	"HT",	"HU",	"ID",	"IE",	"IL",	"IN",	"IO",	"IQ",	"IR",	"IS",	"IT",	"JM",	"JO",	"JP",	"KE",	"KG",	"KH",	"KI",	"KM",	"KN",	"KP",	"KR",	"KW",	"KY",	"KZ",	"LA",	"LB",	"LC",	"LI",	"LK",	"LR",	"LS",	"LT",	"LU",	"LV",	"LY",	"MA",	"MC",	"MD",	"MG",	"MH",	"MK",	"ML",	"MM",	"MN",	"MO",	"MP",	"MQ",	"MR",	"MS",	"MT",	"MU",	"MV",	"MW",	"MX",	"MY",	"MZ",	"NA",	"NC",	"NE",	"NF",	"NG",	"NI",	"NL",	"NO",	"NP",	"NR",	"NT",	"NU",	"NZ",	"OM",	"PA",	"PE",	"PF",	"PG",	"PH",	"PK",	"PL",	"PM",	"PN",	"PR",	"PT",	"PW",	"PY",	"QA",	"RE",	"RO",	"RU",	"RW",	"SA",	"SB",	"SC",	"SD",	"SE",	"SG",	"SH",	"SI",	"SJ",	"SK",	"SL",	"SM",	"SN",	"SO",	"SR",	"ST",	"SU",	"SV",	"SY",	"SZ",	"TC",	"TD",	"TF",	"TG",	"TH",	"TJ",	"TK",	"TM",	"TN",	"TO",	"TP",	"TR",	"TT",	"TV",	"TW",	"TZ",	"UA",	"UG",	"UM",	"US",	"UY",	"UZ",	"VA",	"VC",	"VE",	"VG",	"VI",	"VN",	"VU",	"WF",	"WS",	"YD",	"YE",	"YT",	"YU",	"ZA",	"ZM",	"ZR",	"ZW",	"JE",	"LE",	"IM",	"CD",	"RS",	"ME",	"CW"]
        currency = ["ADP",	"AED",	"AFA",	"ALL",	"AMD",	"ANG",	"AON",	"AOR",	"ARS",	"ATS",	"AUD",	"AWG",	"AZM",	"BAD",	"BBD",	"BDT",	"BEF",	"BGN",	"BHD",	"BIF",	"BMD",	"BND",	"BOB",	"BOV",	"BRL",	"BSD",	"BTN",	"BWP",	"BYB",	"BZD",	"CAD",	"CHF",	"CLF",	"CLP",	"CNY",	"COP",	"CRC",	"CUP",	"CVE",	"CYP",	"CZK",	"DEM",	"DJF",	"DKK",	"DOP",	"DZD",	"ECS",	"ECV",	"EEK",	"EGP",	"ESP",	"ETB",	"EUR",	"FIM",	"FJD",	"FKP",	"FRF",	"GBP",	"GEL",	"GHC",	"GIP",	"GMD",	"GNF",	"GRD",	"GTQ",	"GWP",	"GYD",	"HKD",	"HNL",	"HRK",	"HTG",	"HUF",	"IDR",	"IEP",	"ILS",	"INR",	"IQD",	"IRR",	"ISK",	"ITL",	"JMD",	"JOD",	"JPY",	"KES",	"KGS",	"KHR",	"KMF",	"KPW",	"KRW",	"KWD",	"KYD",	"KZT",	"LAK",	"LBP",	"LKR",	"LRD",	"LSL",	"LTL",	"LUF",	"LVL",	"LYD",	"MAD",	"MDL",	"MGF",	"MKD",	"MMK",	"MNT",	"MOP",	"MRO",	"MTL",	"MUR",	"MVR",	"MWK",	"MXN",	"MYR",	"MZM",	"NAD",	"NGN",	"NIO",	"NLG",	"NOK",	"NPR",	"NZD",	"OMR",	"PAB",	"PEN",	"PGK",	"PHP",	"PKR",	"PLN",	"PLZ",	"PTE",	"PYG",	"QAR",	"ROL",	"RUR",	"RWF",	"SAR",	"SBD",	"SCR",	"SDD",	"SEK",	"SGD",	"SHP",	"SIT",	"SKK",	"SLL",	"SOS",	"SRG",	"STD",	"SVC",	"SYP",	"SZL",	"THB",	"TJR",	"TMM",	"TND",	"TOP",	"TPE",	"TRL",	"TTD",	"TWD",	"TZS",	"UAG",	"UAK",	"UGX",	"USD",	"UYU",	"UZS",	"VEB",	"VND",	"VUV",	"WST",	"XAF",	"XCD",	"XEU",	"XOF",	"XPF",	"YER",	"YUM",	"ZAL",	"ZAR",	"ZMK",	"ZRN",	"ZWD",	"RUB",	"TRY",	"UAH",	"RON",	"CSD",	"RSD",	"GHS",	"AZN",	"MXV",	"BYR",	"ZMW",	"CNH"]
        
        df['TNACash']=''
        df['SecurityCoupon']=''
        df['SecurityMaturity']=''
        df['MarketValue']=''
        df['NumberOfShares']=''
        df['PublicationDate']=''
        
        columns={
            'PositionDate':'PortfolioDate',
            'AssetType':'SecurityCFI',
            'SEDOL':'SecuritySEDOL',
            'ISIN':'SecurityISIN',
            'AssetName':'SecurityName',
            'PortfolioCode': 'FundISIN',
            'AssetCurrency':'SecurityCurrency',
            'AssetCountry':'SecurityCountry',
            'PositionWeight':'Percent',
            'FundCurrency':'FundCurrency',
            'SEDOL': 'SecuritySEDOL'
            
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
                        
            df['CFICode'] = df['SecurityCFI'].map(di).fillna('')
            
            df = df.replace(np.nan, '', regex=True) 
                    
            df['SecurityCountry'] = df['SecurityCountry'].apply(lambda i: i if i in country else '')
            df['SecurityCurrency'] = df['SecurityCurrency'].apply(lambda i: i if i in currency else '')
            df['FundCurrency'] = df['FundCurrency'].apply(lambda i: i if i in currency else '')
                    
            df['Percent'] = round(df['Percent'].astype(float), 5)
            #df['Market Value'] = round(df['Market Value'], 2)
               
            df['SecurityISIN'] = df['SecurityISIN'].apply(lambda x: x if len(str(x)) == 12 else '')
            df['SecuritySEDOL'] = df['SecuritySEDOL'].apply(lambda x: x if len(str(x)) == 7 else '')
            df['FundCurrency'] = df['FundCurrency'].apply(lambda x: x if len(str(x)) == 3 else '')
            df['SecurityCurrency'] = df['SecurityCurrency'].apply(lambda x: x if len(str(x)) == 3 else '')
            df['SecurityCountry'] = df['SecurityCountry'].apply(lambda x: x if len(str(x)) == 2 else '')
                    
            df["CurrentDate"] = datetime.today().strftime('%Y-%m-%dT%H:%M:%S+01:00')
            
            df['Egen']=df.apply(lambda x: (x.SecurityName+x.SecurityCurrency+x.SecurityCountry).replace(' ', '') if len(str(x.SecurityISIN)) == 0 else '', axis=1)


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

