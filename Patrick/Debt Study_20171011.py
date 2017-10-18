# -*- coding: utf-8 -*-
"""
Created on Thu Sep 28 13:24:11 2017

@author: konicpa
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Sep 22 11:22:15 2017

@author: konicpa
"""
#%%
import pypyodbc
import pandas as pd
import pandas.io.sql
#import datetime as dt
#from pandas.tseries.offsets import BDay
#import time
#from datetime import datetime, timedelta
import numpy as np
from pandas.tseries.offsets import CustomBusinessDay
from pandas.tseries.holiday import USFederalHolidayCalendar
from scipy import stats

ciqids = pd.read_excel(r'\\evprodfsg01\QR_Workspace\Debt Issue Study\ciqids.xlsx', 'Sheet1', na_values=['NA'])
nim = pd.read_excel(r'\\evprodfsg01\QR_Workspace\Debt Issue Study\New Issue Monitor Test_Python.xlsx', 'Screening', na_values=['NA'])
lifecycle = pd.read_excel(r'\\evprodfsg01\QR_Workspace\Debt Issue Study\Debt_Equity_Study_MASTER.xlsx', 'Lifecycle', na_values=['NA'])
#spx = pd.read_excel(r'Q:\Debt Issue Study\Debt_Equity_Study_MASTER.xlsx', 'SPX', na_values=['NA'])
#%%
ccdb = pypyodbc.connect("DRIVER={SQL Server}; SERVER=devExt\Ext; DATABASE=CCDB; UID=konicpa; PWD=934Ql6d19t5GnoIt", autocommit=True)

Id_list = tuple(ciqids['id'].values)[0:50]

gvkeyQuerry = """
set NOCOUNT ON
select gvkey + iid as id from UNIV_SECURITY where 
companyId in 
""" + str(Id_list)

gvkey_DF = pd.read_sql_query(gvkeyQuerry, ccdb)
gvkey_list = tuple(gvkey_DF['id'].values)
#gvkey_lista = gvkey_list[0:999]
#gvkey_listb = gvkey_list[1000:1999]
#gvkey_listc = gvkey_list[2000:2999]

string1 = ', '.join('(\'{0}\')'.format(w) for w in gvkey_list)
#string1a = ', '.join('(\'{0}\')'.format(w) for w in gvkey_lista)
#string1b = ', '.join('(\'{0}\')'.format(w) for w in gvkey_listb)
#string1c = ', '.join('(\'{0}\')'.format(w) for w in gvkey_listc)
#%%
ciqdb = pypyodbc.connect("DRIVER={SQL Server}; SERVER=MNDEVDB01\DEVMTTM; DATABASE=CompustatData;UID=tranda;PWD=Ekb4Fpr3s27w79GN;", autocommit=True)

sql = """
set NOCOUNT ON

SELECT co.[companyId],[companyName],se.[securityId],[securityName],[securityStartDate],[securityEndDate],se.[securitySubTypeId],[securitySubTypeName],se.[primaryFlag],[tradingItemId],ti.[tickerSymbol],ti.[exchangeId],[currencyId],[tradingItemStatusId],ti.[primaryFlag],[companyStatusTypeId],[companyTypeId],co.[simpleIndustryId],[simpleIndustryDescription],[gvkey],[iid]
FROM [CompustatData].[dbo].[ciqCompany] co
left join [CompustatData].[dbo].[ciqSecurity] se on se.[companyId] = co.[companyId]
left join [CompustatData].[dbo].[ciqTradingItem] ti on ti.securityId = se.securityId
left join [CompustatData].[dbo].[ciqSimpleIndustry] si on si.[simpleIndustryId] = co.[simpleIndustryId]
left join [CompustatData].[dbo].[ciqSecuritySubType] st on st.[securitySubTypeId] = se.[securitySubTypeId]
left join [CompustatData].[dbo].[ciqGvkeyIID] gv on ti.[tradingItemId] = gv.[objectId]
where st.[securitySubTypeId] = 1
and se.[primaryFlag] = 1
and ti.[primaryFlag] = 1
and [securityEndDate] is null
and gv.gvkey is not null and gv.iid is not null
and co.[companyId] in""" + str(Id_list)

company = pandas.io.sql.read_sql(sql, ciqdb)
#%%                                
sql = """
set NOCOUNT ON

declare @dataItemId int
set @dataItemId = '4193' --1007 total assets
declare @evalDate datetime2
set @evalDate = dateadd(day,-1, cast(getdate() as date))
declare @gvtable table (gvstring varchar(12))
insert into @gvtable values """ + string1 + """


SELECT c.[companyId],gv.[gvkey],gv.[iid],s.[securityId],s.[primaryFlag]
,cast(fi.[periodEndDate] as date) as [periodEndDate]
,cast(fi.[filingDate] as date) as [filingDate],pt.[periodTypeName]
,fp.[calendarQuarter],fp.[calendarYear],fd.[dataItemId],di.[dataItemName],fd.[dataItemValue] 
FROM ciqCompany c 
JOIN [CompustatData].[dbo].[ciqSecurity] s on c.companyId = s.companyId 
JOIN [CompustatData].[dbo].[ciqTradingItem] ti on ti.securityId = s.securityId 
JOIN [CompustatData].[dbo].[ciqExchange] e on e.exchangeId = ti.exchangeId 
JOIN [CompustatData].[dbo].[ciqFinPeriod] fp on fp.companyId = c.companyId 
JOIN [CompustatData].[dbo].[ciqPeriodType] pt on pt.periodTypeId = fp.periodTypeId 
JOIN [CompustatData].[dbo].[ciqFinInstance] fi on fi.financialPeriodId = fp.financialPeriodId 
JOIN [CompustatData].[dbo].[ciqFinInstanceToCollection] ic on ic.financialInstanceId = fi.financialInstanceId 
JOIN [CompustatData].[dbo].[ciqFinCollection] fc on fc.financialCollectionId = ic.financialCollectionId 
JOIN [CompustatData].[dbo].[ciqFinCollectionData] fd on fd.financialCollectionId = ic.financialCollectionId 
JOIN [CompustatData].[dbo].[ciqDataItem] di on di.dataItemId = fd.dataItemId 
JOIN [CompustatData].[dbo].[ciqGvkeyIID] gv on ti.[tradingItemId] = gv.[objectId] 
WHERE fd.[dataItemId] = @dataItemId
--in (1002, 1001, 1008, 1004, 1007, 1009, 1276, 1006, 1275, 1013, 106) --= @dataItemId
AND fp.[periodTypeId] = 2		--quarterly 
AND fi.[restatementTypeId] = 2	--original
AND fi.[filingDate] between '1/1/05' and @evalDate 
AND gv.gvkey + gv.iid in (select gvstring from @gvtable) 
and s.primaryFlag = 1
--and se.primaryFlag = 1
ORDER BY fd.[dataItemId],gv.[gvkey],fi.[periodEndDate] DESC
"""

fund = pandas.io.sql.read_sql(sql, ciqdb)
#%%
qaidb = pypyodbc.connect("DRIVER={SQL Server}; SERVER=prodQAI\QAI; DATABASE=qai_view", autocommit=True)

sql = """
declare @evalDate datetime2
set @evalDate = dateadd(day,-1, cast(getdate() as date))

SELECT * FROM [qai_view].[dbo].[DS2IndexData]
Where DSIndexCode = 41620
and ValueDate between '12/20/2004' and @evalDate
"""
spx = pd.read_sql_query(sql, qaidb)
spx = spx[['valuedate','ri']]
spx = spx.set_index('valuedate')
spx = spx.pct_change()
spx = spx.reset_index()
spx = spx.rename(columns = {'ri':'Return'})
spx = spx.rename(columns = {'valuedate':'Date'})
#%%
sql = """
set NOCOUNT ON

declare @evalDate datetime2
set @evalDate = dateadd(day,-1, cast(getdate() as date))
declare @gvtable table (gvstring varchar(12))
insert into @gvtable values """ + string1 + """

SELECT dp.[gvkey],dp.[iid],dp.[datadate],((isnull(dt.[trfd],1)) * dp.[prccd]) /  isnull(ad.[splitvalue],1) AS prccd_adj ,((isnull(dt.[trfd],1)) * dp.[prcod]) /  isnull(ad.[splitvalue],1) AS prcod_adj , ((isnull(dt.[trfd],1)) * dp.[cshtrd]) /  isnull(ad.[splitvalue],1) AS cshtrd_adj
FROM [CompustatData].[dbo].[sec_dprc] dp
outer apply
(select top 1 [trfd] --, max([trfd]) as trfd_max
FROM [CompustatData].[dbo].[sec_dtrt]
where [gvkey] = dp.gvkey and [iid] = dp.[iid] and [datadate] <= dp.[datadate]
order by [datadate] desc
)  dt
outer apply
(select top 1 [splitvalue]
from [CompustatData].[dbo].[sec_adjfact]
where [gvkey] = dp.gvkey and [iid] = dp.[iid] and [effdate] <= dp.[datadate]
order by [effdate] desc
)  ad
left join 
(select [gvkey], [iid], max(trfd) as trfd_max
from [CompustatData].[dbo].[sec_dtrt]
--where [gvkey] = dp.gvkey and [iid] = dp.[iid]
group by [gvkey],[iid]
) tx
on tx.[gvkey] = dp.gvkey and tx.[iid] = dp.[iid]
where dp.gvkey + dp.iid in (select gvstring from @gvtable)
and dp.[datadate] between '12/20/2004' and @evalDate
order by dp.gvkey + dp.iid, datadate
"""

adjprices = pandas.io.sql.read_sql(sql, ciqdb)
#%%                     
sql = """
set NOCOUNT ON

declare @evalDate datetime2
set @evalDate = dateadd(day,-1, cast(getdate() as date))

SELECT [companyId],[pricingDate],[marketCap],[TEV],[sharesOutstanding]
FROM [CompustatData].[dbo].[ciqMarketCap]
where companyId  in """ + str(Id_list) + """
and [pricingDate] BETWEEN '1/1/2005' AND @evalDate
"""

mktcap = pandas.io.sql.read_sql(sql, ciqdb)
#%%
matrix = company[['gvkey','iid','companyid','companyname','simpleindustrydescription']].copy(deep=True)
issuer = pd.merge(matrix, nim, left_on=['companyid'], right_on = ['IssuerID'],how='outer')
issuerpar = pd.merge(matrix, nim, left_on=['companyid'], right_on = ['IssuerParentID'],how='outer')
ultpar = pd.merge(matrix, nim, left_on=['companyid'], right_on = ['UltimateParentID'],how='outer')
issuer = issuer.dropna(how='any')
issuer = issuer.drop_duplicates()
issuerpar = issuerpar.dropna(how='any')
issuerpar = issuerpar.drop_duplicates()
ultpar = ultpar.dropna(how='any')
ultpar = ultpar.drop_duplicates()
frames = [issuer, issuerpar, ultpar]
matrix2 = pd.concat(frames)
del matrix2['SecurityID']
matrix2 = matrix2.dropna(how='any')
matrix2 = matrix2.drop_duplicates()
matrix2['testyear'] = pd.DatetimeIndex(matrix2['OfferingDate']).year
matrix2['testmonth'] = pd.DatetimeIndex(matrix2['OfferingDate']).month
#%%
step = pd.merge(company, adjprices, left_on=['gvkey','iid'], right_on = ['gvkey','iid'],how='inner')
adjprices1 = step[['gvkey', 'iid', 'datadate', 'prccd_adj', 'prcod_adj','cshtrd_adj']]
adjprices2 = adjprices1.pivot_table(index='gvkey',columns='datadate',values='prccd_adj')
adjreturn = adjprices1.pivot_table(index='datadate',columns='gvkey',values='prccd_adj')
adjreturn2 = adjreturn.pct_change()
gvkeys = list(adjreturn2.columns)
matrix2 = matrix2[matrix2['gvkey'].isin(gvkeys)]
sample_return = matrix2.apply(lambda row: adjreturn2[row['gvkey']][row['OfferingDate']], axis=1)
volume = adjprices.pivot_table(index='datadate',columns='gvkey',values='cshtrd_adj')
volumechg = volume.pct_change()
sample_volumechg = matrix2.apply(lambda row: volumechg[row['gvkey']][row['OfferingDate']], axis=1)
matrix2 = pd.concat([matrix2, sample_return,sample_volumechg], axis=1, join_axes=[matrix2.index])

matrix2 = matrix2.rename(columns = {0:'tdreturn'})
matrix2 = matrix2.rename(columns = {1:'tdvolumechg'})
#%%

fundlookup = matrix2[['gvkey','iid','OfferingDate']]
#fundlookup = fundlookup.drop_duplicates()

#from pandas.io.parsers import StringIO
from pandasql import sqldf

def dbGetQuery(q):
    return sqldf(q, globals())

querry = """
select f.* , fu.filingdate, fu.dataitemname, fu.dataitemvalue
from fundlookup as f
left join fund as fu
on f.gvkey = fu.gvkey
and f.iid = fu.iid
and fu.filingdate = (
        select max(filingdate) from fund 
        where fund.gvkey = f.gvkey 
        and fund.iid = f.iid 
        and fund.filingdate <= f.OfferingDate) 
"""

df3 = dbGetQuery(querry)
df3['OfferingDate'] = pd.to_datetime(df3['OfferingDate'])
matrix2 = pd.merge(matrix2, df3, left_on=['gvkey','iid', 'OfferingDate'], right_on = ['gvkey','iid', 'OfferingDate'],how='inner')
matrix2 = matrix2.drop_duplicates()
matrix2 = matrix2.rename(columns = {'dataitemvalue':'Net Debt/EBITDA'})
del matrix2['dataitemname']
#%%                                
sql = """
set NOCOUNT ON

declare @dataItemId int
set @dataItemId = '4191' --1007 total assets
declare @evalDate datetime2
set @evalDate = dateadd(day,-1, cast(getdate() as date))
declare @gvtable table (gvstring varchar(12))
insert into @gvtable values """ + string1 + """


SELECT c.[companyId],gv.[gvkey],gv.[iid],s.[securityId],s.[primaryFlag]
,cast(fi.[periodEndDate] as date) as [periodEndDate]
,cast(fi.[filingDate] as date) as [filingDate],pt.[periodTypeName]
,fp.[calendarQuarter],fp.[calendarYear],fd.[dataItemId],di.[dataItemName],fd.[dataItemValue] 
FROM ciqCompany c 
JOIN [CompustatData].[dbo].[ciqSecurity] s on c.companyId = s.companyId 
JOIN [CompustatData].[dbo].[ciqTradingItem] ti on ti.securityId = s.securityId 
JOIN [CompustatData].[dbo].[ciqExchange] e on e.exchangeId = ti.exchangeId 
JOIN [CompustatData].[dbo].[ciqFinPeriod] fp on fp.companyId = c.companyId 
JOIN [CompustatData].[dbo].[ciqPeriodType] pt on pt.periodTypeId = fp.periodTypeId 
JOIN [CompustatData].[dbo].[ciqFinInstance] fi on fi.financialPeriodId = fp.financialPeriodId 
JOIN [CompustatData].[dbo].[ciqFinInstanceToCollection] ic on ic.financialInstanceId = fi.financialInstanceId 
JOIN [CompustatData].[dbo].[ciqFinCollection] fc on fc.financialCollectionId = ic.financialCollectionId 
JOIN [CompustatData].[dbo].[ciqFinCollectionData] fd on fd.financialCollectionId = ic.financialCollectionId 
JOIN [CompustatData].[dbo].[ciqDataItem] di on di.dataItemId = fd.dataItemId 
JOIN [CompustatData].[dbo].[ciqGvkeyIID] gv on ti.[tradingItemId] = gv.[objectId] 
WHERE fd.[dataItemId] = @dataItemId
--in (1002, 1001, 1008, 1004, 1007, 1009, 1276, 1006, 1275, 1013, 106) --= @dataItemId
AND fp.[periodTypeId] = 2		--quarterly 
AND fi.[restatementTypeId] = 2	--original
AND fi.[filingDate] between '1/1/05' and @evalDate 
AND gv.gvkey + gv.iid in (select gvstring from @gvtable) 
and s.primaryFlag = 1
--and se.primaryFlag = 1
ORDER BY fd.[dataItemId],gv.[gvkey],fi.[periodEndDate] DESC
"""

fund_4191 = pandas.io.sql.read_sql(sql, ciqdb)

query = """
select f.* , fu.filingdate, fu.dataitemname, fu.dataitemvalue
from fundlookup as f
left join fund_4191 as fu
on f.gvkey = fu.gvkey
and f.iid = fu.iid
and fu.filingdate = (
        select max(filingdate) from fund 
        where fund.gvkey = f.gvkey 
        and fund.iid = f.iid 
        and fund.filingdate <= f.OfferingDate) 
"""

df4 = dbGetQuery(query)

#%% 
# FSTDB
fdidb = pypyodbc.connect("DRIVER={SQL Server}; SERVER=PRODCREDIT\CREDIT; DATABASE=FSTDB", autocommit=True)

sql = """
select AttributeName, fd.EntityID, DataTypeID, fd.NumericValue, fd.StringValue, CalendarYear, CalendarQuarter, FilingDate, ed.StringValue as CompanyID
from FinancialData fd 
inner join FinancialPeriod fp on fp.FinancialPeriodID = fd.FinancialPeriodID 
inner join Attribute a on a.AttributeID = fd.AttributeID
left join EntityData ed on ed.EntityID = fd.EntityID
where a.AttributeID in (293,288,311,406)
and ed.AttributeID = 4
"""

Attdf_tmp = pandas.io.sql.read_sql(sql, fdidb) 
#%%
from pandasql import sqldf

def dbGetQuery(q):
    return sqldf(q, globals())

query = """
select * from Attdf_tmp where entityid = 48

"""

df_tmp = dbGetQuery(query)

#%%
query = """
select df1.entityid, df1.companyid, df1.numericvalue as reportingdate, df1.filingdate,
df2.numericvalue as cash,
df3.numericvalue as debt,
df4.numericvalue as ebitda,
from df_tmp df1
left join df_tmp df2 on df2.filingdate = df1.filingdate and df2.calendarquarter = df1.calendarquarter
left join df_tmp df3 on df3.filingdate = df1.filingdate and df3.calendarquarter = df1.calendarquarter
left join df_tmp df4 on df4.filingdate = df1.filingdate and df4.calendarquarter = df1.calendarquarter
where df1.attributename = 'Reporting_Date'
and df2.attributename = 'Cash'
and df3.attributename = 'Total_Debt'
and df4.attributename = 'EBITDA'
"""
attdf = dbGetQuery(query)

#attdf['reportingdate'] = pd.to_datetime(attdf['reportingdate'],format="%Y%m%d")
#%%
query = """
select a1.entityid,
a1.companyid,
--CAST(a1.numericvalue as char(8)) as reportingdate
CAST(CAST(a1.numericvalue as char(8)) as datetime) as reportingdate,
(a3.numericvalue - a2.numericvalue)/a4.numericvalue as cal, 
a1.filingdate

from Attdf_tmp a1
join attdf_tmp a2 on a1.entityid = a2.entityid and a1.filingdate = a2.filingdate
join attdf_tmp a3 on a1.entityid = a3.entityid and a1.filingdate = a3.filingdate
join attdf_tmp a4 on a1.entityid = a4.entityid and a1.filingdate = a4.filingdate

where a1.attributename = 'Reporting_Date' 
and a2.attributename = 'Cash'
and a3.attributename = 'Total_Debt'
and a4.attributename = 'EBITDA'
--and a1.entityid = 1
"""

df5 = dbGetQuery(query)
df5['reportingdate'] = pd.to_datetime(df5['reportingdate'],format="%Y%m%d")

#%%
matrix2 = pd.merge(matrix2, mktcap, left_on=['companyid','OfferingDate'], right_on = ['companyid','pricingdate'])
del matrix2['pricingdate']
#%%
bday_us = CustomBusinessDay(calendar=USFederalHolidayCalendar())
matrix2['OfferingDate+1'] = matrix2['OfferingDate']+bday_us
sample_return = matrix2.apply(lambda row: adjreturn2[row['gvkey']][row['OfferingDate+1']], axis=1)
sample_volumechg = matrix2.apply(lambda row: volumechg[row['gvkey']][row['OfferingDate+1']], axis=1)
matrix2 = pd.concat([matrix2, sample_return,sample_volumechg], axis=1, join_axes=[matrix2.index])
matrix2 = matrix2.rename(columns = {0:'td+1return'})
matrix2 = matrix2.rename(columns = {1:'td+1volumechg'})

matrix2['OfferingDate+2'] = matrix2['OfferingDate+1']+bday_us
sample_return = matrix2.apply(lambda row: adjreturn2[row['gvkey']][row['OfferingDate+2']], axis=1)
sample_volumechg = matrix2.apply(lambda row: volumechg[row['gvkey']][row['OfferingDate+2']], axis=1)
matrix2 = pd.concat([matrix2, sample_return,sample_volumechg], axis=1, join_axes=[matrix2.index])
matrix2 = matrix2.rename(columns = {0:'td+2return'})
matrix2 = matrix2.rename(columns = {1:'td+2volumechg'})

matrix2['OfferingDate+3'] = matrix2['OfferingDate+2']+bday_us
sample_return = matrix2.apply(lambda row: adjreturn2[row['gvkey']][row['OfferingDate+3']], axis=1)
sample_volumechg = matrix2.apply(lambda row: volumechg[row['gvkey']][row['OfferingDate+3']], axis=1)
matrix2 = pd.concat([matrix2, sample_return,sample_volumechg], axis=1, join_axes=[matrix2.index])
matrix2 = matrix2.rename(columns = {0:'td+3return'})
matrix2 = matrix2.rename(columns = {1:'td+3volumechg'})

matrix2['OfferingDate+4'] = matrix2['OfferingDate+3']+bday_us
sample_return = matrix2.apply(lambda row: adjreturn2[row['gvkey']][row['OfferingDate+4']], axis=1)
sample_volumechg = matrix2.apply(lambda row: volumechg[row['gvkey']][row['OfferingDate+4']], axis=1)
matrix2 = pd.concat([matrix2, sample_return,sample_volumechg], axis=1, join_axes=[matrix2.index])
matrix2 = matrix2.rename(columns = {0:'td+4return'})
matrix2 = matrix2.rename(columns = {1:'td+4volumechg'})

matrix2['OfferingDate+5'] = matrix2['OfferingDate+4']+bday_us
sample_return = matrix2.apply(lambda row: adjreturn2[row['gvkey']][row['OfferingDate+5']], axis=1)
sample_volumechg = matrix2.apply(lambda row: volumechg[row['gvkey']][row['OfferingDate+5']], axis=1)
matrix2 = pd.concat([matrix2, sample_return,sample_volumechg], axis=1, join_axes=[matrix2.index])
matrix2 = matrix2.rename(columns = {0:'td+5return'})
matrix2 = matrix2.rename(columns = {1:'td+5volumechg'})

matrix2['OfferingDate-1'] = matrix2['OfferingDate']-bday_us
sample_return = matrix2.apply(lambda row: adjreturn2[row['gvkey']][row['OfferingDate-1']], axis=1)
sample_volumechg = matrix2.apply(lambda row: volumechg[row['gvkey']][row['OfferingDate-1']], axis=1)
matrix2 = pd.concat([matrix2, sample_return,sample_volumechg], axis=1, join_axes=[matrix2.index])
matrix2 = matrix2.rename(columns = {0:'td-1return'})
matrix2 = matrix2.rename(columns = {1:'td-1volumechg'})

matrix2['OfferingDate-2'] = matrix2['OfferingDate-1']-bday_us
sample_return = matrix2.apply(lambda row: adjreturn2[row['gvkey']][row['OfferingDate-2']], axis=1)
sample_volumechg = matrix2.apply(lambda row: volumechg[row['gvkey']][row['OfferingDate-2']], axis=1)
matrix2 = pd.concat([matrix2, sample_return,sample_volumechg], axis=1, join_axes=[matrix2.index])
matrix2 = matrix2.rename(columns = {0:'td-2return'})
matrix2 = matrix2.rename(columns = {1:'td-2volumechg'})

matrix2['OfferingDate-3'] = matrix2['OfferingDate-2']-bday_us
sample_return = matrix2.apply(lambda row: adjreturn2[row['gvkey']][row['OfferingDate-3']], axis=1)
sample_volumechg = matrix2.apply(lambda row: volumechg[row['gvkey']][row['OfferingDate-3']], axis=1)
matrix2 = pd.concat([matrix2, sample_return,sample_volumechg], axis=1, join_axes=[matrix2.index])
matrix2 = matrix2.rename(columns = {0:'td-3return'})
matrix2 = matrix2.rename(columns = {1:'td-3volumechg'})

matrix2['OfferingDate-4'] = matrix2['OfferingDate-3']-bday_us
sample_return = matrix2.apply(lambda row: adjreturn2[row['gvkey']][row['OfferingDate-4']], axis=1)
sample_volumechg = matrix2.apply(lambda row: volumechg[row['gvkey']][row['OfferingDate-4']], axis=1)
matrix2 = pd.concat([matrix2, sample_return,sample_volumechg], axis=1, join_axes=[matrix2.index])
matrix2 = matrix2.rename(columns = {0:'td-4return'})
matrix2 = matrix2.rename(columns = {1:'td-4volumechg'})

matrix2['OfferingDate-5'] = matrix2['OfferingDate-4']-bday_us
sample_return = matrix2.apply(lambda row: adjreturn2[row['gvkey']][row['OfferingDate-5']], axis=1)
sample_volumechg = matrix2.apply(lambda row: volumechg[row['gvkey']][row['OfferingDate-5']], axis=1)
matrix2 = pd.concat([matrix2, sample_return,sample_volumechg], axis=1, join_axes=[matrix2.index])
matrix2 = matrix2.rename(columns = {0:'td-5return'})
matrix2 = matrix2.rename(columns = {1:'td-5volumechg'})
##matrix2 = pd.merge(matrix2, mktcap.reset_index(), left_on=['companyid','OfferingDate+1'], right_on = ['companyid','pricingdate'])
##del matrix2['pricingdate']
#%%
##matrix2 = matrix2.dropna(subset=['sedol'])
##matrix2['testyear'] = matrix2['testyear'].astype(np.int64)
##matrix2['testmonth'] = matrix2['testmonth'].astype(np.int64)
matrix2 = pd.merge(matrix2, lifecycle, left_on=['companyid','testyear','testmonth'], right_on = ['ciqid','YEAR','MONTH'])
#%%
matrix2 = matrix2.set_index('OfferingDate').join(spx.set_index('Date'))
matrix2 = matrix2.rename(columns = {'Return':'tdmktret'})
matrix2 = matrix2.reset_index()
matrix2 = matrix2.rename(columns = {'index':'OfferingDate'})

matrix2 = matrix2.set_index('OfferingDate+1').join(spx.set_index('Date'))
matrix2 = matrix2.rename(columns = {'Return':'td+1mktret'})
matrix2 = matrix2.reset_index()
matrix2 = matrix2.rename(columns = {'index':'OfferingDate+1'})

matrix2 = matrix2.set_index('OfferingDate+2').join(spx.set_index('Date'))
matrix2 = matrix2.rename(columns = {'Return':'td+2mktret'})
matrix2 = matrix2.reset_index()
matrix2 = matrix2.rename(columns = {'index':'OfferingDate+2'})

matrix2 = matrix2.set_index('OfferingDate+3').join(spx.set_index('Date'))
matrix2 = matrix2.rename(columns = {'Return':'td+3mktret'})
matrix2 = matrix2.reset_index()
matrix2 = matrix2.rename(columns = {'index':'OfferingDate+3'})

matrix2 = matrix2.set_index('OfferingDate+4').join(spx.set_index('Date'))
matrix2 = matrix2.rename(columns = {'Return':'td+4mktret'})
matrix2 = matrix2.reset_index()
matrix2 = matrix2.rename(columns = {'index':'OfferingDate+4'})

matrix2 = matrix2.set_index('OfferingDate+5').join(spx.set_index('Date'))
matrix2 = matrix2.rename(columns = {'Return':'td+5mktret'})
matrix2 = matrix2.reset_index()
matrix2 = matrix2.rename(columns = {'index':'OfferingDate+5'})

matrix2 = matrix2.set_index('OfferingDate-1').join(spx.set_index('Date'))
matrix2 = matrix2.rename(columns = {'Return':'td-1mktret'})
matrix2 = matrix2.reset_index()
matrix2 = matrix2.rename(columns = {'index':'OfferingDate-1'})

matrix2 = matrix2.set_index('OfferingDate-2').join(spx.set_index('Date'))
matrix2 = matrix2.rename(columns = {'Return':'td-2mktret'})
matrix2 = matrix2.reset_index()
matrix2 = matrix2.rename(columns = {'index':'OfferingDate-2'})

matrix2 = matrix2.set_index('OfferingDate-3').join(spx.set_index('Date'))
matrix2 = matrix2.rename(columns = {'Return':'td-3mktret'})
matrix2 = matrix2.reset_index()
matrix2 = matrix2.rename(columns = {'index':'OfferingDate-3'})

matrix2 = matrix2.set_index('OfferingDate-4').join(spx.set_index('Date'))
matrix2 = matrix2.rename(columns = {'Return':'td-4mktret'})
matrix2 = matrix2.reset_index()
matrix2 = matrix2.rename(columns = {'index':'OfferingDate-4'})

matrix2 = matrix2.set_index('OfferingDate-5').join(spx.set_index('Date'))
matrix2 = matrix2.rename(columns = {'Return':'td-5mktret'})
matrix2 = matrix2.reset_index()
matrix2 = matrix2.rename(columns = {'index':'OfferingDate-5'})
##matrix2 = pd.merge(matrix2, spx, left_on=['OfferingDate'], right_on = ['IQ2668699'])
##del matrix2['IQ2668699']
##del matrix2['IQ_LASTSALEPRICE']
#%%
matrix2['tdidioret'] = matrix2[['tdreturn']].sub(matrix2['tdmktret'], axis=0)
matrix2['tdidioret+1'] = matrix2[['td+1return']].sub(matrix2['td+1mktret'], axis=0)
matrix2['tdidioret+2'] = matrix2[['td+2return']].sub(matrix2['td+2mktret'], axis=0)
matrix2['tdidioret+3'] = matrix2[['td+3return']].sub(matrix2['td+3mktret'], axis=0)
matrix2['tdidioret+4'] = matrix2[['td+4return']].sub(matrix2['td+4mktret'], axis=0)
matrix2['tdidioret+5'] = matrix2[['td+5return']].sub(matrix2['td+5mktret'], axis=0)

matrix2['tdidioret-1'] = matrix2[['td-1return']].sub(matrix2['td-1mktret'], axis=0)
matrix2['tdidioret-2'] = matrix2[['td-2return']].sub(matrix2['td-2mktret'], axis=0)
matrix2['tdidioret-3'] = matrix2[['td-3return']].sub(matrix2['td-3mktret'], axis=0)
matrix2['tdidioret-4'] = matrix2[['td-4return']].sub(matrix2['td-4mktret'], axis=0)
matrix2['tdidioret-5'] = matrix2[['td-5return']].sub(matrix2['td-5mktret'], axis=0)
##matrix2 = pd.concat([matrix2, fund['dataitemvalue'],fund['periodenddate'],fund['filingdate']], axis=1, join_axes=[matrix2.index])
##matrix2=matrix2.rename(columns = {'dataitemvalue':'netlev'})


##pandas.merge(mktcap_2.reset_index(), sample_df.reset_index(), left_on=['companyid','pricingdate'], right_on = ['companyid','date'])
#%%
matrix2['totidio+1'] = matrix2['tdidioret'] + matrix2['tdidioret+1']
matrix2['totidio+2'] = matrix2['totidio+1'] + matrix2['tdidioret+2']
matrix2['totidio+3'] = matrix2['totidio+2'] + matrix2['tdidioret+3']
matrix2['totidio+4'] = matrix2['totidio+3'] + matrix2['tdidioret+4']
matrix2['totidio+5'] = matrix2['totidio+4'] + matrix2['tdidioret+5']

matrix2['totidio-1'] = matrix2[['tdidioret']].sub(matrix2['tdidioret-1'], axis=0)
matrix2['totidio-2'] = matrix2[['totidio-1']].sub(matrix2['tdidioret-2'], axis=0)
matrix2['totidio-3'] = matrix2[['totidio-2']].sub(matrix2['tdidioret-3'], axis=0)
matrix2['totidio-4'] = matrix2[['totidio-3']].sub(matrix2['tdidioret-4'], axis=0)
matrix2['totidio-5'] = matrix2[['totidio-4']].sub(matrix2['tdidioret-5'], axis=0)

#%%
namatrix2 = matrix2[(matrix2['Domicile'] =='United States') | (matrix2['Domicile'] =='Canada')]
converts = namatrix2[(namatrix2['SecurityType'] =='Corporate Convertible')]
converts = converts.fillna(0)
converts1 = converts[(converts['LifeCode'] == 1)]
converts2 = converts[(converts['LifeCode'] == 2)]
converts3 = converts[(converts['LifeCode'] == 3)]
converts4 = converts[(converts['LifeCode'] == 4)]
converts5 = converts[(converts['LifeCode'] == 5)]
converts5ret = converts5[['testyear','tdidioret','tdidioret+1', 'tdidioret+2', 'tdidioret+3', 'tdidioret+4','tdidioret+5', 'tdidioret-1', 'tdidioret-2', 'tdidioret-3','tdidioret-4', 'tdidioret-5', 'totidio+1', 'totidio+2', 'totidio+3', 'totidio+4','totidio+5', 'totidio-2', 'totidio-3', 'totidio-4', 'totidio-5']]
converts4ret = converts4[['testyear','tdidioret','tdidioret+1', 'tdidioret+2', 'tdidioret+3', 'tdidioret+4','tdidioret+5', 'tdidioret-1', 'tdidioret-2', 'tdidioret-3','tdidioret-4', 'tdidioret-5', 'totidio+1', 'totidio+2', 'totidio+3', 'totidio+4','totidio+5', 'totidio-2', 'totidio-3', 'totidio-4', 'totidio-5']]
converts3ret = converts3[['testyear','tdidioret','tdidioret+1', 'tdidioret+2', 'tdidioret+3', 'tdidioret+4','tdidioret+5', 'tdidioret-1', 'tdidioret-2', 'tdidioret-3','tdidioret-4', 'tdidioret-5', 'totidio+1', 'totidio+2', 'totidio+3', 'totidio+4','totidio+5', 'totidio-2', 'totidio-3', 'totidio-4', 'totidio-5']]
converts2ret = converts2[['testyear','tdidioret','tdidioret+1', 'tdidioret+2', 'tdidioret+3', 'tdidioret+4','tdidioret+5', 'tdidioret-1', 'tdidioret-2', 'tdidioret-3','tdidioret-4', 'tdidioret-5', 'totidio+1', 'totidio+2', 'totidio+3', 'totidio+4','totidio+5', 'totidio-2', 'totidio-3', 'totidio-4', 'totidio-5']]
converts1ret = converts1[['testyear','tdidioret','tdidioret+1', 'tdidioret+2', 'tdidioret+3', 'tdidioret+4','tdidioret+5', 'tdidioret-1', 'tdidioret-2', 'tdidioret-3','tdidioret-4', 'tdidioret-5', 'totidio+1', 'totidio+2', 'totidio+3', 'totidio+4','totidio+5', 'totidio-2', 'totidio-3', 'totidio-4', 'totidio-5']]

stats.ttest_1samp(converts5ret,0)
stats.ttest_1samp(converts4ret,0)
stats.ttest_1samp(converts3ret,0)
stats.ttest_1samp(converts2ret,0)
stats.ttest_1samp(converts1ret,0)
#%%
corps = namatrix2[(namatrix2['SecurityType'] =='Corporate Debentures')|(namatrix2['SecurityType'] =='Corporate MTN')]
corps = corps.fillna(0)
corps1 = corps[(corps['LifeCode'] == 1)]
corps2 = corps[(corps['LifeCode'] == 2)]
corps3 = corps[(corps['LifeCode'] == 3)]
corps4 = corps[(corps['LifeCode'] == 4)]
corps5 = corps[(corps['LifeCode'] == 5)]
corps5ret = corps5[['testyear','tdidioret','tdidioret+1', 'tdidioret+2', 'tdidioret+3', 'tdidioret+4','tdidioret+5', 'tdidioret-1', 'tdidioret-2', 'tdidioret-3','tdidioret-4', 'tdidioret-5', 'totidio+1', 'totidio+2', 'totidio+3', 'totidio+4','totidio+5', 'totidio-2', 'totidio-3', 'totidio-4', 'totidio-5']]
corps4ret = corps4[['testyear','tdidioret','tdidioret+1', 'tdidioret+2', 'tdidioret+3', 'tdidioret+4','tdidioret+5', 'tdidioret-1', 'tdidioret-2', 'tdidioret-3','tdidioret-4', 'tdidioret-5', 'totidio+1', 'totidio+2', 'totidio+3', 'totidio+4','totidio+5', 'totidio-2', 'totidio-3', 'totidio-4', 'totidio-5']]
corps3ret = corps3[['testyear','tdidioret','tdidioret+1', 'tdidioret+2', 'tdidioret+3', 'tdidioret+4','tdidioret+5', 'tdidioret-1', 'tdidioret-2', 'tdidioret-3','tdidioret-4', 'tdidioret-5', 'totidio+1', 'totidio+2', 'totidio+3', 'totidio+4','totidio+5', 'totidio-2', 'totidio-3', 'totidio-4', 'totidio-5']]
corps2ret = corps2[['testyear','tdidioret','tdidioret+1', 'tdidioret+2', 'tdidioret+3', 'tdidioret+4','tdidioret+5', 'tdidioret-1', 'tdidioret-2', 'tdidioret-3','tdidioret-4', 'tdidioret-5', 'totidio+1', 'totidio+2', 'totidio+3', 'totidio+4','totidio+5', 'totidio-2', 'totidio-3', 'totidio-4', 'totidio-5']]
corps1ret = corps1[['testyear','tdidioret','tdidioret+1', 'tdidioret+2', 'tdidioret+3', 'tdidioret+4','tdidioret+5', 'tdidioret-1', 'tdidioret-2', 'tdidioret-3','tdidioret-4', 'tdidioret-5', 'totidio+1', 'totidio+2', 'totidio+3', 'totidio+4','totidio+5', 'totidio-2', 'totidio-3', 'totidio-4', 'totidio-5']]



