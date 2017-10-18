# -*- coding: utf-8 -*-
"""
Created on Thu Oct  5 09:45:17 2017

@author: tranda
"""

import pandas as pd
from sqlalchemy import create_engine

# connects to CompustatData
engine1 = create_engine('mssql+pyodbc://tranda:Ekb4Fpr3s27w79GN@MNDEVDB01\DEVMTTM/CompustatData?driver=SQL+Server+Native+Client+10.0')
# connects to CCDB
engine2 = create_engine('mssql+pyodbc://tranda:Ekb4Fpr3s27w79GN@devEXT\ext/CCDB?driver=SQL+Server+Native+Client+10.0')

# read IDs from test_daniel.xlsx
reader = pd.read_excel(r'\\evprodfsg01\QR_Workspace\Debt Issue Study\ciqids.xlsx', 'Sheet1', na_values=['NA'])
Id_list = tuple(reader['id'].values)

gvkeyQuerry = """
set NOCOUNT ON
select gvkey + iid as id from UNIV_SECURITY where 
companyId in 
""" + str(Id_list)

gvkey_DF = pd.read_sql_query(gvkeyQuerry, engine2)
gvkey_list = tuple(gvkey_DF['id'].values)
string = ', '.join('\'{0}\''.format(w) for w in gvkey_list)

priceQuery = """
declare @dataItemId int
set @dataItemId = '4193' --1007 total assets
declare @evalDate datetime2
set @evalDate = dateadd(day,-1, cast(getdate() as date))

SELECT dp.[gvkey],dp.[iid],dp.[datadate],((isnull(dt.[trfd],1)) * dp.[prccd]) /  isnull(ad.[splitvalue],1) AS PRCCD_ADJ ,((isnull(dt.[trfd],1)) * dp.[cshtrd]) /  isnull(ad.[splitvalue],1) AS CSHTRD_ADJ
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
where dp.gvkey + dp.iid in ( """ + string + """ )
and dp.[datadate] between '12/20/2004' and @evalDate
order by dp.gvkey + dp.iid, datadate
"""

#adjprices = pd.read_sql_query(priceQuery, engine1)


