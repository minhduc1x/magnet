# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pypyodbc
import pandas as pd
import pandas.io.sql
from pandasql import sqldf

def dbGetQuery(q):
    return sqldf(q, globals())

#%% Get attribute data
fdidb = pypyodbc.connect("DRIVER={SQL Server}; SERVER=PRODCREDIT\CREDIT; DATABASE=FSTDB", autocommit=True)

sqlquerry = """
select ed1.NumericValue as PeriodType,AttributeName, ed.StringValue as CompanyID, fd.EntityID, DataTypeID, fd.NumericValue, fd.StringValue, CalendarYear, CalendarQuarter, FilingDate
from FinancialData fd 
left join FinancialPeriod fp on fp.FinancialPeriodID = fd.FinancialPeriodID 
left join Attribute a on a.AttributeID = fd.AttributeID
left join EntityData ed on ed.EntityID = fd.EntityID
left join EntityData ed1 on ed1.EntityID = fd.EntityID
where a.AttributeID in (293,288,311,406)
and ed.AttributeID = 4 
--and fd.EntityID in (13) --
and ed1.AttributeID = 318
--order by fd.EntityID, ed.StringValue, FilingDate,  ed1.NumericValue --
"""

attribute_df = pandas.io.sql.read_sql(sqlquerry, fdidb) 
attribute_df = attribute_df.drop_duplicates()

#%% Re-sort the dataframe. List all attributes based on entityid and companyid
query = """
select a1.entityid, a1.companyid, a1.numericvalue as reportingdate,
a1.filingdate, a1.calendarquarter,a1.calendaryear,a1.periodtype,
a2.numericvalue as cash,
a3.numericvalue as debt,
a4.numericvalue as ebitda

from attribute_df a1

inner join attribute_df a2 on a1.companyid = a2.companyid and a1.filingdate = a2.filingdate
                                and a1.periodtype = a2.periodtype
                                and a1.calendarquarter = a2.calendarquarter
                                and a1.entityid = a2.entityid
                                
inner join attribute_df a3 on a1.companyid = a3.companyid and a1.filingdate = a3.filingdate
                                and a1.periodtype = a3.periodtype
                                and a1.calendarquarter = a3.calendarquarter
                                and a1.entityid = a3.entityid

inner join attribute_df a4 on a1.companyid = a4.companyid and a1.filingdate = a4.filingdate
                                and a1.periodtype = a4.periodtype
                                and a1.calendarquarter = a4.calendarquarter
                                and a1.entityid = a4.entityid

where a1.attributename = 'Reporting_Date'
and a2.attributename = 'Cash'
and a3.attributename = 'Total_Debt'
and a4.attributename = 'EBITDA'

order by a1.entityid, a1.companyid, a1.periodtype, a1.filingdate
"""

companyattribute = dbGetQuery(query)
# parse reporting date
companyattribute['reportingdate'] = pd.to_datetime(companyattribute['reportingdate'],format="%Y%m%d")
# drop calendar quarter = 0
companyattribute = companyattribute[companyattribute.calendarquarter != 0]

#%% Do rolling sums
# period type = 0
company_prd00  = companyattribute[companyattribute.periodtype == 0]

# period type = 1
company_prd01 = companyattribute[companyattribute.periodtype == 1]
company_prd01['sumebitda']= company_prd01.groupby('entityid')['ebitda'].rolling(1).sum().reset_index(0,drop=True)
attribute_updated = company_prd01

# period type = 2
company_prd02 = companyattribute[companyattribute.periodtype == 2]
company_prd02['sumebitda']= company_prd02.groupby('entityid')['ebitda'].rolling(2).sum().reset_index(0,drop=True)
attribute_updated = attribute_updated.append(company_prd02,ignore_index=True)

# period type = 4
company_prd04 = companyattribute[companyattribute.periodtype == 4]
company_prd04['sumebitda']= company_prd04.groupby('entityid')['ebitda'].rolling(4).sum().reset_index(0,drop=True)
attribute_updated = attribute_updated.append(company_prd04,ignore_index=True)

