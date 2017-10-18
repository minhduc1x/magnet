--Attribute

Select * from Attribute
Order by AttributeName

--Financial Numeric Example

select AttributeName, DataTypeID, NumericValue, StringValue, CalendarYear, FilingDate 
from FinancialData fd inner join FinancialPeriod fp on fp.FinancialPeriodID = fd.FinancialPeriodID 
inner join Attribute a on a.AttributeID = fd.AttributeID 
where fd.EntityID = 299 and (CalendarQuarter is null or CalendarQuarter=0)

select EntityID, AttributeName, NumericValue, CalendarYear, CalendarQuarter, FilingDate 
from FinancialData fd inner join FinancialPeriod fp on fp.FinancialPeriodID = fd.FinancialPeriodID 
inner join Attribute a on a.AttributeID = fd.AttributeID 
where fd.AttributeID = 293 
--and (CalendarQuarter=1 or CalendarQuarter=2 or CalendarQuarter=3 or CalendarQuarter=4)


--Entity Numeric Example

Select EntityID, EBITDA_LTM as EBITDA, Interest_Expense_LTM as Int_Exp, Total_Debt_Latest as Total_Debt, Cash_Latest as Cash, CFO_LTM as CFO, 
Capex_LTM as Capex, ST_Investments_LTM as ST_Inv, MAGROI_LTM as MAGROI, MAGROI_Assets_LTM as MAGROI_Assets
FROM(select ed.EntityID, AttributeName, NumericValue 
from DataSet ds inner join DataSetMap dsm on ds.DataSetID = dsm.DataSetID 
inner join EntityData ed on ed.EntityDataID = dsm.ReferenceID 
inner join Attribute b on b.AttributeID = ed.AttributeID 
where ds.DataSetDate ='4/21/2017' and dsm.ReferenceTypeID = 1 and ed.AttributeID in (58,38,48,53,78,73,1075,2010,2014))p
PIVOT(SUM(NumericValue) FOR AttributeName IN (EBITDA_LTM, Interest_Expense_LTM,Total_Debt_Latest,Cash_Latest,CFO_LTM,Capex_LTM,ST_Investments_LTM,MAGROI_LTM,MAGROI_Assets_LTM))
AS pvt
Order by EntityID

--Entity Text Example

Select EntityID, ENTITY_NAME as Name, CIQ_INDUSTRY as Industry, CIQ_Subindustry as Subindustry
FROM(select ed.EntityID, AttributeName, StringValue 
from DataSet ds inner join DataSetMap dsm on ds.DataSetID = dsm.DataSetID 
inner join EntityData ed on ed.EntityDataID = dsm.ReferenceID 
inner join Attribute b on b.AttributeID = ed.AttributeID 
where ds.DataSetDate ='4/21/2017' and dsm.ReferenceTypeID = 1 and ed.AttributeID in (1,18,19))p
PIVOT(Max(StringValue) FOR AttributeName IN ([ENTITY_NAME],[CIQ_INDUSTRY],[CIQ_Subindustry]))
AS pvt
Order by EntityID

--Spread Numeric Code

Select EntityID, Markit_Recovery as Markit_Recovery, JTD_Risk_Equity as JTD_Risk_Equity, Market_Cap_USD_Market as Market_Cap_USD_Market, 
MAGROI_Mom_NTM as MAGROI_Mom_NTM, Equity_Notional as Equity_Notional, PD_BBG as PD_BBG, MARKIT_SPREAD as MARKIT_SPREAD_3YR
FROM(select EntityID, AttributeName, NumericValue 
from DataSet ds inner join DataSetMap dsm on ds.DataSetID = dsm.DataSetID 
inner join EntitySpreadCurveData esc on esc.EntitySpreadCurveDataID = dsm.ReferenceID 
inner join Attribute d on d.AttributeID = esc.AttributeID 
where ds.DataSetDate = '4/21/2017' and dsm.ReferenceTypeID = 3 and esc.AttributeID in (176,1958,1940,2043,184,2050,1943) and TenorID in (0,3))p
PIVOT(SUM(NumericValue) FOR AttributeName IN (Markit_Recovery, JTD_Risk_Equity, Market_Cap_USD_Market, MAGROI_Mom_NTM, Equity_Notional,PD_BBG,MARKIT_SPREAD))
AS pvt
Order by EntityID

--Spread Text Example

Select EntityID, SP_Rating as SP_Rating, SP_Outlook as SP_Outlook
FROM(select EntityID, AttributeName, StringValue 
from DataSet ds inner join DataSetMap dsm on ds.DataSetID = dsm.DataSetID 
inner join EntitySpreadCurveData esc on esc.EntitySpreadCurveDataID = dsm.ReferenceID 
inner join Attribute d on d.AttributeID = esc.AttributeID 
where ds.DataSetDate = '4/21/2017' and dsm.ReferenceTypeID = 3 and esc.AttributeID in (1936,1937))p
PIVOT(Max(StringValue) FOR AttributeName IN ([SP_Rating],[SP_Outlook]))
AS pvt
Order by EntityID

--Curve Code

Select EntityID, Year as Year, Month as Month, DEBT_MATURITY as Debt_Maturity, PF_OP_LIQUIDITY as PF_OP_LIQUIDITY, PF_DIVIDENDS as PF_DIVIDENDS, PF_UNDRAWN_REVOLVER as PF_UNDRAWN_REVOLVER
FROM(select esc.EntityID, AttributeName, Year, Month, NumericValue 
from DataSet ds inner join DataSetMap dsm on ds.DataSetID = dsm.DataSetID 
inner join EntityDebtCurveData esc on esc.EntityDebtCurveDataID = dsm.ReferenceID 
inner join Attribute d on d.AttributeID = esc.AttributeID 
where ds.DataSetDate = '4/24/2017' and dsm.ReferenceTypeID = 2 and esc.AttributeID in (166,1952,1953,1959) and Year>2015 and Year<2021)p
PIVOT(SUM(NumericValue) FOR AttributeName IN (DEBT_MATURITY, PF_OP_LIQUIDITY, PF_DIVIDENDS,PF_UNDRAWN_REVOLVER))
AS pvt
Order by EntityID, Year, Month

--PF Liquidity

Select q.EntityID, sum(PF_OP_LIQUIDITY)+SUM(PF_DIVIDENDS)+SUM(PF_UNDRAWN_REVOLVER)-(SUM(Debt_Maturity)/1000000) as PF, SUM(Debt_Maturity)/1000000 as Debt_Due
From(
	Select EntityID, DEBT_MATURITY as Debt_Maturity, PF_OP_LIQUIDITY as PF_OP_LIQUIDITY, PF_DIVIDENDS as PF_DIVIDENDS, PF_UNDRAWN_REVOLVER as PF_UNDRAWN_REVOLVER
	FROM(
	Select EntityID, AttributeName, NumericValue
	From (select esc.EntityID, AttributeName, Convert(date,(convert(varchar,Year) +'-'+ convert(varchar,Month) + '-20')) as Date, NumericValue 
		from DataSet ds inner join DataSetMap dsm on ds.DataSetID = dsm.DataSetID 
			inner join EntityDebtCurveData esc on esc.EntityDebtCurveDataID = dsm.ReferenceID 
			inner join Attribute d on d.AttributeID = esc.AttributeID 
		where ds.DataSetDate = '4/24/2017' and dsm.ReferenceTypeID = 2 and esc.AttributeID in (166,1952,1953,1959) and Year>2015 and Year <2021)r
	Where Date < '2020-07-20'
	)p
	PIVOT(SUM(NumericValue) FOR AttributeName IN (DEBT_MATURITY, PF_OP_LIQUIDITY, PF_DIVIDENDS,PF_UNDRAWN_REVOLVER))AS pvt
)q
GROUP BY q.EntityID
Order BY q.EntityID

--PDs 304=RF and 705=B+

select EntityID, DataSetDate, NumericValue 
from ProbabilityDefaultData P 
INNER JOIN DataSet DS On DS.DataSetID = P.DataSetID 
where ProbabilityDefaultModelID =705 and ProbabilityDefaultTypeID =3 and DS.DataSetDate >'1/1/2014'