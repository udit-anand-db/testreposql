SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
GO
/* =============================================
   Author:      MJepsen
   Create date: 10/08/2014
   Description: Extracts the data for the data warehouse table factAccountHistory
   Change history:
        01/29/2015  P044166 MJepsen Corrected calculation for MarketFlux
        02/11/2015  P044168 MJepsen Added "IsManaged" output for SnapshotInfo lookup and updated to no longer use Financial_CashFlows_Base  (old logic)
        04/21/2015  P048164 MJepsen Added HouseholdAge
        05/07/2015  P048164 MJepsen Added table variables to improve performance (wouldn't finish in FTJ)
        05/13/2015  P050694 MJepsen Added DOB logic to handle invalid dates (source column is varchar)
        09/17/2015  P056709 MJepsen Discovered decimal data type error
        11/09/2015  P057299 ERubeck Added Account Asset Range calculation
        11/17/2015  P056109 MJepsen Modified cash div per Zach
        12/03/2015  P057299 ERubeck Added Account Value calculation, added Household Value calculation
        12/07/2015  P057299 ERubeck You shouldn't SUM a SUM, unless you really mean to do that.  Fixed Account/Household Value calculations
        01/07/2016  P065445 MJepsen Modified cash div per Zach (it was changed on build day when the change from 11/17 wasn't correct)
        02/01/2016  P065562 MJepsen Removed "IsIncluded" logic for transaction buckets 
        04/01/2017  P085587 ERubeck Turned table variable into a temp table for performance on larger databases and made optimizations
                                on some subquery left joins for SQL2014 CE.
        09/18/2017  P090731 ERubeck For weekly runs, change @dtForMonthEnding to GETDATE() in order to get results back from tblAssetValue and tblAssetValueChange 
                            when run earlier than EOM.
        03/12/2018  P110419 MJepsen Added comment for "Managed/Unmanaged" logic because it keeps coming up!
        03/19/2018  P112207 MJepsen Changed managed logic per Zach and cleaned up debug code
		03/26/2018	P112207	MJepsen	Removed INNER JOIN TO #AssetList so we get values of all assets, not just those with transactions
		03/27/2018	P112207	MJepsen	Removed #AssetList all together because it was setting all assets that had no activity to "Not Managed"
		04/14/2018	P110419	MJepsen	NULL fkRep in account causing data load error, need to handle NULLs
*		BIDevOps68397 06/14/2021  MJepsen     Moving to dw schema
		BIDevOps179058 10/26/2022  JDuke    Add IsAIP column
		193829      12/27/2022  ccleaveland change the creation of temp table #UserDefData and the index on the table to occur following the data insert for Brookstone test
		195624      1/3/2023  jduke change index creation until after temp table creation and inserts
==============================================*/
CREATE OR ALTER PROCEDURE [dw].[spDWExtractfactAccountHistory]
    @dtForMonthEnding DATETIME
AS
-- for testing
--exec spDWExtractfactAccountHistory '2022-12-31'
--DECLARE @dtForMonthEnding datetime = '12/31/2022'
SET NOCOUNT ON;

-- THIS PACKAGE IS ALWAYS A MONTHLY LOAD, so adjust extract dates accordingly
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
-- NOTE:  ending dates are always stored as date in the tables we are using, so we can ignore the time aspect

DECLARE @Server varchar (128)

IF @@ServerName LIKE 'SQLCommonAG%'
    BEGIN
        SET @Server = 'COMMONDB'
    END
ELSE
    BEGIN
        SET @Server = @@SERVERNAME
    END

DECLARE @ForMonthEnding int = CAST(@dtForMonthEnding AS float)

DECLARE @SnapshotMonthKey VARCHAR(10) = CONVERT (varchar(10), @dtForMonthEnding, 112) --Calculate this with the ending date now, before possibly changing @dtForMonthEnding

-- determine the first day of the month
DECLARE @dtForMonthBeginning datetime = DATEADD(dd,-(DAY(@ForMonthEnding)-1),@ForMonthEnding)
DECLARE @ForMonthBeginning int = CAST(@dtForMonthBeginning AS float)
-- determine the last day of the previous month
DECLARE @dtForPrevMonthEnding datetime = DATEADD(s, -1, DATEADD(mm, DATEDIFF(m, 0, @dtForMonthBeginning), 0)) 
DECLARE @ForPrevMonthEnding int = CAST(@dtForPrevMonthEnding AS float)
-- determine the first day of the previous month
DECLARE @dtForPrevMonthBeginning datetime = DATEADD(m, -1, @ForMonthBeginning)
DECLARE @ForPrevMonthBeginning int = CAST(@dtForPrevMonthBeginning AS float)

/*For weekly builds, move @dtForMonthEnding and @ForMonthEnding back to today if we're running this before @dtForMonthEnding*/
IF GETDATE() < @dtForMonthEnding
BEGIN
    SET @dtForMonthEnding = CAST(GETDATE() AS DATE);
    SET @ForMonthEnding = CAST(@dtForMonthEnding AS FLOAT);
END

/* for performance reasons, do some temp tables */
DROP TABLE IF EXISTS #Asset
CREATE TABLE #Asset (
    pkAccount int,
    AcctStartDate datetime,
    pkClient int,
    fkProduct int,
    pkAsset int,
    pkRegistration int,
    fkRep int,
    DOB datetime,
    IsActive bit,
    UsedFor INT
	--,
 --   INDEX #Asset_pkAccount CLUSTERED (pkAccount),
 --   INDEX #Asset_pkAsset NONCLUSTERED (pkAsset, fkProduct, pkAccount, pkClient)
)

DROP TABLE IF EXISTS #Trans
CREATE TABLE #Trans (
    pkAsset int,
    FinancialType tinyint,
    TWRType tinyint,
    Included tinyint,
    TransAmount decimal(19,2),
    Name varchar(50)
	--,
 --   INDEX #Trans_pkAsset CLUSTERED (pkAsset)
)

DROP TABLE IF EXISTS #UserDefData
CREATE TABLE #UserDefData (
    fkAsset INT,
    UsedFor INT 
	--,
 --   INDEX #UserDefData_pkAsset CLUSTERED (fkAsset)
)

DROP TABLE IF EXISTS #MinimumInceptDate
CREATE TABLE #MinimumInceptDate (
    fkRegistration int,
    MinimumInceptionDate DATETIME
	--,
 --   INDEX #MinimumInceptDate_fkRegistration CLUSTERED (fkRegistration)
)

DROP TABLE IF EXISTS #CashFlows
CREATE TABLE #CashFlows (
    pkAsset int,
    Included TINYINT,
    ContribAmt DECIMAL(19,2),
    DistribAmt DECIMAL(19,2),
    DivCash DECIMAL(19,2),
    ExchangeOut DECIMAL(19,2),
    ExchangeIn DECIMAL(19,2),
    AdvFeePaid DECIMAL(19,2),
    MiscCharge DECIMAL(19,2),
    JournalOut DECIMAL(19,2),
    JournalIn DECIMAL(19,2),
    MergeOut DECIMAL(19,2),
    MergeIn DECIMAL(19,2),
    DivInterestReinvest DECIMAL(19,2)
	--,
 --   INDEX #CashFlows_pkAsset CLUSTERED (pkAsset)
)

DROP TABLE IF EXISTS #AssetValueME
CREATE TABLE #AssetValueME (
    AsOfDate int,
    fkAsset int,
    UnitBalance DECIMAL(19,7),
    CalculatedValue DECIMAL(19,2)
	--,
 --   INDEX #AssetValueME_fkAsset CLUSTERED (AsOfDate, fkAsset)
)

DROP TABLE IF EXISTS #AssetValuePME
CREATE TABLE #AssetValuePME (
    AsOfDate int,
    fkAsset int,
    UnitBalance DECIMAL(19,7),
    CalculatedValue DECIMAL(19,2)
	--,
 --   INDEX #AssetValuePME_fkAsset CLUSTERED (AsOfDate, fkAsset)
)

DROP TABLE IF EXISTS #AssetValueChangeME
CREATE TABLE #AssetValueChangeME (
    fkAsset int,
    AsOfDate int,
    StartValue MONEY,
    ValueChange MONEY,
    FeeAmount MONEY,
    StartAccruedInt MONEY,
    AccruedIntChange MONEY,
    CashFlowAmount MONEY
	--,
 --   INDEX #AssetValueME_fkAsset CLUSTERED (fkAsset, AsOfDate)
)

DROP TABLE IF EXISTS #AssetValueChangePME
CREATE TABLE #AssetValueChangePME (
    fkAsset int,
    AsOfDate int,
    StartValue MONEY,
    ValueChange MONEY,
    FeeAmount MONEY,
    StartAccruedInt MONEY,
    AccruedIntChange MONEY,
    CashFlowAmount MONEY
	--,
 --   INDEX #AssetValuePME_fkAsset CLUSTERED (fkAsset, AsOfDate)
)

DROP TABLE IF EXISTS #AlternativeAsset
CREATE TABLE #AlternativeAsset (
    fkAsset INT
	--,
 --   INDEX #AlternativeAsset_fkAsset CLUSTERED (fkAsset)
)

--build a temp table for included unmanaged assets instead of left joining to a subquery
INSERT INTO #UserDefData
SELECT pkAsset, vw.UsedFor
FROM dbo.vwAssetUnmanagedOption vw -- preferred method per Zach 3/19/2018 

CREATE CLUSTERED INDEX #UserDefData_pkAsset ON #UserDefData (fkAsset)


UPDATE STATISTICS #UserDefData

INSERT INTO #Asset 
SELECT 
    acct.pkAccount
    , acct.AcctStartDate
    , c.pkClient
    , asset.fkProduct
    , asset.pkAsset
    , reg.pkRegistration
    , c.fkRep
    , CASE WHEN ISDATE(rp.DOB) = 1 THEN rp.DOB ELSE '01/01/1900' END
    , IsActive = acct.IsActive & reg.IsActive & c.IsActive
    -- it's not just whether it is unmanaged at the account, asset or product level, there is also an entity option override that determines status
    -- when in doubt if this is working, select from vwAssetUnmanagedOption... anything with bitwise 2 & UsedFor = 2, then we use it in Trends (Activity Summary report output type)
    , vw.UsedFor
FROM dbo.tblAsset asset
INNER JOIN dbo.tblAccount acct ON asset.fkAccount = acct.pkAccount
INNER JOIN dbo.tblRegistration reg ON acct.fkRegistration = reg.pkRegistration
INNER JOIN dbo.tblPersonal rp ON reg.fkPersonal = rp.pkPersonal
INNER JOIN dbo.tblClient c ON reg.fkClient = c.pkClient
INNER JOIN dbo.tblProductExt pExt ON asset.fkProduct = pExt.fkProduct
LEFT JOIN #UserDefData vw ON vw.fkAsset = asset.pkAsset

CREATE CLUSTERED INDEX #Asset_pkAccount ON #Asset (pkAccount)
CREATE NONCLUSTERED INDEX #Asset_pkAsset ON #Asset (pkAsset, fkProduct, pkAccount, pkClient)

UPDATE STATISTICS #Asset 

INSERT INTO #Trans
SELECT  asset.pkAsset, tt.FinancialType, tt.TWRType
    , Included = CASE 
            WHEN (2 & UsedFor) = 2 THEN 1 /* Used for Activity Summary*/ 
            ELSE 0
        END 
    , trx.TransAmount
    , tt.Name
FROM dbo.tblTransaction trx
    INNER JOIN #Asset asset ON trx.fkAsset = asset.pkAsset
    INNER JOIN dbo.tblTransType tt ON trx.fkTransType = tt.pkTransType
WHERE trx.fkTradeStatus = 1
    AND trx.fkTransType <> 46
    AND trx.TransDate BETWEEN @dtForMonthBeginning AND @dtForMonthEnding -- datetime

CREATE CLUSTERED INDEX #Trans_pkAsset ON #Trans (pkAsset)

UPDATE STATISTICS #Trans

--build a temp table for cash flows instead of left joining to a subquery
INSERT INTO #CashFlows
SELECT  trx.pkAsset, trx.Included
        , ContribAmt = ISNULL(CASE WHEN FinancialType = 1 THEN TransAmount END, 0) --contributions
        , DistribAmt = ISNULL(CASE WHEN FinancialType = 2 THEN TransAmount END, 0)-- distributions, including merge outs / mgmt style changes
        , DivCash = ISNULL(CASE WHEN FinancialType = 7 AND TWRType = 2 THEN trx.TransAmount END, 0) -- cash div's
        , ExchangeOut = ISNULL(CASE WHEN FinancialType IN (4,9) AND TransAmount < 0 THEN TransAmount END, 0)
        , ExchangeIn = ISNULL(CASE WHEN FinancialType IN (4,9) AND TransAmount > 0 THEN TransAmount END, 0)
        , AdvFeePaid = ISNULL(CASE WHEN FinancialType = 6 THEN TransAmount END, 0)
        , MiscCharge = ISNULL(CASE WHEN FinancialType = 10 THEN TransAmount END, 0)
        , JournalOut = ISNULL(CASE 
            WHEN trx.Included = 0 AND FinancialType IN (4,9) AND TransAmount > 0 THEN -TransAmount
            WHEN trx.Included = 1 AND FinancialType = 11 AND TransAmount < 0 THEN TransAmount 
        END, 0)
        , JournalIn =  ISNULL(CASE 
            WHEN trx.Included = 0 AND FinancialType IN (4,9) AND TransAmount < 0 THEN -TransAmount
            WHEN trx.Included = 1 AND FinancialType = 11 AND TransAmount > 0 THEN TransAmount 
        END, 0)
        , MergeOut = ISNULL(CASE WHEN FinancialType = 12 AND TransAmount < 0 THEN TransAmount END, 0)
        , MergeIn = ISNULL(CASE WHEN FinancialType = 12 AND TransAmount > 0 THEN TransAmount END, 0)
        , DivInterestReinvest = ISNULL(CASE WHEN FinancialType = 5 THEN TransAmount END, 0)
FROM #Trans AS trx

CREATE CLUSTERED INDEX #CashFlows_pkAsset ON #CashFlows (pkAsset)

UPDATE STATISTICS #CashFlows

--build a temp table for minimum household inception date instead of left joining to a subquery
INSERT INTO #MinimumInceptDate
SELECT fkRegistration, 
        MinimumInceptionDate = COALESCE(CAST(MIN(i.InceptionDate) AS datetime), MIN(acct.AcctStartDate))
FROM dbo.tblAsset a
JOIN dbo.tblAccount acct ON a.fkAccount = acct.pkAccount
LEFT JOIN dbo.tblAssetInceptionCloseDates i ON i.fkAsset = a.pkAsset
GROUP BY acct.fkRegistration

CREATE CLUSTERED INDEX #MinimumInceptDate_fkRegistration ON #MinimumInceptDate (fkRegistration)

UPDATE STATISTICS #MinimumInceptDate

INSERT INTO #AssetValueME
SELECT av.AsOfDate ,av.fkAsset, av.UnitBalance, av.CalculatedValue
FROM #Asset a 
INNER JOIN dbo.tblAssetValue av ON av.fkAsset = a.pkAsset AND av.AsOfDate = @ForMonthEnding -- int

CREATE CLUSTERED INDEX #AssetValueME_fkAsset ON #AssetValueME (AsOfDate, fkAsset)

UPDATE STATISTICS #AssetValueME

INSERT INTO #AssetValuePME
SELECT prevav.AsOfDate ,prevav.fkAsset, prevav.UnitBalance, prevav.CalculatedValue
FROM #Asset a 
INNER JOIN dbo.tblAssetValue prevav ON prevav.fkAsset = a.pkAsset AND prevav.AsOfDate = @ForPrevMonthEnding -- int

CREATE CLUSTERED INDEX #AssetValuePME_fkAsset ON #AssetValuePME (AsOfDate, fkAsset)

UPDATE STATISTICS #AssetValuePME


INSERT INTO #AssetValueChangeME
SELECT avc.fkAsset, avc.AsOfDate, avc.StartValue, avc.ValueChange, avc.FeeAmount, avc.StartAccruedInt, avc.AccruedIntChange, avc.CashFlowAmount
FROM #Asset a 
INNER JOIN dbo.tblAssetValueChange avc ON avc.fkAsset = a.pkAsset AND avc.AsOfDate = @ForMonthEnding  + 1 -- int

CREATE CLUSTERED INDEX #AssetValueME_fkAsset ON #AssetValueChangeME (fkAsset, AsOfDate)

UPDATE STATISTICS #AssetValueChangeME

INSERT INTO #AssetValueChangePME
SELECT prevavc.fkAsset, prevavc.AsOfDate, prevavc.StartValue, prevavc.ValueChange, prevavc.FeeAmount, prevavc.StartAccruedInt, prevavc.AccruedIntChange, prevavc.CashFlowAmount
FROM #Asset a 
INNER JOIN dbo.tblAssetValueChange prevavc ON prevavc.fkAsset = a.pkAsset AND prevavc.AsOfDate = @ForPrevMonthEnding + 1 --int

CREATE CLUSTERED INDEX #AssetValuePME_fkAsset ON #AssetValueChangePME (fkAsset, AsOfDate)

UPDATE STATISTICS #AssetValueChangePME

INSERT INTO #AlternativeAsset
SELECT DISTINCT z.pkasset
FROM
(
SELECT pkasset FROM dbo.vwAlternativeAssets
UNION ALL
SELECT pkasset FROM dbo.vwAlternativeProducts
WHERE pkAsset IS NOT NULL
) z

CREATE CLUSTERED INDEX #AlternativeAsset_fkAsset ON #AlternativeAsset (fkAsset)

SELECT 
    -- for key lookups
    DatabaseName = CAST(DB_NAME() AS VARCHAR(255))
    , ServerName = CAST(@Server AS VARCHAR(255))
    , SnapshotMonthKey = @SnapshotMonthKey
    , AccountId = ISNULL(a.pkAccount, -1) -- for AccountKey
    , ProductId = ISNULL(a.fkProduct, -1) -- for ProductKey
    , AssetID = ISNULL(a.pkAsset, -1)
    , RepresentativeID = ISNULL(a.fkRep, -1) -- for RepresentativeKey
    , Age = CAST(CASE 
        WHEN a.DOB IS NULL THEN -1
        WHEN a.DOB = '' THEN -1
        WHEN DATEDIFF(yyyy, a.DOB, @ForMonthEnding) > 0
            AND DATEDIFF(yyyy, a.DOB, @ForMonthEnding) < 114 THEN DATEDIFF(yyyy,a.DOB, @ForMonthEnding)
        ELSE -1
    END AS SMALLINT)-- for AgeRangeKey
    , AccountAge = CAST(ISNULL(DATEDIFF(mm, a.AcctStartDate, @dtForMonthEnding), -1) AS SMALLINT) -- for AccountAgeRangeKey
    , NumberOfUnits = ISNULL(av.UnitBalance, 0.00)
    , BeginningAUM = CAST(ISNULL(prevav.CalculatedValue, 0.00) AS DECIMAL(19,4))
    , BeginningBondAccrualValue = CAST(ISNULL(prevavc.StartAccruedInt, 0.00) AS DECIMAL(19,4))
    , EndingAUM = CAST(ISNULL(av.CalculatedValue, 0.00) AS DECIMAL(19,4)) 
    , EndingBondAccrualValue = CAST(ISNULL(avc.StartAccruedInt, 0.00) AS DECIMAL(19,4))
    , ContributionAmount = CAST(SUM(ISNULL(cf.ContribAmt, 0.00)) AS DECIMAL(19,4))
    , DistributionAmount = CAST(SUM(ISNULL(cf.DistribAmt, 0.00)) AS DECIMAL(19,4))
    , CashDividendAmount = CAST(SUM(ISNULL(cf.DivCash, 0.00)) AS DECIMAL(19,4))
    , TransferInAmount = CAST(SUM(ISNULL(cf.ExchangeIn , 0.00)) AS DECIMAL(19,4))
    , TransferOutAmount = CAST(SUM(ISNULL(cf.ExchangeOut, 0.00)) AS DECIMAL(19,4))
    , JournalInAmount = CAST(SUM(ISNULL(cf.JournalIn, 0.00)) AS DECIMAL(19,4))
    , JournalOutAmount = CAST(SUM(ISNULL(cf.JournalOut, 0.00)) AS DECIMAL(19,4))
    , MergeInAmount = CAST(SUM(ISNULL(cf.MergeIn, 0.00)) AS DECIMAL(19,4))
    , MergeOutAmount = CAST(SUM(ISNULL(cf.MergeOut, 0.00)) AS DECIMAL(19,4))
    , NewAccountValue = CAST(0.00 AS DECIMAL(19,4))
    , ClosedAccountValue = CAST(0.00 AS DECIMAL(19,4))
    , AdvisoryFeeAmount = CAST(SUM(ISNULL(cf.AdvFeePaid, 0.00)) AS DECIMAL(19,4))
    , MiscellaneousChargesAmount = CAST(SUM(ISNULL(cf.MiscCharge, 0.00)) AS DECIMAL(19,4))
    , ReinvestmentAmount = CAST(SUM(ISNULL(cf.DivInterestReinvest, 0.00)) AS DECIMAL(19,4))
    , MarketFluxAmount = ISNULL(av.CalculatedValue, 0.00) - ISNULL(prevav.CalculatedValue, 0.00) - 
        (
          ISNULL(SUM(cf.ContribAmt), 0.00) 
        + ISNULL(SUM(cf.DistribAmt), 0.00) 
        + ISNULL(SUM(cf.DivCash), 0.00) 
        + ISNULL(SUM(cf.ExchangeIn), 0.00)
        + ISNULL(SUM(cf.ExchangeOut), 0.00)
        + ISNULL(SUM(cf.JournalIn), 0.00)
        + ISNULL(SUM(cf.JournalOut), 0.00)
        + ISNULL(SUM(cf.MergeIn), 0.00)
        + ISNULL(SUM(cf.MergeOut), 0.00)
        + ISNULL(SUM(cf.AdvFeePaid), 0.00)
        + ISNULL(SUM(cf.MiscCharge), 0.00)
        + ISNULL(SUM(cf.DivInterestReinvest), 0.00)
     )
    , a.IsActive
    , PerformanceStartValue =  CAST(COALESCE(prevavc.StartValue, prevav.CalculatedValue, 0.00) AS DECIMAL(19,4))
    , PerformanceValueChange = CAST(COALESCE(avc.ValueChange, 0.00) AS DECIMAL(19,4))
    , PerformanceFeeAmount = CAST(COALESCE(avc.FeeAmount, 0.00) AS DECIMAL(19,4))
    , PerformanceCashFlowAmount = CAST(COALESCE(avc.CashFlowAmount, 0.00) AS DECIMAL(19,4))
    , PerformanceAccruedIntChange = CAST(COALESCE(avc.AccruedIntChange, 0.00) AS DECIMAL(19,4))

    -- for SnapshotInfoKey lookup
    , IsManaged = CAST(CASE 
            WHEN (2 & a.UsedFor) = 2 THEN 'Managed' /* Used for Activity Summary*/ 
            ELSE 'Not Managed'
        END AS VARCHAR(50))
    , HouseholdAge = CAST(ISNULL(DATEDIFF(mm, hi.MinimumInceptionDate, @dtForMonthEnding), -1) AS SMALLINT) -- for AccountAgeRangeKey
    ,SUM(CAST(ISNULL(av.CalculatedValue, 0.00) AS DECIMAL(19,4))) OVER (PARTITION BY ISNULL(a.pkAccount, -1)) AS AccountValue
    ,SUM(CAST(ISNULL(av.CalculatedValue, 0.00) AS DECIMAL(19,4))) OVER (PARTITION BY ISNULL(a.pkClient, -1)) AS HouseholdValue
	,CAST(CASE WHEN altasst.fkAsset IS NOT NULL THEN 1 ELSE 0 END AS SMALLINT) AS IsAIP
FROM #Asset a 
LEFT JOIN #AssetValueME av ON av.fkAsset = a.pkAsset AND av.AsOfDate = @ForMonthEnding -- int
LEFT JOIN #AssetValueChangeME avc ON avc.fkAsset = a.pkAsset AND avc.AsOfDate = @ForMonthEnding  + 1 -- int
LEFT JOIN #AssetValuePME prevav ON prevav.fkAsset = a.pkAsset AND prevav.AsOfDate = @ForPrevMonthEnding -- int
LEFT JOIN #AssetValueChangePME prevavc ON prevavc.fkAsset = a.pkAsset AND prevavc.AsOfDate = @ForPrevMonthEnding + 1 --int
LEFT JOIN #MinimumInceptDate hi ON hi.fkRegistration = a.pkRegistration
-- get cash flows for activity summary
LEFT JOIN #CashFlows cf ON a.pkAsset = cf.pkAsset 
LEFT JOIN #AlternativeAsset altasst ON a.pkAsset = altasst.fkAsset
GROUP BY a.pkAsset
    , a.fkProduct
    , a.pkAccount
    , a.pkClient
    , a.IsActive
    , a.UsedFor
    , a.AcctStartDate
    , a.DOB
    , a.fkRep
    , av.CalculatedValue
    , av.UnitBalance
    , avc.AccruedIntChange
    , avc.ValueChange
    , avc.FeeAmount
    , avc.CashFlowAmount
    , avc.StartAccruedInt
    , prevav.CalculatedValue
    , prevavc.StartValue
    , prevavc.StartAccruedInt
    , prevavc.ValueChange
    , prevavc.FeeAmount
    , prevavc.CashFlowAmount
    , hi.MinimumInceptionDate
	, CAST(CASE WHEN altasst.fkAsset IS NOT NULL THEN 1 ELSE 0 END AS SMALLINT)
ORDER BY a.pkAccount
GO