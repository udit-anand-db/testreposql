SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/* =============================================
DevOps:  133975
Author: ccleaveland
Create date: 05/05/2022
Description:    Extracts the the main data at the Account grain used for Invoicing

Change history:
        01/27/2023  192921			 Add column HouseHold Category to distinguish Employee accounts
		10/25/2023	248936	JDuke	 Added SET NOCOUNT ON
		06/11/2024	BI-31	DSiehler Added IsAPIAutomatedAccount Column
==============================================*/
CREATE OR ALTER PROCEDURE [dw].[spDWExtractBillingAccountDetail] 
                     @asOf DATETIME    -- job Controller ExtractEndDate
AS

BEGIN
SET NOCOUNT ON;

-- Variable set up
--------declare @asOf datetime = '01/31/2023'    /*  TESTING ONLY  */

declare @clientName varchar(50)
    , @fkSvcTeam INT
    , @demo INT
    , @pkALClient INTEGER
    , @isSleeveTrade varchar(20)
    , @quarterStart DATETIME
    , @dbname VARCHAR(128)

SET @dbname = DB_NAME()    

select @clientName = alc.ClientName, @fkSvcTeam = alc.fkServiceTeam, @demo = alc.IsDemo, @pkALClient = alc.pkALClient
from AdvLynx.dbo.tblALClient alc
where alc.DBName = @dbname
    AND alc.ClientName NOT IN ('CLS Retire', 'PSI Retire')

if @clientName = 'CLS Retire' OR @clientName = 'PSI Retire' OR @demo = 1
    RETURN
    


select @isSleeveTrade = ISNULL(data.FieldValue, udd.DefaultValue)
from dbo.tblUserDefineDef udd
    left join dbo.tblUserDefineData data ON data.fkUserDefineDef = udd.pkUserDefineDef AND data.EntityEnum = 39
where udd.Description = 'Sleeve Trading'

if @asOf is null
    select @asOf = floor( cast( getdate() as float ) )


select @quarterStart = dbo.QuarterStart( @asOf )



DROP TABLE IF EXISTS #sleeves
create table dbo.#sleeves (
      fkRegistration    int             primary key clustered
    , Sleeves           int
)


DROP TABLE IF EXISTS #byallFormat
create table dbo.#byallFormat (
      fkaccount    int             primary key clustered
    , IsByall           bit
)

DROP TABLE IF EXISTS #MoneyMarkets
CREATE TABLE #MoneyMarkets (
    fkAccount         int             primary key clustered
    , [value]  DECIMAL (18,2)
)

DROP TABLE IF EXISTS #AcctValue
CREATE TABLE #AcctValue (
    fkAccount   INT PRIMARY key clustered
    , [value]   DECIMAL (18,2)
)

DROP TABLE IF EXISTS #AcctCashPerc
CREATE TABLE #AcctCashPerc (
    fkAccount   INT PRIMARY KEY CLUSTERED
    , [AccountPctInCash]   DECIMAL (19,6)
)



------------------------------create account % cash column---------------------------------------------------------------
INSERT INTO #MoneyMarkets ( fkAccount, [value] )
SELECT 
    a.fkAccount
    , SUM(CAST( aov.UnitBalance * aov.navPrice AS DECIMAL(18,2) ) ) 
FROM dbo.tblAsset a
INNER JOIN dbo.tblAssetValue aov ON a.pkasset = AOV.fkAsset AND AsOfDate = FLOOR(CAST(CAST(@asOf AS DATETIME) AS FLOAT))
INNER JOIN dbo.tblProductExt pext ON a.fkproduct = pext.fkProduct
INNER join dbo.tblProductClass pc on pext.fkProductClass = pc.pkProductClass
where pc.category = 'M'
group by a.fkAccount

INSERT INTO #AcctValue ( fkAccount, [value] )
select a.fkAccount
    , Sum(cast( aov.UnitBalance * aov.navPrice as decimal(18,2) ) ) 
from dbo.tblAsset a
    inner join dbo.tblAssetValue aov on a.pkasset = AOV.fkAsset and AsOfDate = FLOOR(Cast(Cast(@asOf as datetime) as float))
group by a.fkAccount

INSERT INTO #AcctCashPerc ( fkAccount, [AccountPctInCash] )
SELECT m.fkAccount
    , cast( m.Value / a.Value * 100 as decimal( 19,6 ) )
FROM #MoneyMarkets m
JOIN #AcctValue a ON a.fkAccount = m.fkAccount
WHERE a.value <> 0


INSERT INTO #byallFormat ( fkaccount, IsByall )
SELECT DISTINCT vac.fkAccount
    , IsByall = 1
FROM dbo.tblByAllAccount ba
JOIN dbo.tblAcctCode vac ON vac.AcctCode = ba.AcctCode
JOIN dbo.fnEntityAsOfValueSimple(7,null,@asof) v ON v.fkEntity = vac.fkAccount
WHERE ba.isNotInSource = 0


-----------------------------------find sleeves-----------------------------------

if @isSleeveTrade = 'True' 
	BEGIN
		insert into dbo.#sleeves (fkRegistration, Sleeves)
		select acct.fkRegistration, COUNT( * ) AS Sleeves
	   FROM dbo.tblAccount acct
			INNER join 
			   (
				SELECT fkAccount 
				FROM dbo.tblAsset
				GROUP BY fkAccount
				) a on acct.pkAccount = a.fkAccount
			INNER join dbo.tblRegistration r on acct.fkRegistration = r.pkRegistration
			inner join dbo.tblAccountSleeve s ON s.fkAccount = acct.pkAccount
		where acct.IsActive = 1
			AND acct.IsHistorical = 0
				and s.SleeveType = 0
		group by acct.fkRegistration

	END


--non-sleeve accounts first
SELECT
      [AdvisorName] = @clientName
    , [AdvisorNumber] = data.FieldValue
    , [AccountID] = acct.pkAccount
    , [Sleeves] = 0
    , ClientID = c.pkClient
    , ClientLastName = cp.LName                                                                      --- 20220428 LName only ok per Jason
    , RegID = r.pkRegistration
    , AccountNumber = vac.AcctCode
    , AsOfDate = @asOf
    , MgmtStyle = ISNULL(pf.Name, '')
    , RepName = repp.EntityName
    , AccountIsActive = acct.IsActive
    , RegType = rt.sRegDesc
    , AccountStatus = CASE
            WHEN acct.AccountStatus = 1 THEN 'Normal Downloading'
            WHEN acct.AccountStatus = 2 THEN 'Non-Downloading'
            WHEN acct.AccountStatus = 3 THEN 'Manually Managed'
            WHEN acct.AccountStatus = 4 THEN 'DB Copy'
            WHEN acct.AccountStatus = 5 THEN 'Demo'
            WHEN acct.AccountStatus = 6 THEN 'Pending'
            ELSE 'Normal Downloading'
        END
    , acct.OutsideID
    , Custodian = custp.EntityName
    , ff.FundName
    , acct.AcctStartDate
    , SubAdvisor = subp.EntityName
    , FeeSchedule = bs.sSchedule
    , ModelName = m.[Model Name]
    , BusinessLine = blp.EntityName
    , DBName = @dbname
    , DownloadSource = DB_NAME() + '_' + CAST( acct.fkDownloadSource AS VARCHAR(4) )
    , IsSleeved = CAST( 0 AS BIT )
	, CAST(IIF(acct.CreatedBy='api_automatedaccounts',1,0) AS BIT) AS IsAPIAutomatedAccount
    , RepID = rep.pkRep
    , rep.RepNo
    , bdPers.EntityName AS 'BrokerDealerName'
    , meth.sTransmitMethod AS StatementTransmitMethod 
    , atc.[AccountPctInCash]
    , pkALClient = @pkALClient   
    , CommonCustodian = CASE WHEN ds.DownloadDesc LIKE 'ByAll%' THEN 'ByAllAccounts'
                             WHEN ds.DownloadDesc LIKE 'Quovo%' THEN 'Quovo'
                            WHEN ds.DownloadDesc LIKE 'DST%' THEN 'DST'
                             WHEN  ds.pkDownloadSource IS NOT NULL THEN ISNULL(cc.[Name],ds.DownloadDesc)  
                            WHEN ds.DownloadDesc IS NULL OR ds.DownloadDesc IN ('Spreadsheet','SWOT')  THEN 'ManuallyMaintained'
                             ELSE NULL  
                        END
    , ds.fkDownloadFormat
    , ds.DownloadDesc
    , ds.CustodianCommonCode
    , HouseholdCategoryCode = ISNULL(cat.Name, '?Unknown')
    , HouseholdCategoryName = ISNULL(cat.[Description], '?Unknown')

FROM dbo.tblAccount acct
	INNER JOIN dbo.tblBillAccount ba ON acct.pkAccount = ba.fkAccount
    INNER JOIN dbo.tblBillSchedule bs ON ba.fkBillFeeSchedule = bs.pkBillSchedule
    INNER JOIN AdvLynx.dbo.tblServiceTeam st ON st.pkServiceTeam = @fkSvcTeam
    INNER JOIN dbo.tblUserDefineDef def ON def.Description = 'Advisor Number' AND def.EntityEnum = 69
    LEFT JOIN dbo.tblUserDefineData data ON def.pkUserDefineDef = data.fkUserDefineDef 
    INNER JOIN dbo.tblRegistration r ON acct.fkRegistration = r.pkRegistration
    INNER JOIN dbo.tblPersonal rp ON r.fkPersonal = rp.pkPersonal
    INNER JOIN dbo.tblClient c ON r.fkClient = c.pkClient
    LEFT OUTER JOIN dbo.tblBillTransmitMethod meth ON c.fkBillTransmitMethod = meth.pkBillTransmitMethod 
    INNER JOIN dbo.tblPersonal cp ON c.fkPersonal = cp.pkPersonal
    INNER JOIN dbo.tblRep rep ON c.fkRep = rep.pkRep
    LEFT JOIN dbo.tblBrokerDealer bd ON rep.fkBrokerDealer = bd.pkBrokerDealer
    LEFT JOIN dbo.tblPersonal bdPers ON bd.fkPersonal = bdPers.pkPersonal
    INNER JOIN dbo.tblPersonal repp ON rep.fkPersonal = repp.pkPersonal
    INNER JOIN dbo.tblRegistrationType rt ON r.fkRegistrationType = rt.pkRegistrationType
    INNER JOIN dbo.tblFundFamily ff ON acct.fkFundFamily = ff.pkFundFamily
    INNER JOIN dbo.tblCustodian cust ON acct.fkCustodian = cust.pkCustodian
    INNER JOIN dbo.tblPersonal custp ON cust.fkPersonal = custp.pkPersonal
    INNER JOIN dbo.tblAcctCode vac ON acct.pkAccount = vac.fkAccount
    LEFT JOIN dbo.tblPlatform pf ON acct.fkPlatform = pf.pkPlatform
    LEFT JOIN dbo.tblSubAdvisor sub ON acct.fkSubAdvisor = sub.pkSubAdvisor
    LEFT JOIN dbo.tblPersonal subp ON sub.fkPersonal = subp.pkPersonal
    LEFT JOIN ACCOUNTS_BY_MODELAGG m ON acct.pkAccount = m.[Account ID]
    LEFT JOIN dbo.tblAccountBusinessLine abl ON acct.pkAccount = abl.fkAccount
    LEFT JOIN dbo.tblBusinessLine bl ON abl.fkBusinessLine = bl.pkBusinessLine
    LEFT JOIN dbo.tblPersonal blp ON bl.fkPersonal = blp.pkPersonal
    LEFT JOIN dbo.tblRegistrationSleeve rs ON acct.fkRegistration = rs.fkRegistration AND rs.IsActive = 1 AND @isSleeveTrade = 'True'
    LEFT JOIN #AcctCashPerc atc ON atc.fkAccount = acct.pkAccount
    LEFT JOIN dbo.tblDownloadSource ds ON ds.pkDownloadSource = acct.fkDownloadSource
    LEFT JOIN AdvLynx.dbo.tblCustodianCommon cc ON cc.pkCustodianCommon = ds.FkCustodianCommon
	LEFT JOIN dbo.tblClientCat cat ON c.fkClientCat = cat.pkClientCat
WHERE 1 = 1
    AND acct.IsHistorical = 0                                                                                                  -- we don't want historical accounts and this is accurate per Brian Barsch
    AND acct.AccountStatus <> 5 
    AND rs.fkRegistration IS NULL
	AND (cc.Name NOT LIKE '%FTJ%' OR  cc.Name IS NULL)
	AND NOT EXISTS (
			SELECT t.pkAccount, ds.DownloadDesc
			FROM dbo.tblAccount t
			INNER JOIN dbo.tblDownloadSource ds ON ds.pkDownloadSource = t.fkDownloadSource
			WHERE 1 = 1
			AND ds.DownloadDesc = 'AIP SUB - Professional Service'
			AND t.pkAccount = acct.pkAccount)


UNION ALL

--then sleeve accounts
SELECT DISTINCT
      [AdvisorName] = @clientName
    , [AdvisorNumber] = data.FieldValue
    , [AccountID] = acct.pkAccount
    , [Sleeves] = CASE WHEN sleeves.Sleeves > 0 THEN sleeves.Sleeves - 1 ELSE 0 END                                    
    , c.pkClient AS clientID
    , cp.LName AS clientLastName
    , r.pkRegistration AS regID
    , vac.AcctCode AS accountNumber
    , @asOf AS AsOfDate
    , ISNULL(pf.Name, '') AS MgmtStyle
    , repp.EntityName AS repName
    , acct.IsActive AS accountIsActive
    , rt.sRegDesc AS regType
    , CASE
            WHEN acct.AccountStatus = 1 THEN 'Normal Downloading'
            WHEN acct.AccountStatus = 2 THEN 'Non-Downloading'
            WHEN acct.AccountStatus = 3 THEN 'Manually Managed'
            WHEN acct.AccountStatus = 4 THEN 'DB Copy'
            WHEN acct.AccountStatus = 5 THEN 'Demo'
            WHEN acct.AccountStatus = 6 THEN 'Pending'
            ELSE 'Normal Downloading'
        END AS AccountStatus
    , acct.OutsideID
    , custp.EntityName AS custodian
    , ff.FundName
    , acct.AcctStartDate
    , subp.EntityName AS SubAdvisor
    , bs.sSchedule AS FeeSchedule
    , ModelName = m.[Model Name]
    , blp.EntityName AS BusinessLine
    , dbname = @dbname
    , DownloadSource = DB_NAME() + '_' + CAST( acct.fkDownloadSource AS VARCHAR(4) )
    , IsSleeved = CAST( 1 AS BIT )
	, CAST(IIF(acct.CreatedBy='api_automatedaccounts',1,0) AS BIT) AS IsAPIAutomatedAccount
    , RepID = rep.pkRep
    , rep.RepNo
    , bdPers.EntityName AS 'BrokerDealerName'
    , meth.sTransmitMethod AS StatementTransmitMethod
    , atc.[AccountPctInCash]
    , pkALClient = @pkALClient   
    , CommonCustodian = CASE WHEN ds.DownloadDesc LIKE 'ByAll%' THEN 'ByAllAccounts'
                             WHEN ds.DownloadDesc LIKE 'Quovo%' THEN 'Quovo'
                            WHEN ds.DownloadDesc LIKE 'DST%' THEN 'DST'
                             WHEN  ds.pkDownloadSource IS NOT NULL THEN ISNULL(cc.[Name],ds.DownloadDesc)  
                            WHEN ds.DownloadDesc IS NULL OR ds.DownloadDesc IN ('Spreadsheet','SWOT')  THEN 'ManuallyMaintained'
                             ELSE NULL  
                        END
    , ds.fkDownloadFormat
    , ds.DownloadDesc
    , ds.CustodianCommonCode
    , HouseholdCategoryCode = ISNULL(cat.Name, '?Unknown')
    , HouseholdCategoryName = ISNULL(cat.[Description], '?Unknown')
FROM dbo.tblAccount acct
    INNER JOIN dbo.tblAccountSleeve s ON acct.pkAccount = s.fkAccount AND s.SleeveType = 1
    INNER JOIN dbo.tblBillAccount ba ON acct.pkAccount = ba.fkAccount
    INNER JOIN dbo.tblBillSchedule bs ON ba.fkBillFeeSchedule = bs.pkBillSchedule
    INNER JOIN AdvLynx.dbo.tblServiceTeam st ON st.pkServiceTeam = @fkSvcTeam
    INNER JOIN dbo.tblUserDefineDef def ON def.Description = 'Advisor Number' AND def.EntityEnum = 69
    LEFT JOIN dbo.tblUserDefineData data ON def.pkUserDefineDef = data.fkUserDefineDef 
    INNER JOIN dbo.tblRegistration r ON acct.fkRegistration = r.pkRegistration
    INNER JOIN dbo.tblPersonal rp ON r.fkPersonal = rp.pkPersonal
    INNER JOIN dbo.tblClient c ON r.fkClient = c.pkClient
    LEFT OUTER JOIN dbo.tblBillTransmitMethod meth ON c.fkBillTransmitMethod = meth.pkBillTransmitMethod 
    INNER JOIN dbo.tblPersonal cp ON c.fkPersonal = cp.pkPersonal
    INNER JOIN dbo.tblRep rep ON c.fkRep = rep.pkRep
    INNER JOIN dbo.tblPersonal repp ON rep.fkPersonal = repp.pkPersonal
    LEFT JOIN dbo.tblBrokerDealer bd ON rep.fkBrokerDealer = bd.pkBrokerDealer
    LEFT JOIN dbo.tblPersonal bdPers ON bd.fkPersonal = bdPers.pkPersonal
    INNER JOIN dbo.tblRegistrationType rt ON r.fkRegistrationType = rt.pkRegistrationType
    INNER JOIN dbo.tblFundFamily ff ON acct.fkFundFamily = ff.pkFundFamily
    INNER JOIN dbo.tblCustodian cust ON acct.fkCustodian = cust.pkCustodian
    INNER JOIN dbo.tblPersonal custp ON cust.fkPersonal = custp.pkPersonal
    INNER JOIN dbo.tblAcctCode vac ON acct.pkAccount = vac.fkAccount
    LEFT JOIN dbo.tblPlatform pf ON acct.fkPlatform = pf.pkPlatform
    LEFT JOIN dbo.tblSubAdvisor sub ON acct.fkSubAdvisor = sub.pkSubAdvisor
    LEFT JOIN dbo.tblPersonal subp ON sub.fkPersonal = subp.pkPersonal
    LEFT JOIN ACCOUNTS_BY_MODELAGG m ON acct.pkAccount = m.[Account ID]
    LEFT JOIN dbo.tblAccountBusinessLine abl ON acct.pkAccount = abl.fkAccount
    LEFT JOIN dbo.tblBusinessLine bl ON abl.fkBusinessLine = bl.pkBusinessLine
    LEFT JOIN dbo.tblPersonal blp ON bl.fkPersonal = blp.pkPersonal
	/*  Test 1  */
    INNER JOIN dbo.tblRegistrationSleeve rs ON acct.fkRegistration = rs.fkRegistration AND rs.IsActive = 1 AND @isSleeveTrade = 'True'
    LEFT JOIN dbo.#sleeves sleeves ON sleeves.fkRegistration = rs.fkRegistration
    LEFT JOIN #AcctCashPerc atc ON atc.fkAccount = acct.pkAccount
    LEFT JOIN dbo.tblDownloadSource ds ON ds.pkDownloadSource = acct.fkDownloadSource
    LEFT JOIN AdvLynx.dbo.tblCustodianCommon cc ON cc.pkCustodianCommon = ds.FkCustodianCommon
	LEFT JOIN dbo.tblClientCat cat ON c.fkClientCat = cat.pkClientCat
WHERE 1 = 1
    AND acct.IsHistorical = 0                                                                          -- we don't want historical accounts and this is accurate per Brian Barsch
    AND acct.AccountStatus <> 5 
	AND (cc.Name NOT LIKE '%FTJ%' OR cc.Name IS NULL)
	AND NOT EXISTS (
			SELECT t.pkAccount, ds.DownloadDesc
			FROM dbo.tblAccount t
			INNER JOIN dbo.tblDownloadSource ds ON ds.pkDownloadSource = t.fkDownloadSource
			WHERE 1 = 1
			AND ds.DownloadDesc = 'AIP SUB - Professional Service'
			AND t.pkAccount = acct.pkAccount)
ORDER BY [AccountNumber]


END
