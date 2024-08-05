SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/* =============================================
   Author:        MJepsen
   Create date: 10/08/2014
   Description:   Extracts the data for the data warehouse table dimAccount
   Change history:
                  P044443 02/04/2015  MJepsen     Added 3 new columns
                  P048161 04/17/2015  MJepsen     Replaced logic for address to pick it up from client if it doesn't exist on registration
                                                  Renamed MinimumInceptionDate to AccountInceptionDate and added HouseholdInceptionDate
                  P048166 04/17/2015  MJepsen     Added SubAdvisorName
                  P050694 05/13/2015  MJepsen     Added "Unknown" for null fkRep
                  P050694 05/13/2015  MJepsen     Updated BirthMonth logic to handle invalid dates (source column is varchar)
                  P050694 05/15/2015  MJepsen     Changed AccountStartMonth from computed to a column we supply to SSIS
                  P053124 07/31/2015  MJepsen     Added columns: IncludeInBusinessMetrics, AccountTypeDescription, CancelDate
                  P059443 09/29/2015  MJepsen     Added check to see if tblPlatform, tblRegistrationType or tblCustodian changed
                  P070153 03/29/2016  MJepsen     Removed regpers from address logic (since it isn't displayed in OC and users can't edit it)
                  P072529 07/29/2016  ERubeck     Added XML for key-value pairs for Custom Fields (dbo.tblUserDefineDef.Type = 0 (Lists) only!)
                  P080540 12/30/2016  MJepsen     Added IsIRA column from tblRegistration
                  P108440 02/01/2018  MJepsen     Added checkbox custom fields and removed change data capture WHERE clause because we were missing some changes
                  P108440 2018-02-13  ERubeck     Moved left joins on derived tables to temp tables for SQL Server 2016 optimization, removed complexity of #Return   
                  P096820 03/02/2018  MJepsen     Added IsHistorical and Country to extract
                  P110419 03/05/2018  MJepsen     Added default for custom fields so we don't have a NULL CustomFieldList (empty JSON is {})
                  P096820 03/26/2018  MJepsen     Added default value for Country field
                  P110423 04/18/2018  MPope       Added in DownloadVendorName and DownloadVendorID columns, IsDiscretionary
                  P110423 05/08/2018  MJepsen     Added order by clause to customfieldlist because it was creating type2 changes unnecessarily
                  P118717 10/05/2018  MPope       add in InterfaceID column
                    DO 7859 03/16/2020  CCleav      Add column FundFamilyID
                    DO13221 03/16/2020  CCleav      Add columns IsAdvReportable, IsAusReportable, Is13FReportable
                DevOps16334 05/28/2020  MJepsen     Updated for communities data 
                DevOps18621 05/08/2020  MJepsen     Updated method to get account number
                DevOps21201 06/11/2020  MJesen      Resolved duplicates introduced by communities joins
                DevOps21370 06/18/2020  ccleave     Resolve duplicates for AccountID with more than one ModelAssigned
                DevOps16334 07/07/2020  ccleave     Changing Communities requirements. Avoid duplicates and grab Max CommunityModel and Strategiest info
                DevOps16334 07/17/2020  MJepsen     Removed all but SubAdvisorID from communities columns due to multiples not in original requirements
              BIDevOps25636 07/27/2020  ccleave     Add SleeveAccountID
                            07/29/2020  ccleave     Changed logic for SleeveAccountID to use the dbo.tblAcctCode table instead of vwAssetTable
              BIDevOps36616 10/29/2020  ccleabv     Changed #Sleeve logic to get Min(Sleeve) 
							BIDevOps60298 05/12/2021  JDuke     Fixed bug where HouseholdInceptionDate was showing inception date for Registration 
							BIDevOps73813 07/15/2021  JDuke     Changed logic for CustomFieldList
							BIDevOps73813 07/26/2021  JDuke     Changed length of fieldvalue
							BIDevOps64092 03/08/2022  JDuke     Added StrategistTier column with logic from Gene Frerichs
							BIDevOps18848412/07/2022	EFields		Added ModelID column - request from Aayush Khadka
							BIDevOps239730	08/18/2023	EFields	Added IsERISA column to output
							BIDevOps239526	08/21/2023	EFields Added EclipseFirmID to output
							BIDevOps247452	10/23/2023	JDuke	Added IsAstroOptimized to output
              JIRA BI-1557    03/20/2024  EFields Added a LEFT to the #Return temp table to avoid data length failures when buildilng the temp table
							JIRA BI-1787		06/18/2024	EFields	Added in two new fields AstroEnabledDate, EndingMarketValueAsofAstroEnabledDate; also changed logic for EclipseFirmID (to match Ben Bedell's logic)
==============================================*/
CREATE OR ALTER PROCEDURE [dw].[spDWExtractdimAccount]
    @LastLoadDate DATETIME = '01/01/1900' -- this parameter is no longer used
AS
BEGIN
---- for testing
--DECLARE @LastLoadDate DATETIME = '06/01/2024' -- parameter no longer used

    SET NOCOUNT ON;

    DECLARE @Server varchar (128)

    IF @@ServerName IN ('SQLCOMMONAG7501','SQLCOMMONAG7502','SQLCommonAG3','SQLCommonAG4')
        BEGIN
            SET @Server = 'COMMONDB'
        END
    ELSE
        BEGIN
            SET @Server = @@SERVERNAME
        END
        
    IF (SELECT OBJECT_ID('tempdb..#CustomFields')) IS NOT NULL
        DROP TABLE #CustomFields
    IF (SELECT OBJECT_ID('tempdb..#FieldList')) IS NOT NULL
        DROP TABLE #FieldList
    IF (SELECT OBJECT_ID('tempdb..#Return')) IS NOT NULL
        DROP TABLE #Return
    IF (SELECT OBJECT_ID('tempdb..#BusLine')) IS NOT NULL
        DROP TABLE #BusLine
    IF (SELECT OBJECT_ID('tempdb..#InceptionDateAsset')) IS NOT NULL
        DROP TABLE #InceptionDateAsset
    IF (SELECT OBJECT_ID('tempdb..#InceptionDateAccount')) IS NOT NULL
        DROP TABLE #InceptionDateAccount
    IF (SELECT OBJECT_ID('tempdb..#InceptionDateCli')) IS NOT NULL
        DROP TABLE #InceptionDateCli
	IF (SELECT OBJECT_ID('tempdb..#StrategistTier')) IS NOT NULL
        DROP TABLE #StrategistTier

    CREATE TABLE #Return (
        pkAccount INT,
        CustomFields VARCHAR(8000) -- even though target is varchar(max), SSIS can only handle 8000 in a cache connection manager
    )

    CREATE TABLE #BusLine (
        pkAccount INT,
        BusinessLine VARCHAR(255)
    )

    CREATE TABLE #InceptionDateAsset (
        fkAccount INT,
        AcctCode VARCHAR(50),
        InitialPurchaseDate DATETIME
    )

    CREATE TABLE #InceptionDateAccount (
        fkAccount INT,
        MinimumInceptionDate DATETIME
    )

    CREATE TABLE #InceptionDateCli (
        fkClient INT,
        MinimumInceptionDate DATETIME
    )

    CREATE TABLE #CustomFields (
        FieldLevel VARCHAR(50),
        pkAccount INT,
        pkUserDefineDef INT,
        FieldCode VARCHAR(25),
        FieldValue VARCHAR(8000) 
    )

    CREATE TABLE #Sleeve(
        SleeveAccountID INTEGER,
        SleeveAcctCode VARCHAR(50),
        SleevedAccountID INTEGER,
        SleevedAcctCode VARCHAR(50)

    )

	CREATE TABLE #StrategistTier(
        pkSubAdvisor INTEGER,
        StrategistTier INTEGER

    )

	CREATE TABLE #AstroOptimized(
        fkAccount INTEGER,
        isAstroOptimized VARCHAR(10)
	)

    INSERT INTO #Sleeve
    (
        SleeveAccountID,
        SleeveAcctCode,
        SleevedAccountID,
        SleevedAcctCode
    )
    SELECT distinct SleeveAccountID = sleeve.fkAccount 
        , SleeveAcctCode = sleeve.AcctCode
        , SleevedAccountID = sleeved.fkAccount
        , SleevedAcctCode = sleeved.AcctCode
    FROM 
    (
        SELECT fkAccount = MIN(sl.fkAccount), sl.SleeveType , sa.AcctCode
        FROM dbo.tblAccountSleeve sl 
        INNER JOIN [dbo].[tblAcctCode] sa ON sa.fkAccount = sl.fkAccount
        WHERE sl.SleeveType = 1
        GROUP BY sl.SleeveType , sa.AcctCode
        ) sleeve
            INNER JOIN 
            (
            SELECT sl.fkAccount, sl.SleeveType , sa.AcctCode
                , AcctCodeDropSuffix = CASE 
                                        WHEN CHARINDEX('_',sa.AcctCode) >=1 THEN SUBSTRING(sa.AcctCode, 1, CHARINDEX('_',sa.AcctCode) - 1)
                                        ELSE sa.AcctCode
                                       END 
            FROM dbo.tblAccountSleeve sl 
            INNER JOIN [dbo].[tblAcctCode] sa ON sa.fkAccount = sl.fkAccount
            WHERE sl.SleeveType = 0
            ) sleeved ON sleeved.AcctCodeDropSuffix = sleeve.AcctCode


    INSERT #CustomFields ( FieldLevel ,
                                pkAccount ,
                                pkUserDefineDef ,
                                FieldCode ,
                                FieldValue )
    SELECT FieldLevel = CAST('Account' AS VARCHAR(50)),
       pkAccount = udf.fkAccount,
       pkUserDefineDef = udf.fkUserDefineDef,
       Code = COALESCE(udf.Code, ''),
       FieldValue = COALESCE(udf.Value, '')
FROM dbo.vwUserDefinedFieldsForAccount udf
WHERE udf.Type IN ( 0, 1 )

UNION ALL

SELECT FieldLevel = CAST('Registration' AS VARCHAR(50)),
       pkAccount = ac.pkAccount,
       pkUserDefineDef = udf.fkUserDefineDef,
       Code = COALESCE(udf.Code, ''),
       FieldValue = COALESCE(udf.Value, '')
FROM dbo.vwUserDefinedFieldsForRegistration udf
JOIN dbo.tblAccount ac ON ac.fkRegistration = udf.fkRegistration
WHERE udf.Type IN ( 0, 1 )

UNION ALL

SELECT FieldLevel = CAST('Client' AS VARCHAR(50)),
       pkAccount = ac.pkAccount,
       pkUserDefineDef = udf.fkUserDefineDef,
       Code = COALESCE(udf.Code, ''),
       FieldValue = COALESCE(udf.Value, '')
FROM dbo.vwUserDefinedFieldsForClient udf
JOIN dbo.tblRegistration reg ON reg.fkClient = udf.fkClient
JOIN dbo.tblAccount ac ON ac.fkRegistration = reg.pkRegistration
WHERE udf.Type IN ( 0, 1 )

    INSERT #InceptionDateCli ( fkClient ,
                                    MinimumInceptionDate )
    SELECT fkClient, MinimumInceptionDate = COALESCE(CAST(MIN(i.InceptionDate) AS DATETIME), MIN(acct.AcctStartDate))
    FROM dbo.tblAsset a
    JOIN dbo.tblAccount acct ON a.fkAccount = acct.pkAccount
	JOIN dbo.tblRegistration reg ON reg.pkRegistration = acct.fkRegistration
    LEFT JOIN dbo.tblAssetInceptionCloseDates i ON i.fkAsset = a.pkAsset
    GROUP BY fkClient

    INSERT #InceptionDateAccount ( fkAccount ,
                                        MinimumInceptionDate )
    SELECT fkAccount, MinimumInceptionDate = CAST(MIN(i.InceptionDate) AS DATETIME)
    FROM dbo.tblAsset a 
    LEFT JOIN dbo.tblAssetInceptionCloseDates i ON i.fkAsset = a.pkAsset
    GROUP BY fkAccount

    INSERT #InceptionDateAsset ( fkAccount ,
                                 AcctCode ,
                                 InitialPurchaseDate )
    SELECT a.fkAccount, AcctCode = MAX(a.AcctCode), InitialPurchaseDate = CAST(MIN(InceptionDate) AS DATETIME)
    FROM dbo.tblAsset a
    LEFT OUTER JOIN dbo.tblAssetInceptionCloseDates ai ON ai.fkAsset = a.pkAsset
    GROUP BY a.fkAccount

    INSERT #BusLine ( pkAccount ,
                           BusinessLine )
    SELECT acctBusLine.fkAccount, persBusLine.EntityName
    FROM dbo.tblAccountBusinessLine acctBusLine 
        INNER JOIN dbo.tblBusinessLine busline ON acctBusLine.FkBusinessLine = busline.PkBusinessLine
        INNER JOIN dbo.tblPersonal persBusLine ON busline.FkPersonal = persBusLine.PkPersonal

    -- finally, get the final JSON put together that will be stored in dimAccount for parsing
    INSERT #Return ( pkAccount, CustomFields )
    SELECT b.pkAccount,
            LEFT((SELECT a.FieldLevel AS 'level',
                    a.pkUserDefineDef AS 'pkUserDefineDef', 
                    a.FieldCode AS 'code', 
                    a.FieldValue AS 'value'                     
            FROM #CustomFields a
            WHERE a.pkAccount = b.pkAccount
            ORDER BY a.FieldLevel, a.pkUserDefineDef
            FOR JSON PATH), 8000) CustomFields
    FROM (SELECT DISTINCT pkAccount FROM #CustomFields) b

	INSERT INTO #StrategistTier
	(
	    pkSubAdvisor,
	    StrategistTier
	)
	 
    
    SELECT sa.pkSubAdvisor, COALESCE(dat.FieldValue, def.DefaultValue) AS [StrategistTier]
FROM dbo.tblSubAdvisor sa
JOIN dbo.tblUserDefineDef def
ON def.Code = '52STRATEGISTTIER' AND def.EntityEnum = 52
LEFT JOIN dbo.tblUserDefineData dat
ON dat.fkUserDefineDef = def.pkUserDefineDef
AND dat.fkParent = sa.pkSubAdvisor
LEFT JOIN dbo.tblPersonal saPers
ON saPers.pkPersonal = sa.fkPersonal

	INSERT INTO #AstroOptimized
	(
		fkAccount,
		isAstroOptimized
	)

	SELECT defdata.fkParent AS fkAccount
      ,defdata.FieldValue
  FROM dbo.tblUserDefineData defdata
  JOIN dbo.tblUserDefineDef def ON def.pkUserDefineDef = defdata.fkUserDefineDef
  WHERE def.Description = 'Astro Optimized Account'
  AND defdata.FieldValue = 'True'
  AND defdata.EntityEnum = 7 --Account

CREATE TABLE #EclipseFirmID
(
	pkAccount INT,
	EclipseFirmID INT
)
INSERT INTO #EclipseFirmID
SELECT acct.pkaccount, COALESCE( sa.EclipseFirmId, rs.EclipseFirmId, acct.EclipseFirmId ) AS EclipseFirmID
FROM 
		dbo.tblAccount acct
		LEFT JOIN dbo.tblsubadvisor sa ON acct.fksubadvisor = sa.pksubadvisor
		LEFT JOIN dbo.tblRegistrationSleeve rs ON acct.fkRegistration = rs.fkRegistration

CREATE TABLE #OCI
(
	ClientName nvarchar(255),
	FkAccount INT,
	Calc_Date DATE,
	CashFlowAmount DECIMAL(18,2),
	EndMarketValue DECIMAL(18,2),
	IsAstroEnabled BIT,
	ActionTaken nvarchar(255),
	OCIType nvarchar(255)
)


DECLARE @BegDate DATE = @LastLoadDate, @EndDate DATE = cast(getdate() as date)
DECLARE @BegDate2 DATE = dateadd(day,-1,@LastLoadDate)
IF @LastLoadDate <= '06/18/2024' OR @LastLoadDate IS NULL
	INSERT INTO #OCI
	EXEC [dw].[spDWOrionCustomIndexingCashFlowReport_AlldB]  @BegDate, @EndDate
ELSE
	INSERT INTO #OCI
	EXEC [dw].[spDWOrionCustomIndexingCashFlowReport_AlldB] @BegDate2, @LastLoadDate;


    SELECT 
        -- for key lookups
        DatabaseName = CAST(DB_NAME() AS VARCHAR(255))
        , ServerName = CAST(@Server AS VARCHAR(255))
        , fkRep = COALESCE(c.fkRep, -1)
        , ClientID = c.pkClient
        , ClientThirdPartyIdentifier = ISNULL(CAST(alcli.[GUID] AS VARCHAR(255)), '_N/A')
        , HouseholdLastName = clipers.LName
        , HouseholdCategoryCode = ISNULL(cc.Name, '?Unknown')
        , HouseholdCategoryName = ISNULL(cc.[Description], '?Unknown')
        , RegistrationId = acct.fkRegistration
        , RegistrationLastName = regpers.LName
        , AccountId = acct.pkAccount
        , AccountThirdPartyIdentifier = ISNULL(acct.OutsideID, '_N/A')
        , BirthMonth = CASE WHEN ISDATE(regpers.DOB) = 1 THEN ISNULL(DATEADD(dd,-(DAY(CAST(regpers.DOB AS DATETIME))-1),CAST(regpers.DOB AS DATETIME)), '01/01/1900') ELSE '01/01/1900' END
        , [State] = CAST(COALESCE(NULLIF(clipers.[State], ''),'?Unknown') AS VARCHAR(10))
        , ZIP = COALESCE(NULLIF(clipers.ZIP, ''),'?Unknown')
        , AccountNumber = CASE
                            WHEN COALESCE(acctcode.AcctCode, '?Unknown') = '?Unknown' THEN '?Unknown'
                            WHEN COALESCE(acctcode.AcctCode, '') = '' THEN '?Unknown'
                            ELSE RIGHT(acctcode.AcctCode, 3) -- only store the right 3 characters in the warehouse on purpose: NO PII
                        END
        , AccountType = rt.sRegCode
        , AccountStartDate = ISNULL(asset.InitialPurchaseDate, '1/1/1900')
        , AccountStartMonth = CASE WHEN asset.InitialPurchaseDate IS NULL THEN '01/01/1900' ELSE DATEADD(dd,-(DAY(asset.InitialPurchaseDate)-1),asset.InitialPurchaseDate) END
        , AccountStartValue = ISNULL(acct.AcctStartValue, 0.00)
        , FeeSchedule = ISNULL(bs.sSchedule, '?Unknown')
        , Custodian = ISNULL(cust.Name, '?Unknown')
        , ModelID = ISNULL(mag.pkModelAgg, -2)
        , Model = ISNULL(mag.AggregationName, '_N/A')
        , ManagementStyle = ISNULL(plat.Name, '?Unknown')
        , FundFamilyName = ISNULL(ff.FundName, '?Unknown')
        , ShareClass = ISNULL(sc.Class, '?Unknown')
        , ShareClassDescription = ISNULL(sc.[Description], '?Unknown') 
        , PlanNumber = ISNULL(pl.PlanNo, '_N/A')
        , PlanName = ISNULL(pl.PlanName, '_N/A')
        , PlanSponsor = ISNULL(pl.PlanSponsor, '_N/A')
        , AccountStatus = stat.Status
        , BusinessLine = ISNULL(busLine.BusinessLine, '?Unknown')
        , IsTradingBlocked = CASE acct.IsTradingBlocked
                                WHEN 1 THEN 'Trading Blocked'
                                ELSE 'Trading Not Blocked'
                            END
        , IsManaged = CASE acct.IsManaged
                            WHEN 1 THEN 'Managed'
                            ELSE 'Unmanaged'
                        END
        , IsQualified = CASE rt.IsQual
                            WHEN 1 THEN 'Qualified'
                            ELSE 'Not Qualified'
                        END
        , IsAnnuity = CASE ff.IsAnnuity
                            WHEN 1 THEN 'Annuity'
                            ELSE 'Not Annuity'
                        END
        , IsActive = CASE acct.IsActive 
                        WHEN 1 THEN 'Active' 
                        ELSE 'Inactive' 
                    END
        , acct.CreatedDate
        , ManagementStyleID = COALESCE(acct.fkPlatform, -1)
        , AccountInceptionDate = COALESCE(i.MinimumInceptionDate, acct.AcctStartDate, '01/01/1900')
        , HouseholdInceptionDate = COALESCE(hi.MinimumInceptionDate, '01/01/1900')
        , SubAdvisorName = COALESCE(sap.EntityName, '_N/A')
        , IncludeInBusinessMetrics = CAST(CASE stat.IncludeInBusMetrics 
                                        WHEN 1 THEN 'Include in Business Metrics'
                                        WHEN 0 THEN 'Not Included in Business Metrics'
                                        ELSE '?Unknown'
                                        END AS VARCHAR(50))
        , AccountTypeDescription = COALESCE(rt.sRegDesc, '?Unknown')
        , CancelDate = COALESCE(acct.CancelDate, '01/01/1900')
        , CustomFieldList = COALESCE(LEFT(clist.CustomFields, 8000), '{}')
        , IsIRA = CASE rt.IsIRA 
                        WHEN 1 THEN 'IRA Account'
                        ELSE 'Not IRA Account'
                    END
				, IsERISA
        , IsHistorical = CASE WHEN acct.IsHistorical = 1 THEN 'Historical' ELSE 'Not Historical' END
        , Country = COALESCE(regPers.Country, 'United States of America')
        , DownloadVenderName = COALESCE(custcom.Name, '_N/A')
        , DownloadVenderID = COALESCE(custcom.pkCustodianCommon, -2)
        , IsDiscretionary = CASE WHEN acct.IsDiscretionary = 1 THEN 'Discretionary' ELSE 'Not Discretionary' END
        , InterfaceID = COALESCE(acct.fkDownloadSource, -2)
        , FundFamilyID = ISNULL(ff.pkFundFamily, -1)
        , IsADVReportable = CASE WHEN acct.IsADVReportable = 1 THEN 'ADV Reportable' ELSE 'Not ADV Reportable' END  
        , IsAUAReportable = CASE WHEN acct.IsAUAReportable = 1 THEN 'AUA Reportable' ELSE 'Not AUA Reportable' END  
        , Is13FReportable = CASE WHEN acct.Is13FReportable = 1 THEN '13F Reportable' ELSE 'Not 13F Reportable' END
        , SubAdvisorID = COALESCE(sa.pkSubAdvisor, -2)
				, StrategistTier = COALESCE(st.StrategistTier,-2)
        , SleeveAccountID = COALESCE(spc.SleeveAccountID, -2)
				--, EclipseFirmID = sa.EclipseFirmID
				, EclipseFirmID = ef.EclipseFirmID
				, IsAstroOptimized = CASE WHEN ao.isAstroOptimized = 'True' THEN 1 ELSE 0 END
				, AstroEnabledDate = oci.Calc_Date
				, EndingMarketValueAsofAstroEnabledDate = oci.EndMarketValue
    FROM dbo.tblAccount acct
    INNER JOIN dbo.tblRegistration r ON acct.fkRegistration = r.pkRegistration
    INNER JOIN dbo.tblPersonal regPers ON r.fkPersonal = regPers.pkPersonal
    JOIN dbo.tblClient c ON r.fkClient = c.pkClient
    JOIN dbo.tblPersonal clipers ON c.fkPersonal = clipers.pkPersonal   
    LEFT JOIN dbo.tblFundFamily ff ON acct.fkFundFamily = ff.pkFundFamily
    LEFT JOIN dbo.tblRegistrationType rt ON r.fkRegistrationType = rt.pkRegistrationType
    LEFT JOIN dbo.tblCustodian cust ON acct.fkCustodian = cust.pkCustodian
    LEFT JOIN dbo.tblShareClass sc ON acct.fkShareClass = sc.pkShareClass
    LEFT JOIN dbo.tblPlatform plat ON acct.fkPlatform = plat.pkPlatform
    LEFT JOIN dbo.vwimPlan pl ON acct.fkPlan = pl.pkPlan
    LEFT JOIN dbo.tblAcctCode acctcode ON acct.pkAccount = acctcode.fkAccount -- new method for getting account code
    LEFT JOIN #BusLine busLine ON acct.pkAccount = busLine.pkAccount
    LEFT JOIN #InceptionDateAsset asset ON acct.pkAccount = asset.fkAccount
    LEFT JOIN AdvLynx.dbo.tblALClient alcli ON c.pkClient = alcli.pkALClient
    LEFT JOIN dbo.tblClientCat cc ON c.fkClientCat = cc.pkClientCat
    LEFT JOIN dbo.tblBillAccount ba ON acct.pkAccount = ba.fkAccount
    LEFT JOIN dbo.tblBillSchedule bs ON ba.fkBillFeeSchedule = bs.pkBillSchedule
    LEFT JOIN dbo.tblMdlAccount ma ON acct.pkAccount = ma.fkAccount
    LEFT JOIN dbo.tblModelAgg mag ON ma.fkModelAgg = mag.pkModelAgg
    LEFT JOIN dbo.tblAccountStatus stat ON acct.AccountStatus = stat.pkAccountStatus
    LEFT JOIN #InceptionDateAccount i ON i.fkAccount = acct.pkAccount
    LEFT JOIN #InceptionDateCli hi ON c.pkClient = hi.fkClient
    LEFT JOIN dbo.tblSubAdvisor sa ON sa.pkSubAdvisor = acct.fkSubAdvisor 
    LEFT JOIN dbo.tblPersonal sap ON sap.pkPersonal = sa.fkPersonal 
    LEFT JOIN #Return clist ON clist.pkAccount = acct.pkAccount
    LEFT JOIN dbo.tblDownloadSource ds ON ds.pkDownloadSource = acct.fkDownloadSource
    LEFT JOIN AdvLynx.dbo.tblCustodianCommon custcom ON custcom.pkCustodianCommon = ds.FkCustodianCommon
    LEFT JOIN #Sleeve spc ON spc.SleevedAccountID = acct.pkAccount
		LEFT JOIN #StrategistTier st ON sa.pkSubAdvisor = st.pkSubAdvisor
		LEFT JOIN #AstroOptimized ao ON acct.pkAccount = ao.fkAccount
		LEFT JOIN #OCI oci ON acct.pkaccount = oci.fkaccount AND oci.ActionTaken = 'Enrolled'
		LEFT JOIN #EclipseFirmID ef ON acct.pkaccount = ef.pkaccount
END