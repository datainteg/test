query_string = """
SELECT
    s.uai_id,
    s.acct_id,
    CASE 
        WHEN TRIM(NVL(s.ASif_Attribute, '')) = 'Y' THEN '0015'
        WHEN TRIM(NVL(s.PIFSIF_Flag, '')) = 'Y' THEN '0014'
        WHEN TRIM(NVL(s.BK_Flag, '')) = 'Y' THEN '0013'
        WHEN TRIM(NVL(s.Lit_Flag, '')) = 'Y' THEN '0012'
        WHEN TRIM(NVL(s.Repo_Flag, '')) = 'Y' THEN '0011'
        WHEN TRIM(NVL(s.Deceased_Flag, '')) = 'Y' THEN '0010'
        WHEN TRIM(NVL(s.AR_Flag, '')) = 'Y' THEN '0009'
        WHEN TRIM(NVL(s.CD_Flag, '')) = 'Y' THEN '0008'
        WHEN TRIM(NVL(s.Active_Complaint_Flag, '')) = 'Y' THEN '0007'
        WHEN TRIM(NVL(s.CCCS_Flag, '')) = 'Y' THEN '0006'
        WHEN TRIM(NVL(s.Wisc128_Flag, '')) = 'Y' THEN '0005'
        WHEN TRIM(NVL(s.Dispute_Flag, '')) = 'Y' THEN '0004'
        WHEN TRIM(NVL(s.Fraud_Flag, '')) = 'Y' THEN '0003'
        WHEN TRIM(NVL(s.Mod_Attribute, '')) = 'Y' THEN '0002'
        WHEN TRIM(NVL(s.a3p_dsa_flag, '')) = 'Y' THEN '0016'
        WHEN TRIM(NVL(s.confidential_Flag, '')) = 'Y' THEN '0001'
        ELSE '0000' 
    END::VARCHAR(4) AS Waterfall
FROM
(
    SELECT
        c.*,
        BK.BK_Attribute,
        Complaint.Complaint_Attribute,
        CD.CD_Attribute,
        Fraud.Fraud_Attribute,
        PifSif.PifSif_Attribute,
        ASif.ASif_Attribute,
        AR.AR_Attribute,
        Lit.LIT_Attribute,
        CCCS.CCCS_Attribute,
        Deceased.Deceased_Attribute,
        Modatt.Mod_Attribute,
        conf.confidential_Flag,
        CASE 
            WHEN TRIM(NVL(BK.BK_Attribute::VARCHAR, '')) = 'Y'
                OR TRIM(NVL(c.chapt7_yn::VARCHAR, '')) = 'Y'
                OR TRIM(NVL(c.chapt11_yn::VARCHAR, '')) = 'Y'
                OR TRIM(NVL(c.chapt13_yn::VARCHAR, '')) = 'Y'
            THEN 'Y' ELSE 'N' 
        END AS BK_Flag,
        CASE WHEN TRIM(NVL(Complaint.Complaint_Attribute::VARCHAR, '')) = 'Y' THEN 'Y' ELSE 'N' END AS Active_Complaint_Flag,
        CASE 
            WHEN TRIM(NVL(c.judgement_yn::VARCHAR, '')) = 'Y'
                OR TRIM(NVL(Lit.LIT_Attribute::VARCHAR, '')) = 'Y'
            THEN 'Y' ELSE 'N' 
        END AS Lit_Flag,
        CASE 
            WHEN TRIM(NVL(PifSif.PifSif_Attribute::VARCHAR, '')) = 'Y'
                OR TRIM(NVL(c.acct_status::VARCHAR, '')) = 'P'
            THEN 'Y' ELSE 'N' 
        END AS PIFSIF_Flag,
        CASE WHEN TRIM(NVL(CD.CD_Attribute::VARCHAR, '')) = 'Y' THEN 'Y' ELSE 'N' END AS CD_Flag,
        CASE WHEN TRIM(NVL(Fraud.Fraud_Attribute::VARCHAR, '')) = 'Y' THEN 'Y' ELSE 'N' END AS Fraud_Flag,
        CASE WHEN TRIM(NVL(AR.AR_Attribute::VARCHAR, '')) = 'Y' THEN 'Y' ELSE 'N' END AS AR_Flag,
        CASE 
            WHEN TRIM(NVL(c.cccs_yn::VARCHAR, '')) = 'Y'
                OR TRIM(NVL(CCCS.CCCS_Attribute::VARCHAR, '')) = 'Y'
            THEN 'Y' ELSE 'N' 
        END AS CCCS_Flag,
        CASE WHEN TRIM(NVL(Deceased.Deceased_Attribute::VARCHAR, '')) = 'Y' THEN 'Y' ELSE 'N' END AS Deceased_Flag,
        CASE WHEN TRIM(NVL(wisc.Wisc128_Flag::VARCHAR, '')) = 'Y' THEN 'Y' ELSE 'N' END AS Wisc128_Flag,
        dsa.a3p_dsa_flag
    FROM
    (
        SELECT
            a.*,
            CASE WHEN TRIM(NVL(b.Repo_Attribute::VARCHAR, '')) = 'Y' THEN 'Y' ELSE 'N' END::VARCHAR(1) AS Repo_Flag
        FROM
        (
            SELECT
                Uai_Id,
                acct_id,
                score_br AS branch_n,
                Acct_Status,
                chgoff_date,
                CASE WHEN chgoff_date IS NOT NULL OR TRIM(NVL(acct_status::VARCHAR, '')) = 'C' THEN 'Y' ELSE 'N' END AS Charge_Off_Flag,
                CASE WHEN TRIM(NVL(prod_line::VARCHAR, '')) BETWEEN '1100' AND '1199' THEN 'PHL' ELSE 'PUL' END AS prod_type,
                chapt7_yn,
                chapt11_yn,
                chapt13_yn,
                judgement_yn,
                repo_yn,
                cccs_yn,
                fraud_yn,
                in_dispute_yn,
                CASE WHEN TRIM(NVL(in_dispute_yn::VARCHAR, '')) = 'Y' THEN 'Y' ELSE 'N' END AS Dispute_Flag
            FROM ods.sat_account_daily_management_report_Detail
            WHERE period_end_dt = (CURRENT_DATE - 1)
        ) AS a
        LEFT OUTER JOIN
        (
            SELECT DISTINCT
                uai_id,
                'Y' AS Repo_Attribute
            FROM ods.sat_account_cqs_attribute
            WHERE UPPER(NVL(attribute_desc::VARCHAR, '')) LIKE '%REPOSSESSION%'
                AND TRIM(NVL(attribute_name::VARCHAR, '')) NOT IN ('REPOTURNDN', 'REPOREDEEM', 'REPOSOLD')
        ) AS b
        ON TRIM(NVL(a.uai_id::VARCHAR, '')) = TRIM(NVL(b.uai_id::VARCHAR, ''))
    ) AS c
    LEFT OUTER JOIN
    (
        SELECT DISTINCT
            uai_id,
            'Y' AS BK_Attribute
        FROM ods.sat_account_cqs_attribute
        WHERE UPPER(NVL(attribute_desc::VARCHAR, '')) LIKE '%BANK%'
            AND NVL(attribute_name::VARCHAR, '') NOT LIKE '%SCRUBFULL%'
    ) AS BK
    ON TRIM(NVL(c.uai_id::VARCHAR, '')) = TRIM(NVL(BK.uai_id::VARCHAR, ''))
    LEFT OUTER JOIN
    (
        SELECT DISTINCT
            uai_id,
            'Y' AS Complaint_Attribute
        FROM ods.sat_account_cqs_attribute
        WHERE UPPER(NVL(attribute_desc::VARCHAR, '')) LIKE '%ACTIVE COMPLAINT%'
    ) AS Complaint
    ON TRIM(NVL(c.uai_id::VARCHAR, '')) = TRIM(NVL(Complaint.uai_id::VARCHAR, ''))
    LEFT OUTER JOIN
    (
        SELECT DISTINCT
            uai_id,
            'Y' AS CD_Attribute
        FROM ods.sat_account_cqs_attribute
        WHERE UPPER(NVL(attribute_name::VARCHAR, '')) LIKE 'CEASE_DST%'
            OR NVL(attribute_name::VARCHAR, '') LIKE '%CANDDREV%'
    ) AS CD
    ON TRIM(NVL(c.uai_id::VARCHAR, '')) = TRIM(NVL(CD.uai_id::VARCHAR, ''))
    LEFT OUTER JOIN
    (
        SELECT DISTINCT
            uai_id,
            'Y' AS Fraud_Attribute
        FROM ods.sat_account_cqs_attribute
        WHERE UPPER(NVL(attribute_desc::VARCHAR, '')) LIKE '%FRAUD%'
    ) AS Fraud
    ON TRIM(NVL(c.uai_id::VARCHAR, '')) = TRIM(NVL(Fraud.uai_id::VARCHAR, ''))
    LEFT OUTER JOIN
    (
        SELECT DISTINCT
            uai_id,
            'Y' AS PifSif_Attribute
        FROM ods.sat_account_cqs_attribute
        WHERE UPPER(NVL(attribute_desc::VARCHAR, '')) LIKE '%PIF%'
            OR UPPER(NVL(attribute_desc::VARCHAR, '')) LIKE '%SIF%'
    ) AS PifSif
    ON TRIM(NVL(c.uai_id::VARCHAR, '')) = TRIM(NVL(PifSif.uai_id::VARCHAR, ''))
    LEFT OUTER JOIN
    (
        SELECT DISTINCT
            uai_id,
            active_sif_i AS ASif_Attribute
        FROM ods.sat_account_daily_class_extension
        WHERE TRIM(NVL(active_sif_i::VARCHAR, '')) = 'Y'
            AND ods_update_dt = CURRENT_DATE
    ) AS ASif
    ON TRIM(NVL(c.uai_id::VARCHAR, '')) = TRIM(NVL(ASif.uai_id::VARCHAR, ''))
    LEFT OUTER JOIN
    (
        SELECT DISTINCT
            uai_id,
            'Y' AS AR_Attribute
        FROM ods.sat_account_cqs_attribute
        WHERE UPPER(NVL(attribute_desc::VARCHAR, '')) LIKE 'ATTO%'
            OR NVL(attribute_name::VARCHAR, '') LIKE '%ARREVIEW%'
    ) AS AR
    ON TRIM(NVL(c.uai_id::VARCHAR, '')) = TRIM(NVL(AR.uai_id::VARCHAR, ''))
    LEFT OUTER JOIN
    (
        SELECT DISTINCT
            uai_id,
            'Y' AS LIT_Attribute
        FROM ods.sat_account_cqs_attribute
        WHERE UPPER(NVL(attribute_desc::VARCHAR, '')) LIKE '%LITIGATION%'
            AND UPPER(NVL(attribute_name::VARCHAR, '')) NOT LIKE '%LITTURNDN%'
    ) AS LIT
    ON TRIM(NVL(c.uai_id::VARCHAR, '')) = TRIM(NVL(LIT.uai_id::VARCHAR, ''))
    LEFT OUTER JOIN
    (
        SELECT DISTINCT
            uai_id,
            'Y' AS CCCS_Attribute
        FROM ods.sat_account_cqs_attribute
        WHERE UPPER(NVL(attribute_desc::VARCHAR, '')) LIKE '%CCCS%'
    ) AS CCCS
    ON TRIM(NVL(c.uai_id::VARCHAR, '')) = TRIM(NVL(CCCS.uai_id::VARCHAR, ''))
    LEFT OUTER JOIN
    (
        SELECT DISTINCT
            uai_id,
            'Y' AS Mod_Attribute
        FROM ods.sat_account_cqs_attribute
        WHERE TRIM(UPPER(NVL(attribute_name::VARCHAR, ''))) IN ('M0', 'M1', 'M2', 'M3', 'M4')
    ) AS Modatt
    ON TRIM(NVL(c.uai_id::VARCHAR, '')) = TRIM(NVL(Modatt.uai_id::VARCHAR, ''))
    LEFT OUTER JOIN
    (
        SELECT DISTINCT
            uai_id,
            'Y' AS Deceased_Attribute
        FROM ods.sat_account_cqs_attribute
        WHERE UPPER(NVL(attribute_desc::VARCHAR, '')) LIKE '%DECEASED%'
            OR NVL(attribute_name::VARCHAR, '') LIKE '%DECREVIEW%'
    ) AS Deceased
    ON TRIM(NVL(c.uai_id::VARCHAR, '')) = TRIM(NVL(Deceased.uai_id::VARCHAR, ''))
    LEFT OUTER JOIN
    (
        SELECT DISTINCT
            b.uai_id,
            'Y' AS confidential_Flag
        FROM
        (
            SELECT
                account_key,
                adv_rcpt_cd
            FROM ods.sat_account_daily_management_report_detail
            WHERE period_end_dt = DATE(DATE_TRUNC('MONTH', CURRENT_DATE) - INTERVAL '1 DAY')
                AND TRIM(NVL(adv_rcpt_cd::VARCHAR, '')) = 'C'
        ) AS a
        INNER JOIN
        (
            SELECT
                uai_id,
                account_key
            FROM ods.sat_account_daily_management_report_detail
            WHERE period_end_dt = DATE(DATE_TRUNC('MONTH', CURRENT_DATE) - INTERVAL '1 DAY')
        ) AS b
        ON TRIM(NVL(a.account_key::VARCHAR, '')) = TRIM(NVL(b.account_key::VARCHAR, ''))
    ) AS conf
    ON TRIM(NVL(c.uai_id::VARCHAR, '')) = TRIM(NVL(conf.uai_id::VARCHAR, ''))
    LEFT OUTER JOIN
    (
        SELECT
            uai_id,
            wisconsin_128_i AS Wisc128_Flag
        FROM ods.sat_account_daily_class_master
        WHERE TRIM(NVL(wisconsin_128_i::VARCHAR, '')) = 'Y'
            AND load_dt = CURRENT_DATE - 1
    ) AS wisc
    ON TRIM(NVL(c.uai_id::VARCHAR, '')) = TRIM(NVL(wisc.uai_id::VARCHAR, ''))
    LEFT OUTER JOIN
    (
        SELECT
            x.account_n,
            x.branch_n,
            x.a3p_dsa_flag,
            x.a3p_name::VARCHAR(100) AS DSA_Name,
            DATE(x.chg_timestamp) AS Dsa_Date,
            x.a3p_received_date,
            x.a3p_expiration_date,
            DATE(x.add_timestamp) AS add_Date,
            DATE(x.dsaovr_timestamp) AS Dsa_Ovrrde_Date,
            x.dsa_rev_ovr_date AS Override_Expiration_Date,
            x.a3p_poa_flag
        FROM dialer.csa3pt00 AS x
        INNER JOIN
        (
            SELECT
                account_n,
                MAX(chg_timestamp) AS max_ts
            FROM dialer.csa3pt00
            WHERE TRIM(NVL(a3p_dsa_flag::VARCHAR, '')) = 'Y'
            GROUP BY account_n
        ) AS y
        ON x.account_n = y.account_n AND x.chg_timestamp = y.max_ts
        WHERE TRIM(NVL(x.a3p_dsa_flag::VARCHAR, '')) = 'Y'
    ) AS Dsa
    ON TRIM(NVL(c.acct_id::VARCHAR, '')) = TRIM(NVL(dsa.account_n::VARCHAR, ''))
) AS s
"""
