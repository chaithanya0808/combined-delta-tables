WITH All_Cases_CTE AS (
    -- Case Version
    SELECT CASE_RK, VALID_FROM_DTTM
    FROM ECM.CASE_VERSION CV
    WHERE CV.ORA_ROWSCN BETWEEN {startscn} AND {endscn}

    UNION ALL

    -- Case Live
    SELECT CASE_RK, VALID_FROM_DTTM
    FROM ECM.CASE_LIVE CL
    WHERE CL.ORA_ROWSCN BETWEEN {startscn} AND {endscn}

    UNION ALL

    -- Case UDF Char Value
    SELECT CASE_RK, VALID_FROM_DTTM
    FROM ECM.CASE_UDF_CHAR_VALUE UCV
    WHERE UCV.ORA_ROWSCN BETWEEN {startscn} AND {endscn}

    UNION ALL

    -- Case UDF Date Value
    SELECT CASE_RK, VALID_FROM_DTTM
    FROM ECM.CASE_UDF_DATE_VALUE UCV
    WHERE UCV.ORA_ROWSCN BETWEEN {startscn} AND {endscn}

    UNION ALL

    -- Case UDF Num Value
    SELECT CASE_RK, VALID_FROM_DTTM
    FROM ECM.CASE_UDF_NUM_VALUE UCV
    WHERE UCV.ORA_ROWSCN BETWEEN {startscn} AND {endscn}

    UNION ALL

    -- Case Live Join Case X Party
    SELECT IL.CASE_RK, IL.VALID_FROM_DTTM
    FROM ECM.CASE_LIVE IL
    JOIN ECM.CASE_X_PARTY FA ON IL.CASE_RK = FA.CASE_RK
    WHERE FA.ORA_ROWSCN BETWEEN {startscn} AND {endscn}

    UNION ALL

    -- Case Live Join SAS Comment
    SELECT IL.CASE_RK, IL.VALID_FROM_DTTM
    FROM ECM.CASE_LIVE IL
    JOIN EVMSID.SAS_COMMENT SC ON CAST(IL.CASE_RK AS VARCHAR(20)) = SC.OBJECT_ID
    WHERE SC.OBJECT_TYPE_ID = 545000
    AND SC.ORA_ROWSCN BETWEEN {startscn} AND {endscn}
),
Excluded_Cases_CTE AS (
    -- Cases to be excluded
    SELECT CX.CASE_RK
    FROM ECM.CASE_X_PARTY CX
    WHERE CX.PARTY_RK IN (
        SELECT PARTY_RK
        FROM ECM.PARTY_LIVE PL
        WHERE PL.PARTY_ID IN (
            SELECT 'PTY-' || PARTY_NUMBER
            FROM FCFCORE.FSC_PARTY_DIM FPD
            WHERE FPD.X_PROFILE_NARRATIVE = 'MLC'
            AND FPD.CHANGE_CURRENT_IND = 'Y'
        )
    )
)
SELECT CASE_RK, VALID_FROM_DTTM
FROM All_Cases_CTE
WHERE CASE_RK NOT IN (SELECT CASE_RK FROM Excluded_Cases_CTE);
