DROP TABLE IF EXISTS analytics_hdhi_clean;

CREATE TABLE analytics_hdhi_clean AS
WITH cleaned AS (
    SELECT
        -- === DATE CLEANING (supports "-" and "/") ===
        CASE
            WHEN "D.O.A" ~ '^\d{1,2}-\d{1,2}-\d{4}$'
                THEN to_date("D.O.A", 'DD-MM-YYYY')
            WHEN "D.O.A" ~ '^\d{1,2}/\d{1,2}/\d{4}$'
                 AND split_part("D.O.A", '/', 1)::int <= 12
                THEN to_date("D.O.A", 'MM/DD/YYYY')   -- Only if first part is <= 12
            WHEN "D.O.A" ~ '^\d{1,2}/\d{1,2}/\d{4}$'
                THEN to_date("D.O.A", 'DD/MM/YYYY')   -- Otherwise assume day/month
            ELSE NULL
        END AS doa_date,

        CASE
            WHEN "D.O.D" ~ '^\d{1,2}-\d{1,2}-\d{4}$'
                THEN to_date("D.O.D", 'DD-MM-YYYY')
            WHEN "D.O.D" ~ '^\d{1,2}/\d{1,2}/\d{4}$'
                 AND split_part("D.O.D", '/', 1)::int <= 12
                THEN to_date("D.O.D", 'MM/DD/YYYY')
            WHEN "D.O.D" ~ '^\d{1,2}/\d{1,2}/\d{4}$'
                THEN to_date("D.O.D", 'DD/MM/YYYY')
            ELSE NULL
        END AS dod_date,

        -- Demographics
        "SNO" AS sno,
        "MRD No." AS mrd_no,
        "AGE" AS age,
        "GENDER" AS gender,
        "RURAL" AS is_rural,

        -- Admission info
        "TYPE OF ADMISSION-EMERGENCY/OPD" AS admission_type,
        "month year" AS month_year,
        "DURATION OF STAY" AS stay_duration,
        "duration of intensive unit stay" AS icu_stay_duration,
        "OUTCOME" AS outcome,

        -- Lifestyle / comorbidities (fixed 1 trailing space)
        "SMOKING " AS smoking,
        "ALCOHOL" AS alcohol,
        "DM" AS diabetes,
        "HTN" AS hypertension,
        "CAD" AS cad,
        "PRIOR CMP" AS prior_cmp,
        "CKD" AS ckd,

        -- Lab values (clean)
        NULLIF(NULLIF(trim("HB"), 'EMPTY'), '')::numeric AS hb,
        NULLIF(NULLIF(trim("TLC"), 'EMPTY'), '')::numeric AS tlc,
        NULLIF(NULLIF(trim("PLATELETS"), 'EMPTY'), '')::numeric AS platelets,
        NULLIF(NULLIF(trim("GLUCOSE"), 'EMPTY'), '')::numeric AS glucose,
        NULLIF(NULLIF(trim("UREA"), 'EMPTY'), '')::numeric AS urea,
        NULLIF(NULLIF(trim("CREATININE"), 'EMPTY'), '')::numeric AS creatinine,
        NULLIF(NULLIF(trim("BNP"), 'EMPTY'), '')::numeric AS bnp,
        NULLIF(NULLIF(trim("EF"), 'EMPTY'), '')::numeric AS ef,


        "RAISED CARDIAC ENZYMES" AS raised_cardiac_enzymes,

        -- Diagnoses
        "SEVERE ANAEMIA" AS severe_anaemia,
        "ANAEMIA" AS anaemia,
        "STABLE ANGINA" AS stable_angina,
        "ACS" AS acs,
        "STEMI" AS stemi,
        "ATYPICAL CHEST PAIN" AS atypical_chest_pain,
        "HEART FAILURE" AS heart_failure,
        "HFREF" AS hfref,
        "HFNEF" AS hfnef,
        "VALVULAR" AS valvular,
        "CHB" AS chb,
        "SSS" AS sss,
        "AKI" AS aki,
        "CVA INFRACT" AS cva_infarct,
        "CVA BLEED" AS cva_bleed,
        "AF" AS af,
        "VT" AS vt,
        "PSVT" AS psvt,
        "CONGENITAL" AS congenital,
        "UTI" AS uti,
        "NEURO CARDIOGENIC SYNCOPE" AS neurocardiogenic_syncope,
        "ORTHOSTATIC" AS orthostatic,
        "INFECTIVE ENDOCARDITIS" AS infective_endocarditis,
        "DVT" AS dvt,
        "CARDIOGENIC SHOCK" AS cardiogenic_shock,
        "SHOCK" AS shock,
        "PULMONARY EMBOLISM" AS pulmonary_embolism,
        "CHEST INFECTION" AS chest_infection

    FROM hdhi_raw
)

SELECT
    *,
    -----------------------------------------------------
    -- FEATURE ENGINEERING
    -----------------------------------------------------

    CASE 
        WHEN dod_date IS NOT NULL AND doa_date IS NOT NULL 
        THEN (dod_date - doa_date)
        ELSE NULL
    END AS stay_length_days,

    (icu_stay_duration > 5)::int AS long_icu_stay,
    (stay_duration >= 10)::int AS long_stay_flag,

    (glucose > 200 OR urea > 60 OR creatinine > 2)::int AS metabolic_risk,
    (bnp > 300 OR raised_cardiac_enzymes = 1)::int AS cardiac_distress,

    -- Lifestyle risk score (using standardized names!)
    (smoking + alcohol + diabetes + hypertension + cad) 
        AS lifestyle_risk_score

FROM cleaned
WHERE
    age > 0
    AND gender IS NOT NULL;

DROP TABLE IF EXISTS analytics_hdhi_dashboard;

CREATE TABLE analytics_hdhi_dashboard AS
SELECT
    date_trunc('month', doa_date) AS month,

    COUNT(*) AS total_admissions,
    AVG(stay_duration) AS avg_stay,
    AVG(icu_stay_duration) AS avg_icu_stay,

    SUM(long_stay_flag) AS count_long_stay,
    SUM(long_icu_stay) AS count_long_icu,
    SUM(cardiac_distress) AS count_cardiac_distress,
    SUM(metabolic_risk) AS count_metabolic_risk,
    AVG(lifestyle_risk_score) AS avg_lifestyle_risk_score,

    SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) AS male_count,
    SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) AS female_count,

    SUM(CASE WHEN admission_type = 'E' THEN 1 ELSE 0 END) AS emergency_cases,
    SUM(CASE WHEN admission_type = 'O' THEN 1 ELSE 0 END) AS opd_cases


FROM analytics_hdhi_clean
GROUP BY 1
ORDER BY 1;
