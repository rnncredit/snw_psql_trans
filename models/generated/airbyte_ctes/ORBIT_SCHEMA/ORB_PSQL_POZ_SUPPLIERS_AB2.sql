{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "_AIRBYTE_ORBIT_SCHEMA",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('ORB_PSQL_POZ_SUPPLIERS_AB1') }}
select
    cast(TRY_TO_NUMBER(DF_SCORE) as {{ dbt_utils.type_float() }}) as DF_SCORE,
    cast(TRY_TO_NUMBER(PARTY_ID)  as {{ dbt_utils.type_float() }}) as PARTY_ID,
    cast(SEGMENT1 as {{ dbt_utils.type_string() }}) as SEGMENT1,
    cast(VAT_CODE as {{ dbt_utils.type_string() }}) as VAT_CODE,
    cast(NI_NUMBER as {{ dbt_utils.type_string() }}) as NI_NUMBER,
    cast(TYPE_1099 as {{ dbt_utils.type_string() }}) as TYPE_1099,
    cast(TRY_TO_NUMBER(VENDOR_ID)  as {{ dbt_utils.type_float() }}) as VENDOR_ID,
    cast(ATTRIBUTE1 as {{ dbt_utils.type_string() }}) as ATTRIBUTE1,
    cast(ATTRIBUTE2 as {{ dbt_utils.type_string() }}) as ATTRIBUTE2,
    cast(ATTRIBUTE3 as {{ dbt_utils.type_string() }}) as ATTRIBUTE3,
    cast(ATTRIBUTE4 as {{ dbt_utils.type_string() }}) as ATTRIBUTE4,
    cast(ATTRIBUTE5 as {{ dbt_utils.type_string() }}) as ATTRIBUTE5,
    cast(ATTRIBUTE6 as {{ dbt_utils.type_string() }}) as ATTRIBUTE6,
    cast(ATTRIBUTE7 as {{ dbt_utils.type_string() }}) as ATTRIBUTE7,
    cast(ATTRIBUTE8 as {{ dbt_utils.type_string() }}) as ATTRIBUTE8,
    cast(ATTRIBUTE9 as {{ dbt_utils.type_string() }}) as ATTRIBUTE9,
    cast(CREATED_BY as {{ dbt_utils.type_string() }}) as CREATED_BY,
    cast(TRY_TO_NUMBER(PROGRAM_ID)  as {{ dbt_utils.type_float() }}) as PROGRAM_ID,
    cast(TRY_TO_NUMBER(REQUEST_ID)  as {{ dbt_utils.type_float() }}) as REQUEST_ID,
    cast(ATTRIBUTE10 as {{ dbt_utils.type_string() }}) as ATTRIBUTE10,
    cast(ATTRIBUTE11 as {{ dbt_utils.type_string() }}) as ATTRIBUTE11,
    cast(ATTRIBUTE12 as {{ dbt_utils.type_string() }}) as ATTRIBUTE12,
    cast(ATTRIBUTE13 as {{ dbt_utils.type_string() }}) as ATTRIBUTE13,
    cast(ATTRIBUTE14 as {{ dbt_utils.type_string() }}) as ATTRIBUTE14,
    cast(ATTRIBUTE15 as {{ dbt_utils.type_string() }}) as ATTRIBUTE15,
    cast(ATTRIBUTE16 as {{ dbt_utils.type_string() }}) as ATTRIBUTE16,
    cast(ATTRIBUTE17 as {{ dbt_utils.type_string() }}) as ATTRIBUTE17,
    cast(ATTRIBUTE18 as {{ dbt_utils.type_string() }}) as ATTRIBUTE18,
    cast(ATTRIBUTE19 as {{ dbt_utils.type_string() }}) as ATTRIBUTE19,
    cast(ATTRIBUTE20 as {{ dbt_utils.type_string() }}) as ATTRIBUTE20,
    cast(TRY_TO_NUMBER(EMPLOYEE_ID)  as {{ dbt_utils.type_float() }}) as EMPLOYEE_ID,
    cast(REVIEW_TYPE as {{ dbt_utils.type_string() }}) as REVIEW_TYPE,
    cast(TRY_TO_NUMBER(AWT_GROUP_ID)  as {{ dbt_utils.type_float() }}) as AWT_GROUP_ID,
    cast(CUSTOMER_NUM as {{ dbt_utils.type_string() }}) as CUSTOMER_NUM,
    cast(ENABLED_FLAG as {{ dbt_utils.type_string() }}) as ENABLED_FLAG,
    cast(NAME_CONTROL as {{ dbt_utils.type_string() }}) as NAME_CONTROL,
    cast(SUMMARY_FLAG as {{ dbt_utils.type_string() }}) as SUMMARY_FLAG,
    case
        when CREATION_DATE regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(CREATION_DATE, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when CREATION_DATE regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(CREATION_DATE, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when CREATION_DATE regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(CREATION_DATE, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when CREATION_DATE regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(CREATION_DATE, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when CREATION_DATE = '' then NULL
    else to_timestamp_tz(CREATION_DATE)
    end as CREATION_DATE
    ,
    cast(DF_LEGAL_NAME as {{ dbt_utils.type_string() }}) as DF_LEGAL_NAME,
    cast(DF_NAICS_CODE as {{ dbt_utils.type_string() }}) as DF_NAICS_CODE,
    cast(ONE_TIME_FLAG as {{ dbt_utils.type_string() }}) as ONE_TIME_FLAG,
    cast(REBUILD_INDEX as {{ dbt_utils.type_string() }}) as REBUILD_INDEX,
    cast(ALLOW_AWT_FLAG as {{ dbt_utils.type_string() }}) as ALLOW_AWT_FLAG,
    cast(NI_NUMBER_FLAG as {{ dbt_utils.type_string() }}) as NI_NUMBER_FLAG,
    cast({{ empty_string_to_null('ATTRIBUTE_DATE1') }} as {{ type_date() }}) as ATTRIBUTE_DATE1,
    cast({{ empty_string_to_null('ATTRIBUTE_DATE2') }} as {{ type_date() }}) as ATTRIBUTE_DATE2,
    cast({{ empty_string_to_null('ATTRIBUTE_DATE3') }} as {{ type_date() }}) as ATTRIBUTE_DATE3,
    cast({{ empty_string_to_null('ATTRIBUTE_DATE4') }} as {{ type_date() }}) as ATTRIBUTE_DATE4,
    cast({{ empty_string_to_null('ATTRIBUTE_DATE5') }} as {{ type_date() }}) as ATTRIBUTE_DATE5,
    cast({{ empty_string_to_null('ATTRIBUTE_DATE6') }} as {{ type_date() }}) as ATTRIBUTE_DATE6,
    cast({{ empty_string_to_null('ATTRIBUTE_DATE7') }} as {{ type_date() }}) as ATTRIBUTE_DATE7,
    cast({{ empty_string_to_null('ATTRIBUTE_DATE8') }} as {{ type_date() }}) as ATTRIBUTE_DATE8,
    cast({{ empty_string_to_null('ATTRIBUTE_DATE9') }} as {{ type_date() }}) as ATTRIBUTE_DATE9,
    cast(CREATION_SOURCE as {{ dbt_utils.type_string() }}) as CREATION_SOURCE,
    cast(DF_COMPANY_NAME as {{ dbt_utils.type_string() }}) as DF_COMPANY_NAME,
    cast({{ empty_string_to_null('END_DATE_ACTIVE') }} as {{ type_date() }}) as END_DATE_ACTIVE,
    cast(EXTERNAL_SYSTEM as {{ dbt_utils.type_string() }}) as EXTERNAL_SYSTEM,
    cast(LAST_UPDATED_BY as {{ dbt_utils.type_string() }}) as LAST_UPDATED_BY,
    cast(OFFSET_TAX_FLAG as {{ dbt_utils.type_string() }}) as OFFSET_TAX_FLAG,
    cast(OFFSET_VAT_CODE as {{ dbt_utils.type_string() }}) as OFFSET_VAT_CODE,
    cast(TRY_TO_NUMBER(PARENT_PARTY_ID) as {{ dbt_utils.type_float() }}) as PARENT_PARTY_ID,
    cast(TRY_TO_NUMBER(SET_OF_BOOKS_ID) as {{ dbt_utils.type_float() }}) as SET_OF_BOOKS_ID,
    cast({{ empty_string_to_null('ATTRIBUTE_DATE10') }} as {{ type_date() }}) as ATTRIBUTE_DATE10,
    case
        when LAST_UPDATE_DATE regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(LAST_UPDATE_DATE, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when LAST_UPDATE_DATE regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(LAST_UPDATE_DATE, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when LAST_UPDATE_DATE regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(LAST_UPDATE_DATE, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when LAST_UPDATE_DATE regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(LAST_UPDATE_DATE, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when LAST_UPDATE_DATE = '' then NULL
    else to_timestamp_tz(LAST_UPDATE_DATE)
    end as LAST_UPDATE_DATE
    ,
    cast(TRY_TO_NUMBER(MIN_ORDER_AMOUNT)  as {{ dbt_utils.type_float() }}) as MIN_ORDER_AMOUNT,
    cast(TRY_TO_NUMBER(PARENT_VENDOR_ID)  as {{ dbt_utils.type_float() }}) as PARENT_VENDOR_ID,
    cast(TAXPAYER_COUNTRY as {{ dbt_utils.type_string() }}) as TAXPAYER_COUNTRY,
    cast(WOMEN_OWNED_FLAG as {{ dbt_utils.type_string() }}) as WOMEN_OWNED_FLAG,
    cast(TRY_TO_NUMBER(ATTRIBUTE_NUMBER1)  as {{ dbt_utils.type_float() }}) as ATTRIBUTE_NUMBER1,
    cast(TRY_TO_NUMBER(ATTRIBUTE_NUMBER2)  as {{ dbt_utils.type_float() }}) as ATTRIBUTE_NUMBER2,
    cast(TRY_TO_NUMBER(ATTRIBUTE_NUMBER3)  as {{ dbt_utils.type_float() }}) as ATTRIBUTE_NUMBER3,
    cast(TRY_TO_NUMBER(ATTRIBUTE_NUMBER4)  end as {{ dbt_utils.type_float() }}) as ATTRIBUTE_NUMBER4,
    cast(TRY_TO_NUMBER(ATTRIBUTE_NUMBER5)  as {{ dbt_utils.type_float() }}) as ATTRIBUTE_NUMBER5,
    cast(TRY_TO_NUMBER(ATTRIBUTE_NUMBER6)  as {{ dbt_utils.type_float() }}) as ATTRIBUTE_NUMBER6,
    cast(TRY_TO_NUMBER(ATTRIBUTE_NUMBER7)  as {{ dbt_utils.type_float() }}) as ATTRIBUTE_NUMBER7,
    cast(TRY_TO_NUMBER(ATTRIBUTE_NUMBER8)  as {{ dbt_utils.type_float() }}) as ATTRIBUTE_NUMBER8,
    cast(TRY_TO_NUMBER(ATTRIBUTE_NUMBER9)  as {{ dbt_utils.type_float() }}) as ATTRIBUTE_NUMBER9,
    cast(TRY_TO_NUMBER(CHANGE_REQ_NUMBER)  as {{ dbt_utils.type_float() }}) as CHANGE_REQ_NUMBER,
    cast(CORPORATE_WEBSITE as {{ dbt_utils.type_string() }}) as CORPORATE_WEBSITE,
    case
        when DF_LAST_SYNC_DATE regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(DF_LAST_SYNC_DATE, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when DF_LAST_SYNC_DATE regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(DF_LAST_SYNC_DATE, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when DF_LAST_SYNC_DATE regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(DF_LAST_SYNC_DATE, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when DF_LAST_SYNC_DATE regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(DF_LAST_SYNC_DATE, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when DF_LAST_SYNC_DATE = '' then NULL
    else to_timestamp_tz(DF_LAST_SYNC_DATE)
    end as DF_LAST_SYNC_DATE
    ,
    cast(GLOBAL_ATTRIBUTE1 as {{ dbt_utils.type_string() }}) as GLOBAL_ATTRIBUTE1,
    cast(GLOBAL_ATTRIBUTE2 as {{ dbt_utils.type_string() }}) as GLOBAL_ATTRIBUTE2,
    cast(GLOBAL_ATTRIBUTE3 as {{ dbt_utils.type_string() }}) as GLOBAL_ATTRIBUTE3,
    cast(GLOBAL_ATTRIBUTE4 as {{ dbt_utils.type_string() }}) as GLOBAL_ATTRIBUTE4,
    cast(GLOBAL_ATTRIBUTE5 as {{ dbt_utils.type_string() }}) as GLOBAL_ATTRIBUTE5,
    cast(GLOBAL_ATTRIBUTE6 as {{ dbt_utils.type_string() }}) as GLOBAL_ATTRIBUTE6,
    cast(GLOBAL_ATTRIBUTE7 as {{ dbt_utils.type_string() }}) as GLOBAL_ATTRIBUTE7,
    cast(GLOBAL_ATTRIBUTE8 as {{ dbt_utils.type_string() }}) as GLOBAL_ATTRIBUTE8,
    cast(GLOBAL_ATTRIBUTE9 as {{ dbt_utils.type_string() }}) as GLOBAL_ATTRIBUTE9,
    cast(LAST_UPDATE_LOGIN as {{ dbt_utils.type_string() }}) as LAST_UPDATE_LOGIN,
    cast({{ empty_string_to_null('START_DATE_ACTIVE') }} as {{ type_date() }}) as START_DATE_ACTIVE,
    cast(ATTRIBUTE_CATEGORY as {{ dbt_utils.type_string() }}) as ATTRIBUTE_CATEGORY,
    cast(TRY_TO_NUMBER(ATTRIBUTE_NUMBER10)  as {{ dbt_utils.type_float() }}) as ATTRIBUTE_NUMBER10,
    cast(AUTO_TAX_CALC_FLAG as {{ dbt_utils.type_string() }}) as AUTO_TAX_CALC_FLAG,
    cast(BANK_CHARGE_BEARER as {{ dbt_utils.type_string() }}) as BANK_CHARGE_BEARER,
    cast(DATAFOX_COMPANY_ID as {{ dbt_utils.type_string() }}) as DATAFOX_COMPANY_ID,
    cast(TRY_TO_NUMBER(EXTERNAL_SYSTEM_ID)  as {{ dbt_utils.type_float() }}) as EXTERNAL_SYSTEM_ID,
    cast(GLOBAL_ATTRIBUTE10 as {{ dbt_utils.type_string() }}) as GLOBAL_ATTRIBUTE10,
    cast(GLOBAL_ATTRIBUTE11 as {{ dbt_utils.type_string() }}) as GLOBAL_ATTRIBUTE11,
    cast(GLOBAL_ATTRIBUTE12 as {{ dbt_utils.type_string() }}) as GLOBAL_ATTRIBUTE12,
    cast(GLOBAL_ATTRIBUTE13 as {{ dbt_utils.type_string() }}) as GLOBAL_ATTRIBUTE13,
    cast(GLOBAL_ATTRIBUTE14 as {{ dbt_utils.type_string() }}) as GLOBAL_ATTRIBUTE14,
    cast(GLOBAL_ATTRIBUTE15 as {{ dbt_utils.type_string() }}) as GLOBAL_ATTRIBUTE15,
    cast(GLOBAL_ATTRIBUTE16 as {{ dbt_utils.type_string() }}) as GLOBAL_ATTRIBUTE16,
    cast(GLOBAL_ATTRIBUTE17 as {{ dbt_utils.type_string() }}) as GLOBAL_ATTRIBUTE17,
    cast(GLOBAL_ATTRIBUTE18 as {{ dbt_utils.type_string() }}) as GLOBAL_ATTRIBUTE18,
    cast(GLOBAL_ATTRIBUTE19 as {{ dbt_utils.type_string() }}) as GLOBAL_ATTRIBUTE19,
    cast(GLOBAL_ATTRIBUTE20 as {{ dbt_utils.type_string() }}) as GLOBAL_ATTRIBUTE20,
    cast(INCOME_TAX_ID_FLAG as {{ dbt_utils.type_string() }}) as INCOME_TAX_ID_FLAG,
    cast(OBN_MATCHED_STATUS as {{ dbt_utils.type_string() }}) as OBN_MATCHED_STATUS,
    cast(TAX_REPORTING_NAME as {{ dbt_utils.type_string() }}) as TAX_REPORTING_NAME,
    cast(DF_TAXPAYER_COUNTRY as {{ dbt_utils.type_string() }}) as DF_TAXPAYER_COUNTRY,
    cast(JOB_DEFINITION_NAME as {{ dbt_utils.type_string() }}) as JOB_DEFINITION_NAME,
    cast({{ empty_string_to_null('PROGRAM_UPDATE_DATE') }} as {{ type_date() }}) as PROGRAM_UPDATE_DATE,
    cast(SMALL_BUSINESS_FLAG as {{ dbt_utils.type_string() }}) as SMALL_BUSINESS_FLAG,
    cast(AP_TAX_ROUNDING_RULE as {{ dbt_utils.type_string() }}) as AP_TAX_ROUNDING_RULE,
    case
        when ATTRIBUTE_TIMESTAMP1 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP1, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when ATTRIBUTE_TIMESTAMP1 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP1, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when ATTRIBUTE_TIMESTAMP1 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP1, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when ATTRIBUTE_TIMESTAMP1 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP1, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when ATTRIBUTE_TIMESTAMP1 = '' then NULL
    else to_timestamp_tz(ATTRIBUTE_TIMESTAMP1)
    end as ATTRIBUTE_TIMESTAMP1
    ,
    case
        when ATTRIBUTE_TIMESTAMP2 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP2, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when ATTRIBUTE_TIMESTAMP2 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP2, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when ATTRIBUTE_TIMESTAMP2 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP2, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when ATTRIBUTE_TIMESTAMP2 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP2, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when ATTRIBUTE_TIMESTAMP2 = '' then NULL
    else to_timestamp_tz(ATTRIBUTE_TIMESTAMP2)
    end as ATTRIBUTE_TIMESTAMP2
    ,
    case
        when ATTRIBUTE_TIMESTAMP3 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP3, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when ATTRIBUTE_TIMESTAMP3 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP3, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when ATTRIBUTE_TIMESTAMP3 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP3, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when ATTRIBUTE_TIMESTAMP3 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP3, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when ATTRIBUTE_TIMESTAMP3 = '' then NULL
    else to_timestamp_tz(ATTRIBUTE_TIMESTAMP3)
    end as ATTRIBUTE_TIMESTAMP3
    ,
    case
        when ATTRIBUTE_TIMESTAMP4 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP4, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when ATTRIBUTE_TIMESTAMP4 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP4, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when ATTRIBUTE_TIMESTAMP4 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP4, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when ATTRIBUTE_TIMESTAMP4 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP4, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when ATTRIBUTE_TIMESTAMP4 = '' then NULL
    else to_timestamp_tz(ATTRIBUTE_TIMESTAMP4)
    end as ATTRIBUTE_TIMESTAMP4
    ,
    case
        when ATTRIBUTE_TIMESTAMP5 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP5, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when ATTRIBUTE_TIMESTAMP5 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP5, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when ATTRIBUTE_TIMESTAMP5 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP5, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when ATTRIBUTE_TIMESTAMP5 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP5, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when ATTRIBUTE_TIMESTAMP5 = '' then NULL
    else to_timestamp_tz(ATTRIBUTE_TIMESTAMP5)
    end as ATTRIBUTE_TIMESTAMP5
    ,
    case
        when ATTRIBUTE_TIMESTAMP6 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP6, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when ATTRIBUTE_TIMESTAMP6 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP6, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when ATTRIBUTE_TIMESTAMP6 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP6, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when ATTRIBUTE_TIMESTAMP6 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP6, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when ATTRIBUTE_TIMESTAMP6 = '' then NULL
    else to_timestamp_tz(ATTRIBUTE_TIMESTAMP6)
    end as ATTRIBUTE_TIMESTAMP6
    ,
    case
        when ATTRIBUTE_TIMESTAMP7 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP7, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when ATTRIBUTE_TIMESTAMP7 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP7, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when ATTRIBUTE_TIMESTAMP7 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP7, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when ATTRIBUTE_TIMESTAMP7 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP7, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when ATTRIBUTE_TIMESTAMP7 = '' then NULL
    else to_timestamp_tz(ATTRIBUTE_TIMESTAMP7)
    end as ATTRIBUTE_TIMESTAMP7
    ,
    case
        when ATTRIBUTE_TIMESTAMP8 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP8, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when ATTRIBUTE_TIMESTAMP8 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP8, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when ATTRIBUTE_TIMESTAMP8 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP8, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when ATTRIBUTE_TIMESTAMP8 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP8, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when ATTRIBUTE_TIMESTAMP8 = '' then NULL
    else to_timestamp_tz(ATTRIBUTE_TIMESTAMP8)
    end as ATTRIBUTE_TIMESTAMP8
    ,
    case
        when ATTRIBUTE_TIMESTAMP9 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP9, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when ATTRIBUTE_TIMESTAMP9 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP9, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when ATTRIBUTE_TIMESTAMP9 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP9, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when ATTRIBUTE_TIMESTAMP9 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP9, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when ATTRIBUTE_TIMESTAMP9 = '' then NULL
    else to_timestamp_tz(ATTRIBUTE_TIMESTAMP9)
    end as ATTRIBUTE_TIMESTAMP9
    ,
    cast(CREATED_BY_REST_FLAG as {{ dbt_utils.type_string() }}) as CREATED_BY_REST_FLAG,
    cast(DF_CORPORATE_WEBSITE as {{ dbt_utils.type_string() }}) as DF_CORPORATE_WEBSITE,
    cast(case DF_LINKED_ADDRESS_ID when '' then 0 else DF_LINKED_ADDRESS_ID end as {{ dbt_utils.type_float() }}) as DF_LINKED_ADDRESS_ID,
    cast(SUPPLIER_LOCKED_FLAG as {{ dbt_utils.type_string() }}) as SUPPLIER_LOCKED_FLAG,
    cast(VAT_REGISTRATION_NUM as {{ dbt_utils.type_string() }}) as VAT_REGISTRATION_NUM,
    case
        when ATTRIBUTE_TIMESTAMP10 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP10, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when ATTRIBUTE_TIMESTAMP10 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP10, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when ATTRIBUTE_TIMESTAMP10 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP10, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when ATTRIBUTE_TIMESTAMP10 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(ATTRIBUTE_TIMESTAMP10, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when ATTRIBUTE_TIMESTAMP10 = '' then NULL
    else to_timestamp_tz(ATTRIBUTE_TIMESTAMP10)
    end as ATTRIBUTE_TIMESTAMP10
    ,
    cast(BUSINESS_RELATIONSHIP as {{ dbt_utils.type_string() }}) as BUSINESS_RELATIONSHIP,
    cast(TRY_TO_NUMBER(OBJECT_VERSION_NUMBER)  as {{ dbt_utils.type_float() }}) as OBJECT_VERSION_NUMBER,
    cast(STATE_REPORTABLE_FLAG as {{ dbt_utils.type_string() }}) as STATE_REPORTABLE_FLAG,
    cast({{ empty_string_to_null('TAX_VERIFICATION_DATE') }} as {{ type_date() }}) as TAX_VERIFICATION_DATE,
    cast(AUTO_TAX_CALC_OVERRIDE as {{ dbt_utils.type_string() }}) as AUTO_TAX_CALC_OVERRIDE,
    cast(BC_NOT_APPLICABLE_FLAG as {{ dbt_utils.type_string() }}) as BC_NOT_APPLICABLE_FLAG,
    cast({{ empty_string_to_null('GLOBAL_ATTRIBUTE_DATE1') }} as {{ type_date() }}) as GLOBAL_ATTRIBUTE_DATE1,
    cast({{ empty_string_to_null('GLOBAL_ATTRIBUTE_DATE2') }} as {{ type_date() }}) as GLOBAL_ATTRIBUTE_DATE2,
    cast({{ empty_string_to_null('GLOBAL_ATTRIBUTE_DATE3') }} as {{ type_date() }}) as GLOBAL_ATTRIBUTE_DATE3,
    cast({{ empty_string_to_null('GLOBAL_ATTRIBUTE_DATE4') }} as {{ type_date() }}) as GLOBAL_ATTRIBUTE_DATE4,
    cast({{ empty_string_to_null('GLOBAL_ATTRIBUTE_DATE5') }} as {{ type_date() }}) as GLOBAL_ATTRIBUTE_DATE5,
    cast({{ empty_string_to_null('GLOBAL_ATTRIBUTE_DATE6') }} as {{ type_date() }}) as GLOBAL_ATTRIBUTE_DATE6,
    cast({{ empty_string_to_null('GLOBAL_ATTRIBUTE_DATE7') }} as {{ type_date() }}) as GLOBAL_ATTRIBUTE_DATE7,
    cast({{ empty_string_to_null('GLOBAL_ATTRIBUTE_DATE8') }} as {{ type_date() }}) as GLOBAL_ATTRIBUTE_DATE8,
    cast({{ empty_string_to_null('GLOBAL_ATTRIBUTE_DATE9') }} as {{ type_date() }}) as GLOBAL_ATTRIBUTE_DATE9,
    cast(JOB_DEFINITION_PACKAGE as {{ dbt_utils.type_string() }}) as JOB_DEFINITION_PACKAGE,
    cast(case PROGRAM_APPLICATION_ID when '' then 0 else PROGRAM_APPLICATION_ID end as {{ dbt_utils.type_float() }}) as PROGRAM_APPLICATION_ID,
    cast({{ empty_string_to_null('WITHHOLDING_START_DATE') }} as {{ type_date() }}) as WITHHOLDING_START_DATE,
    cast(FEDERAL_REPORTABLE_FLAG as {{ dbt_utils.type_string() }}) as FEDERAL_REPORTABLE_FLAG,
    cast({{ empty_string_to_null('GLOBAL_ATTRIBUTE_DATE10') }} as {{ type_date() }}) as GLOBAL_ATTRIBUTE_DATE10,
    cast(STANDARD_INDUSTRY_CLASS as {{ dbt_utils.type_string() }}) as STANDARD_INDUSTRY_CLASS,
    cast(VENDOR_TYPE_LOOKUP_CODE as {{ dbt_utils.type_string() }}) as VENDOR_TYPE_LOOKUP_CODE,
    cast(AMOUNT_INCLUDES_TAX_FLAG as {{ dbt_utils.type_string() }}) as AMOUNT_INCLUDES_TAX_FLAG,
    cast(TRY_TO_NUMBER(GLOBAL_ATTRIBUTE_NUMBER1)  as {{ dbt_utils.type_float() }}) as GLOBAL_ATTRIBUTE_NUMBER1,
    cast(TRY_TO_NUMBER(GLOBAL_ATTRIBUTE_NUMBER2)  end as {{ dbt_utils.type_float() }}) as GLOBAL_ATTRIBUTE_NUMBER2,
    cast(TRY_TO_NUMBER(GLOBAL_ATTRIBUTE_NUMBER3)  as {{ dbt_utils.type_float() }}) as GLOBAL_ATTRIBUTE_NUMBER3,
    cast(TRY_TO_NUMBER(GLOBAL_ATTRIBUTE_NUMBER4)  as {{ dbt_utils.type_float() }}) as GLOBAL_ATTRIBUTE_NUMBER4,
    cast(TRY_TO_NUMBER(GLOBAL_ATTRIBUTE_NUMBER5)  as {{ dbt_utils.type_float() }}) as GLOBAL_ATTRIBUTE_NUMBER5,
    cast(TRY_TO_NUMBER(GLOBAL_ATTRIBUTE_NUMBER6)  as {{ dbt_utils.type_float() }}) as GLOBAL_ATTRIBUTE_NUMBER6,
    cast(TRY_TO_NUMBER(GLOBAL_ATTRIBUTE_NUMBER7)  as {{ dbt_utils.type_float() }}) as GLOBAL_ATTRIBUTE_NUMBER7,
    cast(TRY_TO_NUMBER(GLOBAL_ATTRIBUTE_NUMBER8)  as {{ dbt_utils.type_float() }}) as GLOBAL_ATTRIBUTE_NUMBER8,
    cast(TRY_TO_NUMBER(GLOBAL_ATTRIBUTE_NUMBER9)  as {{ dbt_utils.type_float() }}) as GLOBAL_ATTRIBUTE_NUMBER9,
    cast(SPEND_AUTH_JUSTIFICATION as {{ dbt_utils.type_string() }}) as SPEND_AUTH_JUSTIFICATION,
    cast(SPEND_AUTH_REVIEW_STATUS as {{ dbt_utils.type_string() }}) as SPEND_AUTH_REVIEW_STATUS,
    cast(DF_INDUSTRY_CATEGORY_CODE as {{ dbt_utils.type_string() }}) as DF_INDUSTRY_CATEGORY_CODE,
    cast(GLOBAL_ATTRIBUTE_CATEGORY as {{ dbt_utils.type_string() }}) as GLOBAL_ATTRIBUTE_CATEGORY,
    cast(TRY_TO_NUMBER(GLOBAL_ATTRIBUTE_NUMBER10)  as {{ dbt_utils.type_float() }}) as GLOBAL_ATTRIBUTE_NUMBER10,
    cast(MINORITY_GROUP_LOOKUP_CODE as {{ dbt_utils.type_string() }}) as MINORITY_GROUP_LOOKUP_CODE,
    case
        when GLOBAL_ATTRIBUTE_TIMESTAMP1 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP1, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when GLOBAL_ATTRIBUTE_TIMESTAMP1 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP1, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when GLOBAL_ATTRIBUTE_TIMESTAMP1 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP1, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when GLOBAL_ATTRIBUTE_TIMESTAMP1 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP1, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when GLOBAL_ATTRIBUTE_TIMESTAMP1 = '' then NULL
    else to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP1)
    end as GLOBAL_ATTRIBUTE_TIMESTAMP1
    ,
    case
        when GLOBAL_ATTRIBUTE_TIMESTAMP2 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP2, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when GLOBAL_ATTRIBUTE_TIMESTAMP2 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP2, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when GLOBAL_ATTRIBUTE_TIMESTAMP2 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP2, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when GLOBAL_ATTRIBUTE_TIMESTAMP2 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP2, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when GLOBAL_ATTRIBUTE_TIMESTAMP2 = '' then NULL
    else to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP2)
    end as GLOBAL_ATTRIBUTE_TIMESTAMP2
    ,
    case
        when GLOBAL_ATTRIBUTE_TIMESTAMP3 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP3, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when GLOBAL_ATTRIBUTE_TIMESTAMP3 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP3, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when GLOBAL_ATTRIBUTE_TIMESTAMP3 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP3, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when GLOBAL_ATTRIBUTE_TIMESTAMP3 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP3, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when GLOBAL_ATTRIBUTE_TIMESTAMP3 = '' then NULL
    else to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP3)
    end as GLOBAL_ATTRIBUTE_TIMESTAMP3
    ,
    case
        when GLOBAL_ATTRIBUTE_TIMESTAMP4 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP4, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when GLOBAL_ATTRIBUTE_TIMESTAMP4 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP4, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when GLOBAL_ATTRIBUTE_TIMESTAMP4 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP4, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when GLOBAL_ATTRIBUTE_TIMESTAMP4 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP4, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when GLOBAL_ATTRIBUTE_TIMESTAMP4 = '' then NULL
    else to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP4)
    end as GLOBAL_ATTRIBUTE_TIMESTAMP4
    ,
    case
        when GLOBAL_ATTRIBUTE_TIMESTAMP5 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP5, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when GLOBAL_ATTRIBUTE_TIMESTAMP5 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP5, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when GLOBAL_ATTRIBUTE_TIMESTAMP5 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP5, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when GLOBAL_ATTRIBUTE_TIMESTAMP5 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP5, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when GLOBAL_ATTRIBUTE_TIMESTAMP5 = '' then NULL
    else to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP5)
    end as GLOBAL_ATTRIBUTE_TIMESTAMP5
    ,
    case
        when GLOBAL_ATTRIBUTE_TIMESTAMP6 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP6, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when GLOBAL_ATTRIBUTE_TIMESTAMP6 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP6, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when GLOBAL_ATTRIBUTE_TIMESTAMP6 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP6, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when GLOBAL_ATTRIBUTE_TIMESTAMP6 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP6, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when GLOBAL_ATTRIBUTE_TIMESTAMP6 = '' then NULL
    else to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP6)
    end as GLOBAL_ATTRIBUTE_TIMESTAMP6
    ,
    case
        when GLOBAL_ATTRIBUTE_TIMESTAMP7 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP7, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when GLOBAL_ATTRIBUTE_TIMESTAMP7 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP7, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when GLOBAL_ATTRIBUTE_TIMESTAMP7 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP7, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when GLOBAL_ATTRIBUTE_TIMESTAMP7 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP7, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when GLOBAL_ATTRIBUTE_TIMESTAMP7 = '' then NULL
    else to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP7)
    end as GLOBAL_ATTRIBUTE_TIMESTAMP7
    ,
    case
        when GLOBAL_ATTRIBUTE_TIMESTAMP8 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP8, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when GLOBAL_ATTRIBUTE_TIMESTAMP8 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP8, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when GLOBAL_ATTRIBUTE_TIMESTAMP8 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP8, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when GLOBAL_ATTRIBUTE_TIMESTAMP8 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP8, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when GLOBAL_ATTRIBUTE_TIMESTAMP8 = '' then NULL
    else to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP8)
    end as GLOBAL_ATTRIBUTE_TIMESTAMP8
    ,
    case
        when GLOBAL_ATTRIBUTE_TIMESTAMP9 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP9, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when GLOBAL_ATTRIBUTE_TIMESTAMP9 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP9, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when GLOBAL_ATTRIBUTE_TIMESTAMP9 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP9, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when GLOBAL_ATTRIBUTE_TIMESTAMP9 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP9, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when GLOBAL_ATTRIBUTE_TIMESTAMP9 = '' then NULL
    else to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP9)
    end as GLOBAL_ATTRIBUTE_TIMESTAMP9
    ,
    case
        when GLOBAL_ATTRIBUTE_TIMESTAMP10 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP10, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when GLOBAL_ATTRIBUTE_TIMESTAMP10 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP10, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when GLOBAL_ATTRIBUTE_TIMESTAMP10 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP10, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when GLOBAL_ATTRIBUTE_TIMESTAMP10 regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP10, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when GLOBAL_ATTRIBUTE_TIMESTAMP10 = '' then NULL
    else to_timestamp_tz(GLOBAL_ATTRIBUTE_TIMESTAMP10)
    end as GLOBAL_ATTRIBUTE_TIMESTAMP10
    ,
    cast(DF_INDUSTRY_SUB_CATEGORY_CODE as {{ dbt_utils.type_string() }}) as DF_INDUSTRY_SUB_CATEGORY_CODE,
    cast(ORGANIZATION_TYPE_LOOKUP_CODE as {{ dbt_utils.type_string() }}) as ORGANIZATION_TYPE_LOOKUP_CODE,
    cast(WITHHOLDING_STATUS_LOOKUP_CODE as {{ dbt_utils.type_string() }}) as WITHHOLDING_STATUS_LOOKUP_CODE,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('ORB_PSQL_POZ_SUPPLIERS_AB1') }}
-- ORB_PSQL_POZ_SUPPLIERS
where 1 = 1

