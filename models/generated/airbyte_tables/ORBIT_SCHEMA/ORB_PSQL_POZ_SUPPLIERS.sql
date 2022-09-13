{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "ORBIT_SCHEMA",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('ORB_PSQL_POZ_SUPPLIERS_AB3') }}
select
    DF_SCORE,
    PARTY_ID,
    SEGMENT1,
    VAT_CODE,
    NI_NUMBER,
    TYPE_1099,
    VENDOR_ID,
    ATTRIBUTE1,
    ATTRIBUTE2,
    ATTRIBUTE3,
    ATTRIBUTE4,
    ATTRIBUTE5,
    ATTRIBUTE6,
    ATTRIBUTE7,
    ATTRIBUTE8,
    ATTRIBUTE9,
    CREATED_BY,
    PROGRAM_ID,
    REQUEST_ID,
    ATTRIBUTE10,
    ATTRIBUTE11,
    ATTRIBUTE12,
    ATTRIBUTE13,
    ATTRIBUTE14,
    ATTRIBUTE15,
    ATTRIBUTE16,
    ATTRIBUTE17,
    ATTRIBUTE18,
    ATTRIBUTE19,
    ATTRIBUTE20,
    EMPLOYEE_ID,
    REVIEW_TYPE,
    AWT_GROUP_ID,
    CUSTOMER_NUM,
    ENABLED_FLAG,
    NAME_CONTROL,
    SUMMARY_FLAG,
    CREATION_DATE,
    DF_LEGAL_NAME,
    DF_NAICS_CODE,
    ONE_TIME_FLAG,
    REBUILD_INDEX,
    ALLOW_AWT_FLAG,
    NI_NUMBER_FLAG,
    ATTRIBUTE_DATE1,
    ATTRIBUTE_DATE2,
    ATTRIBUTE_DATE3,
    ATTRIBUTE_DATE4,
    ATTRIBUTE_DATE5,
    ATTRIBUTE_DATE6,
    ATTRIBUTE_DATE7,
    ATTRIBUTE_DATE8,
    ATTRIBUTE_DATE9,
    CREATION_SOURCE,
    DF_COMPANY_NAME,
    END_DATE_ACTIVE,
    EXTERNAL_SYSTEM,
    LAST_UPDATED_BY,
    OFFSET_TAX_FLAG,
    OFFSET_VAT_CODE,
    PARENT_PARTY_ID,
    SET_OF_BOOKS_ID,
    ATTRIBUTE_DATE10,
    LAST_UPDATE_DATE,
    MIN_ORDER_AMOUNT,
    PARENT_VENDOR_ID,
    TAXPAYER_COUNTRY,
    WOMEN_OWNED_FLAG,
    ATTRIBUTE_NUMBER1,
    ATTRIBUTE_NUMBER2,
    ATTRIBUTE_NUMBER3,
    ATTRIBUTE_NUMBER4,
    ATTRIBUTE_NUMBER5,
    ATTRIBUTE_NUMBER6,
    ATTRIBUTE_NUMBER7,
    ATTRIBUTE_NUMBER8,
    ATTRIBUTE_NUMBER9,
    CHANGE_REQ_NUMBER,
    CORPORATE_WEBSITE,
    DF_LAST_SYNC_DATE,
    GLOBAL_ATTRIBUTE1,
    GLOBAL_ATTRIBUTE2,
    GLOBAL_ATTRIBUTE3,
    GLOBAL_ATTRIBUTE4,
    GLOBAL_ATTRIBUTE5,
    GLOBAL_ATTRIBUTE6,
    GLOBAL_ATTRIBUTE7,
    GLOBAL_ATTRIBUTE8,
    GLOBAL_ATTRIBUTE9,
    LAST_UPDATE_LOGIN,
    START_DATE_ACTIVE,
    ATTRIBUTE_CATEGORY,
    ATTRIBUTE_NUMBER10,
    AUTO_TAX_CALC_FLAG,
    BANK_CHARGE_BEARER,
    DATAFOX_COMPANY_ID,
    EXTERNAL_SYSTEM_ID,
    GLOBAL_ATTRIBUTE10,
    GLOBAL_ATTRIBUTE11,
    GLOBAL_ATTRIBUTE12,
    GLOBAL_ATTRIBUTE13,
    GLOBAL_ATTRIBUTE14,
    GLOBAL_ATTRIBUTE15,
    GLOBAL_ATTRIBUTE16,
    GLOBAL_ATTRIBUTE17,
    GLOBAL_ATTRIBUTE18,
    GLOBAL_ATTRIBUTE19,
    GLOBAL_ATTRIBUTE20,
    INCOME_TAX_ID_FLAG,
    OBN_MATCHED_STATUS,
    TAX_REPORTING_NAME,
    DF_TAXPAYER_COUNTRY,
    JOB_DEFINITION_NAME,
    PROGRAM_UPDATE_DATE,
    SMALL_BUSINESS_FLAG,
    AP_TAX_ROUNDING_RULE,
    ATTRIBUTE_TIMESTAMP1,
    ATTRIBUTE_TIMESTAMP2,
    ATTRIBUTE_TIMESTAMP3,
    ATTRIBUTE_TIMESTAMP4,
    ATTRIBUTE_TIMESTAMP5,
    ATTRIBUTE_TIMESTAMP6,
    ATTRIBUTE_TIMESTAMP7,
    ATTRIBUTE_TIMESTAMP8,
    ATTRIBUTE_TIMESTAMP9,
    CREATED_BY_REST_FLAG,
    DF_CORPORATE_WEBSITE,
    DF_LINKED_ADDRESS_ID,
    SUPPLIER_LOCKED_FLAG,
    VAT_REGISTRATION_NUM,
    ATTRIBUTE_TIMESTAMP10,
    BUSINESS_RELATIONSHIP,
    OBJECT_VERSION_NUMBER,
    STATE_REPORTABLE_FLAG,
    TAX_VERIFICATION_DATE,
    AUTO_TAX_CALC_OVERRIDE,
    BC_NOT_APPLICABLE_FLAG,
    GLOBAL_ATTRIBUTE_DATE1,
    GLOBAL_ATTRIBUTE_DATE2,
    GLOBAL_ATTRIBUTE_DATE3,
    GLOBAL_ATTRIBUTE_DATE4,
    GLOBAL_ATTRIBUTE_DATE5,
    GLOBAL_ATTRIBUTE_DATE6,
    GLOBAL_ATTRIBUTE_DATE7,
    GLOBAL_ATTRIBUTE_DATE8,
    GLOBAL_ATTRIBUTE_DATE9,
    JOB_DEFINITION_PACKAGE,
    PROGRAM_APPLICATION_ID,
    WITHHOLDING_START_DATE,
    FEDERAL_REPORTABLE_FLAG,
    GLOBAL_ATTRIBUTE_DATE10,
    STANDARD_INDUSTRY_CLASS,
    VENDOR_TYPE_LOOKUP_CODE,
    AMOUNT_INCLUDES_TAX_FLAG,
    GLOBAL_ATTRIBUTE_NUMBER1,
    GLOBAL_ATTRIBUTE_NUMBER2,
    GLOBAL_ATTRIBUTE_NUMBER3,
    GLOBAL_ATTRIBUTE_NUMBER4,
    GLOBAL_ATTRIBUTE_NUMBER5,
    GLOBAL_ATTRIBUTE_NUMBER6,
    GLOBAL_ATTRIBUTE_NUMBER7,
    GLOBAL_ATTRIBUTE_NUMBER8,
    GLOBAL_ATTRIBUTE_NUMBER9,
    SPEND_AUTH_JUSTIFICATION,
    SPEND_AUTH_REVIEW_STATUS,
    DF_INDUSTRY_CATEGORY_CODE,
    GLOBAL_ATTRIBUTE_CATEGORY,
    GLOBAL_ATTRIBUTE_NUMBER10,
    MINORITY_GROUP_LOOKUP_CODE,
    GLOBAL_ATTRIBUTE_TIMESTAMP1,
    GLOBAL_ATTRIBUTE_TIMESTAMP2,
    GLOBAL_ATTRIBUTE_TIMESTAMP3,
    GLOBAL_ATTRIBUTE_TIMESTAMP4,
    GLOBAL_ATTRIBUTE_TIMESTAMP5,
    GLOBAL_ATTRIBUTE_TIMESTAMP6,
    GLOBAL_ATTRIBUTE_TIMESTAMP7,
    GLOBAL_ATTRIBUTE_TIMESTAMP8,
    GLOBAL_ATTRIBUTE_TIMESTAMP9,
    GLOBAL_ATTRIBUTE_TIMESTAMP10,
    DF_INDUSTRY_SUB_CATEGORY_CODE,
    ORGANIZATION_TYPE_LOOKUP_CODE,
    WITHHOLDING_STATUS_LOOKUP_CODE,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_ORB_PSQL_POZ_SUPPLIERS_HASHID
from {{ ref('ORB_PSQL_POZ_SUPPLIERS_AB3') }}
-- ORB_PSQL_POZ_SUPPLIERS from {{ source('ORBIT_SCHEMA', '_AIRBYTE_RAW_ORB_PSQL_POZ_SUPPLIERS') }}
where 1 = 1

