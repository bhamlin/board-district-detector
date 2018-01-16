SELECT am.memberno, am.accountno, loc.name AS mapno,
    asub.ACCOUNT_SUB_TYPE_DESC as account_type,
    cc.description AS contact_type, m_ctcs.name as member_name,
    a_addr.street_no AS acct_street_no, a_addr.unit AS acct_street_unit,
    a_addr.street_name AS acct_street_name, a_addr.street_type AS acct_street_type,
    a_addr.city AS acct_city, a_addr.state AS acct_state, a_addr.zip AS acct_zip,
    a_addr.country AS acct_country,
    m_addr.street_no AS mbr_street_no, m_addr.unit AS mbr_street_unit,
    m_addr.street_name AS mbr_street_name, m_addr.street_type AS mbr_street_type,
    m_addr.city AS mbr_city, m_addr.state AS mbr_state, m_addr.zip AS mbr_zip,
    m_addr.country AS mbr_country
FROM
    cisdata.account_master am
    LEFT JOIN cisdata.account_sub_types asub ON am.ACCOUNT_SUB_TYPE_ID = asub.ACCOUNT_SUB_TYPE_ID
    LEFT JOIN cisdata.account_status ast ON am.account_status_id = ast.account_status_id
    LEFT JOIN fmdata.location loc ON am.location_id = loc.location_id
    LEFT JOIN cisdata.services svcs ON am.location_id = svcs.location_id
    LEFT JOIN cisdata.member_contacts mbrc ON am.memberno = mbrc.memberno
    LEFT JOIN cisdata.contact_codes cc ON mbrc.contact_code_id = cc.contact_code_id
    LEFT JOIN cisdata.address a_addr ON svcs.address_id = a_addr.address_id
    LEFT JOIN cisdata.address m_addr ON mbrc.address_id = m_addr.address_id
    LEFT JOIN cisdata.contacts m_ctcs on mbrc.contact_id = m_ctcs.contact_id
WHERE (1 = 1)
        -- For this table, if the account matches the primary account value, then
        --  the account is the member's single primary account.
    AND ( am.accountno = am.primary_acctno )
        -- Inactive and Killed accounts are no longer active and to be excluded
        --  from an active member listing.
        -- Not Final accounts are pending inactive status, and included as at the
        --  time they are still members.
    AND ( NOT ( ast.account_status_desc IN (
        'Inactive',
        'Killed'
    ) ) )
    AND ( asub.ACCOUNT_SUB_TYPE_DESC IN (
        'Electric',
        'Subdivision',
        'SL Only'
    ) )
