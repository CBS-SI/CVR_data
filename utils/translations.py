"""
CVR Data Translation Mappings

Danish to English translation dictionaries for CVR data columns and values.
"""

# Columns translations (Danish -> English)
COLUMN_TRANSLATIONS = {
    # Identifiers
    'cvrNummer': 'cvr_number',
    'enhedsNummer': 'unit_number',
    'enhedstype': 'unit_type',

    # Temporal validity fields
    'gyldigFra': 'valid_from',
    'gyldigTil': 'valid_to',
    'sidstOpdateret': 'last_updated',
    'sidstIndlaest': 'last_loaded',
    'stiftelsesDato': 'founding_date',
    'virkningsDato': 'effective_date',
    'naermesteFremtidigeDato': 'nearest_future_date',

    # Address fields
    'vejnavn': 'street_name',
    'vejkode': 'road_code',
    'husnummerFra': 'house_number_from',
    'husnummerTil': 'house_number_to',
    'bogstavFra': 'letter_from',
    'bogstavTil': 'letter_to',
    'etage': 'floor',
    'sidedoer': 'side_door',
    'conavn': 'care_of_name',
    'postboks': 'po_box',
    'postnummer': 'postal_code',
    'postdistrikt': 'postal_district',
    'bynavn': 'city_name',
    'landekode': 'country_code',
    'kommune.kommuneKode': 'municipality_code',
    'kommune.kommuneNavn': 'municipality_name',
    'kommune.periode': 'municipality_validity_period',
    'kommuneKode': 'municipality_code',
    'kommuneNavn': 'municipality_name',
    'adresseId': 'address_id',
    'sidstValideret': 'last_validated',
    'periode': 'validity_period',
    'fritekst': 'free_text',

    # Industry fields
    'branchekode': 'industry_code',
    'branchetekst': 'industry_text',

    # Attributes fields
    'type': 'type',
    'vaerditype': 'value_type',
    'vaerdier': 'values',
    'vaerdier.vaerdi': 'value',
    'vaerdi': 'value',
    'sekvensnr': 'sequence_number',

    # Employment fields
    'aar': 'year',
    'kvartal': 'quarter',
    'maaned': 'month',
    'antalInklusivEjere': 'count_including_owners',
    'antalAarsvaerk': 'full_time_equivalents',
    'antalAnsatte': 'employee_count',
    'intervalKodeAntalInklusivEjere': 'interval_code_including_owners',
    'intervalKodeAntalAarsvaerk': 'interval_code_fte',
    'intervalKodeAntalAnsatte': 'interval_code_employees',

    # Company form fields
    'virksomhedsformkode': 'legal_form_code',
    'kortBeskrivelse': 'short_description',
    'langBeskrivelse': 'long_description',
    'ansvarligDataleverandoer': 'responsible_data_provider',

    # Contact fields
    'kontaktoplysning': 'contact_info',
    'hemmelig': 'secret',

    # Name fields
    'navn': 'name',

    # Status fields
    'status': 'status',
    'statuskode': 'status_code',
    'statustekst': 'status_text',
    'sammensatStatus': 'composite_status',
    'kreditoplysningkode': 'credit_info_code',
    'kreditoplysningtekst': 'credit_info_text',

    # Participant relation fields
    'deltagerEnhedsNummer': 'participant_unit_number',
    'deltagerCvrNummer': 'participant_cvr_number',
    'deltagerNavn': 'participant_name',
    'deltagerType': 'participant_type',
    'rolle': 'role',
    'kontorsteder': 'office_locations',
    'organisationer': 'organizations',

    # Registration number fields
    'regnummer': 'registration_number',

    # Main file metadata fields
    'reklamebeskyttet': 'advertising_protected',
    'brancheAnsvarskode': 'industry_responsibility_code',
    'dataAdgang': 'data_access',
    'fejlRegistreret': 'error_registered',
    'fejlVedIndlaesning': 'error_on_loading',
    'fejlBeskrivelse': 'error_description',
    'samtId': 'internal_id',
    'virkningsAktoer': 'change_actor',
    'fortroligBeriget': 'confidential_enriched',
    'antalPenheder': 'number_of_p_units',

    # Search index technical fields
    '_score': 'search_score',
    '_id': 'search_id',
    '_type': 'search_type',
    '_index': 'search_index',

    # Main file - Vrvirksomhed base columns
    'Vrvirksomhed_cvrNummer': 'cvr_number',
    'Vrvirksomhed_brancheAnsvarskode': 'industry_responsibility_code',
    'Vrvirksomhed_reklamebeskyttet': 'advertising_protected',
    'Vrvirksomhed_samtId': 'internal_id',
    'Vrvirksomhed_fejlRegistreret': 'error_registered',
    'Vrvirksomhed_dataAdgang': 'data_access',
    'Vrvirksomhed_enhedsNummer': 'unit_number',
    'Vrvirksomhed_enhedstype': 'unit_type',
    'Vrvirksomhed_sidstIndlaest': 'last_loaded',
    'Vrvirksomhed_sidstOpdateret': 'last_updated',
    'Vrvirksomhed_fejlVedIndlaesning': 'error_on_loading',
    'Vrvirksomhed_naermesteFremtidigeDato': 'nearest_future_date',
    'Vrvirksomhed_fejlBeskrivelse': 'error_description',
    'Vrvirksomhed_virkningsAktoer': 'change_actor',
    'Vrvirksomhed_fortroligBeriget': 'confidential_enriched',

    # Main file - virksomhedMetadata general
    'Vrvirksomhed_virksomhedMetadata_sammensatStatus': 'company_status',
    'Vrvirksomhed_virksomhedMetadata_stiftelsesDato': 'founding_date',
    'Vrvirksomhed_virksomhedMetadata_virkningsDato': 'effective_date',
    'Vrvirksomhed_virksomhedMetadata_antalPenheder': 'number_of_p_units',

    # Main file - nyesteNavn (latest name)
    'Vrvirksomhed_virksomhedMetadata_nyesteNavn': 'latest_name_json',
    'Vrvirksomhed_virksomhedMetadata_nyesteNavn_navn': 'latest_name',
    'Vrvirksomhed_virksomhedMetadata_nyesteNavn_periode_gyldigFra': 'latest_name_valid_from',
    'Vrvirksomhed_virksomhedMetadata_nyesteNavn_periode_gyldigTil': 'latest_name_valid_to',
    'Vrvirksomhed_virksomhedMetadata_nyesteNavn_sidstOpdateret': 'latest_name_last_updated',

    # Main file - nyesteVirksomhedsform (latest legal form)
    'Vrvirksomhed_virksomhedMetadata_nyesteVirksomhedsform': 'latest_legal_form_json',
    'Vrvirksomhed_virksomhedMetadata_nyesteVirksomhedsform_virksomhedsformkode': 'latest_legal_form_code',
    'Vrvirksomhed_virksomhedMetadata_nyesteVirksomhedsform_kortBeskrivelse': 'latest_legal_form_short',
    'Vrvirksomhed_virksomhedMetadata_nyesteVirksomhedsform_langBeskrivelse': 'latest_legal_form_long',
    'Vrvirksomhed_virksomhedMetadata_nyesteVirksomhedsform_ansvarligDataleverandoer': 'latest_legal_form_data_provider',
    'Vrvirksomhed_virksomhedMetadata_nyesteVirksomhedsform_periode_gyldigFra': 'latest_legal_form_valid_from',
    'Vrvirksomhed_virksomhedMetadata_nyesteVirksomhedsform_periode_gyldigTil': 'latest_legal_form_valid_to',
    'Vrvirksomhed_virksomhedMetadata_nyesteVirksomhedsform_sidstOpdateret': 'latest_legal_form_last_updated',

    # Main file - nyesteBeliggenhedsadresse (latest business address)
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse': 'latest_address_json',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_landekode': 'latest_address_country_code',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_fritekst': 'latest_address_free_text',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_vejkode': 'latest_address_road_code',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_vejnavn': 'latest_address_street_name',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_husnummerFra': 'latest_address_house_number_from',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_husnummerTil': 'latest_address_house_number_to',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_bogstavFra': 'latest_address_letter_from',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_bogstavTil': 'latest_address_letter_to',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_etage': 'latest_address_floor',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_sidedoer': 'latest_address_side_door',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_conavn': 'latest_address_care_of_name',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_postboks': 'latest_address_po_box',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_postnummer': 'latest_address_postal_code',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_postdistrikt': 'latest_address_postal_district',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_bynavn': 'latest_address_city_name',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_adresseId': 'latest_address_id',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_sidstValideret': 'latest_address_last_validated',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_periode_gyldigFra': 'latest_address_valid_from',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_periode_gyldigTil': 'latest_address_valid_to',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_sidstOpdateret': 'latest_address_last_updated',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_kommune': 'latest_address_municipality_json',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_kommune_kommuneKode': 'latest_address_municipality_code',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_kommune_kommuneNavn': 'latest_address_municipality_name',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_kommune_periode_gyldigFra': 'latest_address_municipality_valid_from',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_kommune_periode_gyldigTil': 'latest_address_municipality_valid_to',
    'Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_kommune_sidstOpdateret': 'latest_address_municipality_last_updated',

    # Main file - nyesteHovedbranche (latest main industry)
    'Vrvirksomhed_virksomhedMetadata_nyesteHovedbranche': 'latest_main_industry_json',
    'Vrvirksomhed_virksomhedMetadata_nyesteHovedbranche_branchekode': 'latest_main_industry_code',
    'Vrvirksomhed_virksomhedMetadata_nyesteHovedbranche_branchetekst': 'latest_main_industry_text',
    'Vrvirksomhed_virksomhedMetadata_nyesteHovedbranche_periode_gyldigFra': 'latest_main_industry_valid_from',
    'Vrvirksomhed_virksomhedMetadata_nyesteHovedbranche_periode_gyldigTil': 'latest_main_industry_valid_to',
    'Vrvirksomhed_virksomhedMetadata_nyesteHovedbranche_sidstOpdateret': 'latest_main_industry_last_updated',

    # Main file - nyesteBibranche1 (latest secondary industry 1)
    'Vrvirksomhed_virksomhedMetadata_nyesteBibranche1': 'latest_secondary_industry1_json',
    'Vrvirksomhed_virksomhedMetadata_nyesteBibranche1_branchekode': 'latest_secondary_industry1_code',
    'Vrvirksomhed_virksomhedMetadata_nyesteBibranche1_branchetekst': 'latest_secondary_industry1_text',
    'Vrvirksomhed_virksomhedMetadata_nyesteBibranche1_periode_gyldigFra': 'latest_secondary_industry1_valid_from',
    'Vrvirksomhed_virksomhedMetadata_nyesteBibranche1_periode_gyldigTil': 'latest_secondary_industry1_valid_to',
    'Vrvirksomhed_virksomhedMetadata_nyesteBibranche1_sidstOpdateret': 'latest_secondary_industry1_last_updated',

    # Main file - nyesteBibranche2 (latest secondary industry 2)
    'Vrvirksomhed_virksomhedMetadata_nyesteBibranche2': 'latest_secondary_industry2_json',
    'Vrvirksomhed_virksomhedMetadata_nyesteBibranche2_branchekode': 'latest_secondary_industry2_code',
    'Vrvirksomhed_virksomhedMetadata_nyesteBibranche2_branchetekst': 'latest_secondary_industry2_text',
    'Vrvirksomhed_virksomhedMetadata_nyesteBibranche2_periode_gyldigFra': 'latest_secondary_industry2_valid_from',
    'Vrvirksomhed_virksomhedMetadata_nyesteBibranche2_periode_gyldigTil': 'latest_secondary_industry2_valid_to',
    'Vrvirksomhed_virksomhedMetadata_nyesteBibranche2_sidstOpdateret': 'latest_secondary_industry2_last_updated',

    # Main file - nyesteBibranche3 (latest secondary industry 3)
    'Vrvirksomhed_virksomhedMetadata_nyesteBibranche3': 'latest_secondary_industry3_json',
    'Vrvirksomhed_virksomhedMetadata_nyesteBibranche3_branchekode': 'latest_secondary_industry3_code',
    'Vrvirksomhed_virksomhedMetadata_nyesteBibranche3_branchetekst': 'latest_secondary_industry3_text',
    'Vrvirksomhed_virksomhedMetadata_nyesteBibranche3_periode_gyldigFra': 'latest_secondary_industry3_valid_from',
    'Vrvirksomhed_virksomhedMetadata_nyesteBibranche3_periode_gyldigTil': 'latest_secondary_industry3_valid_to',
    'Vrvirksomhed_virksomhedMetadata_nyesteBibranche3_sidstOpdateret': 'latest_secondary_industry3_last_updated',

    # Main file - nyesteStatus (latest status)
    'Vrvirksomhed_virksomhedMetadata_nyesteStatus': 'latest_status_json',
    'Vrvirksomhed_virksomhedMetadata_nyesteStatus_statuskode': 'latest_status_code',
    'Vrvirksomhed_virksomhedMetadata_nyesteStatus_statustekst': 'latest_status_text',
    'Vrvirksomhed_virksomhedMetadata_nyesteStatus_kreditoplysningkode': 'latest_status_credit_info_code',
    'Vrvirksomhed_virksomhedMetadata_nyesteStatus_kreditoplysningtekst': 'latest_status_credit_info_text',
    'Vrvirksomhed_virksomhedMetadata_nyesteStatus_periode_gyldigFra': 'latest_status_valid_from',
    'Vrvirksomhed_virksomhedMetadata_nyesteStatus_periode_gyldigTil': 'latest_status_valid_to',
    'Vrvirksomhed_virksomhedMetadata_nyesteStatus_sidstOpdateret': 'latest_status_last_updated',

    # Main file - nyesteAarsbeskaeftigelse (latest annual employment)
    'Vrvirksomhed_virksomhedMetadata_nyesteAarsbeskaeftigelse': 'latest_annual_employment_json',
    'Vrvirksomhed_virksomhedMetadata_nyesteAarsbeskaeftigelse_aar': 'latest_annual_employment_year',
    'Vrvirksomhed_virksomhedMetadata_nyesteAarsbeskaeftigelse_antalInklusivEjere': 'latest_annual_count_including_owners',
    'Vrvirksomhed_virksomhedMetadata_nyesteAarsbeskaeftigelse_antalAarsvaerk': 'latest_annual_full_time_equivalents',
    'Vrvirksomhed_virksomhedMetadata_nyesteAarsbeskaeftigelse_antalAnsatte': 'latest_annual_employee_count',
    'Vrvirksomhed_virksomhedMetadata_nyesteAarsbeskaeftigelse_sidstOpdateret': 'latest_annual_employment_last_updated',
    'Vrvirksomhed_virksomhedMetadata_nyesteAarsbeskaeftigelse_intervalKodeAntalInklusivEjere': 'latest_annual_interval_code_including_owners',
    'Vrvirksomhed_virksomhedMetadata_nyesteAarsbeskaeftigelse_intervalKodeAntalAarsvaerk': 'latest_annual_interval_code_fte',
    'Vrvirksomhed_virksomhedMetadata_nyesteAarsbeskaeftigelse_intervalKodeAntalAnsatte': 'latest_annual_interval_code_employees',

    # Main file - nyesteKvartalsbeskaeftigelse (latest quarterly employment)
    'Vrvirksomhed_virksomhedMetadata_nyesteKvartalsbeskaeftigelse': 'latest_quarterly_employment_json',
    'Vrvirksomhed_virksomhedMetadata_nyesteKvartalsbeskaeftigelse_aar': 'latest_quarterly_employment_year',
    'Vrvirksomhed_virksomhedMetadata_nyesteKvartalsbeskaeftigelse_kvartal': 'latest_quarterly_employment_quarter',
    'Vrvirksomhed_virksomhedMetadata_nyesteKvartalsbeskaeftigelse_antalAarsvaerk': 'latest_quarterly_full_time_equivalents',
    'Vrvirksomhed_virksomhedMetadata_nyesteKvartalsbeskaeftigelse_antalAnsatte': 'latest_quarterly_employee_count',
    'Vrvirksomhed_virksomhedMetadata_nyesteKvartalsbeskaeftigelse_sidstOpdateret': 'latest_quarterly_employment_last_updated',
    'Vrvirksomhed_virksomhedMetadata_nyesteKvartalsbeskaeftigelse_intervalKodeAntalAarsvaerk': 'latest_quarterly_interval_code_fte',
    'Vrvirksomhed_virksomhedMetadata_nyesteKvartalsbeskaeftigelse_intervalKodeAntalAnsatte': 'latest_quarterly_interval_code_employees',

    # Main file - nyesteMaanedsbeskaeftigelse (latest monthly employment)
    'Vrvirksomhed_virksomhedMetadata_nyesteMaanedsbeskaeftigelse': 'latest_monthly_employment_json',
    'Vrvirksomhed_virksomhedMetadata_nyesteMaanedsbeskaeftigelse_aar': 'latest_monthly_employment_year',
    'Vrvirksomhed_virksomhedMetadata_nyesteMaanedsbeskaeftigelse_maaned': 'latest_monthly_employment_month',
    'Vrvirksomhed_virksomhedMetadata_nyesteMaanedsbeskaeftigelse_antalAarsvaerk': 'latest_monthly_full_time_equivalents',
    'Vrvirksomhed_virksomhedMetadata_nyesteMaanedsbeskaeftigelse_antalAnsatte': 'latest_monthly_employee_count',
    'Vrvirksomhed_virksomhedMetadata_nyesteMaanedsbeskaeftigelse_sidstOpdateret': 'latest_monthly_employment_last_updated',
    'Vrvirksomhed_virksomhedMetadata_nyesteMaanedsbeskaeftigelse_intervalKodeAntalAarsvaerk': 'latest_monthly_interval_code_fte',
    'Vrvirksomhed_virksomhedMetadata_nyesteMaanedsbeskaeftigelse_intervalKodeAntalAnsatte': 'latest_monthly_interval_code_employees',

    # Main file - nyesteErstMaanedsbeskaeftigelse (latest first monthly employment)
    'Vrvirksomhed_virksomhedMetadata_nyesteErstMaanedsbeskaeftigelse': 'latest_first_monthly_employment_json',
    'Vrvirksomhed_virksomhedMetadata_nyesteErstMaanedsbeskaeftigelse_aar': 'latest_first_monthly_employment_year',
    'Vrvirksomhed_virksomhedMetadata_nyesteErstMaanedsbeskaeftigelse_maaned': 'latest_first_monthly_employment_month',
    'Vrvirksomhed_virksomhedMetadata_nyesteErstMaanedsbeskaeftigelse_antalAarsvaerk': 'latest_first_monthly_full_time_equivalents',
    'Vrvirksomhed_virksomhedMetadata_nyesteErstMaanedsbeskaeftigelse_antalAnsatte': 'latest_first_monthly_employee_count',
    'Vrvirksomhed_virksomhedMetadata_nyesteErstMaanedsbeskaeftigelse_sidstOpdateret': 'latest_first_monthly_employment_last_updated',
    'Vrvirksomhed_virksomhedMetadata_nyesteErstMaanedsbeskaeftigelse_intervalKodeAntalAarsvaerk': 'latest_first_monthly_interval_code_fte',
    'Vrvirksomhed_virksomhedMetadata_nyesteErstMaanedsbeskaeftigelse_intervalKodeAntalAnsatte': 'latest_first_monthly_interval_code_employees',
}

# Values translations (Danish -> English)
VALUE_TRANSLATIONS = {
    # Company status values
    'NORMAL': 'active',
    'UNDER TVANGSOPLØSNING': 'under_forced_dissolution',
    'TVANGSOPLØST': 'forcibly_dissolved',
    'UNDER FRIVILLIG LIKVIDATION': 'under_voluntary_liquidation',
    'OPLØST EFTER FRIVILLIG LIKVIDATION': 'dissolved_after_voluntary_liquidation',
    'OPLØST EFTER ERKLÆRING': 'dissolved_by_declaration',
    'UNDER KONKURS': 'under_bankruptcy',
    'OPLØST EFTER KONKURS': 'dissolved_after_bankruptcy',
    'UNDER REKONSTRUKTION': 'under_reconstruction',
    'UNDER REASSUMERING': 'under_reassumption',
    'OPLØST EFTER FUSION': 'dissolved_after_merger',
    'OPLØST EFTER SPALTNING': 'dissolved_after_split',
    'SLETTET': 'deleted',
    'UDEN RETSVIRKNING': 'without_legal_effect',

    # Industry codes
    '949900': 'other_organizations_and_associations_nec',
    '980000': 'unspecified',
    '620100': 'computer_programming',
    '702200': 'business_consultancy',

    # Attribute type codes
    'TEGNINGSREGEL': 'signing_rules',
    'VEDTÆGT_SENESTE': 'latest_articles_of_association',
    'FORMÅL': 'company_purpose',
    'REGNSKABSÅR_START': 'financial_year_start',
    'REGNSKABSÅR_SLUT': 'financial_year_end',
    'KAPITAL': 'share_capital',
    'KAPITALVALUTA': 'capital_currency',
    'PSEUDOCVRNR': 'pseudo_cvr_number',
    'ARKIV_REGISTRERINGSNUMMER': 'archive_registration_number',
}
