view: obligors {
  sql_table_name: `sme_test.deeploans_sme_silver_obligors`
    ;;

   drill_fields: [source*]


  dimension_group: pool_cutoff {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.AS1 ;;
  }

  dimension: country {
    type: string
    sql: ${TABLE}.AS15 ;;
  }

  dimension: postcode {
    type: string
    sql: ${TABLE}.AS16 ;;
  }

  dimension: geographic_region {
    type: string
    sql: ${TABLE}.AS17 ;;
  }

  dimension: business_type {
    type: string
    case: {
      when: {
        sql: ${TABLE}.AS18 = "1" ;;
        label: "public company"
      }
      when: {
        sql: ${TABLE}.AS18 = "2" ;;
        label: "limited company"
      }
      when: {
        sql: ${TABLE}.AS18 = "3" ;;
        label: "partnership"
      }
      when: {
        sql: ${TABLE}.AS18 = "4" ;;
        label: "individual"
      }
      when: {
        sql: ${TABLE}.AS18 = "5" ;;
        label: "other"
      }
      else: "unknown"
    }
  }

  dimension_group: obligor_incorporation_date{
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.AS19 ;;
  }

  dimension: pool_identifier {
    type: string
    sql: ${TABLE}.AS2 ;;
  }

  dimension_group: obligor_is_a_customer_since {
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.AS20 ;;
  }

  dimension: customer_segment {
    type: string
    sql: ${TABLE}.AS21 ;;
  }

  dimension: borrower_basel_III_segment {
    type: string
    case: {
      when: {
        sql: ${TABLE}.AS22 = "1" ;;
        label: "corporate"
      }
      when: {
        sql: ${TABLE}.AS22 = "2" ;;
        label: "SME treated as corporate"
      }
      when: {
        sql: ${TABLE}.AS22 = "3" ;;
        label: "retail"
      }
      when: {
        sql: ${TABLE}.AS22 = "4" ;;
        label: "other"
      }
      else: "unknown"
    }
  }

  dimension: originator_affiliate {
    type: yesno
    sql: ${TABLE}.AS23 ;;
  }

  dimension: obligor_tax_code {
    type: string
    sql: ${TABLE}.AS24 ;;
  }

  dimension: asset_type {
    type: string
    case: {
      when: {
        sql: ${TABLE}.AS25 = "1" ;;
        label: "loan"
      }
      when: {
        sql: ${TABLE}.AS25 = "2" ;;
        label: "guarantee"
      }
      when: {
        sql: ${TABLE}.AS25 = "3" ;;
        label: "promissory note"
      }
      when: {
        sql: ${TABLE}.AS25 = "4" ;;
        label: "partecipation rights"
      }
      when: {
        sql: ${TABLE}.AS25 = "5" ;;
        label: "overdraft"
      }
      when: {
        sql: ${TABLE}.AS25 = "6" ;;
        label: "letter of credit"
      }
      when: {
        sql: ${TABLE}.AS25 = "7" ;;
        label: "working capital facility"
      }
      when: {
        sql: ${TABLE}.AS25 = "8" ;;
        label: "other"
      }
      else: "unknown"
    }
  }

  dimension: seniority {
    type: string
    case: {
      when: {
        sql: ${TABLE}.AS26 = "1" ;;
        label: "senior secured"
      }
      when: {
        sql: ${TABLE}.AS26 = "2" ;;
        label: "senior unsecured"
      }
      when: {
        sql: ${TABLE}.AS26 = "3" ;;
        label: "junior"
      }
      when: {
        sql: ${TABLE}.AS26 = "4" ;;
        label: "junior unsecured"
      }
      when: {
        sql: ${TABLE}.AS26 = "5" ;;
        label: "other"
      }
      else: "unknown"
    }
  }

  dimension: tot_credit_limit_granted_to_loan {
    type: number
    sql: ${TABLE}.AS27 ;;
  }

  dimension: tot_credit_limit_used {
    type: number
    sql: ${TABLE}.AS28 ;;
  }

  dimension: syndicated {
    type: yesno
    sql: ${TABLE}.AS29 ;;
  }

  dimension: loan_identifier {
    type: string
    primary_key: yes
    sql: ${TABLE}.AS3 ;;
  }

  dimension: bank_internal_rating {
    type: number
    sql: ${TABLE}.AS30 ;;
  }

  dimension_group: last_internal_obligor_rating_review {
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.AS31 ;;
  }

  dimension: sp_public_rating {
    type: string
    sql: ${TABLE}.AS32 ;;
  }

  dimension: moody_public_rating {
    type: string
    sql: ${TABLE}.AS33 ;;
  }

  dimension: fitch_public_rating {
    type: string
    sql: ${TABLE}.AS34 ;;
  }

  dimension: dbrs_public_rating_ {
    type: string
    sql: ${TABLE}.AS35 ;;
  }

  dimension: other_public_rating {
    type: string
    sql: ${TABLE}.AS36 ;;
  }

  dimension: bank_internal_LGD_estimate {
    type: number
    sql: ${TABLE}.AS37 ;;
  }

  dimension: bank_internal_LGD_estimate_downturn {
    type: number
    sql: ${TABLE}.AS38 ;;
  }

  dimension: sp_industry_code {
    type: number
    sql: ${TABLE}.AS39 ;;
  }

  dimension: originator {
    type: string
    sql: ${TABLE}.AS4 ;;
  }

  dimension: moody_industry_code {
    type: number
    sql: ${TABLE}.AS40 ;;
  }

  dimension: fitch_industry_code {
    type: number
    sql: ${TABLE}.AS41 ;;
  }

  dimension: nace_industry_code {
    type: string
    sql: ${TABLE}.AS42 ;;
  }

  dimension: other_industry_code {
    type: string
    sql: ${TABLE}.AS43 ;;
  }

  dimension: borrower_deposit_amount {
    type: number
    sql: ${TABLE}.AS44 ;;
  }

  dimension: borrower_deposit_currency {
    type: string
    sql: ${TABLE}.AS45 ;;
  }

  dimension: service_identifier {
    type: string
    sql: ${TABLE}.AS5 ;;
  }

  dimension: servicer_name {
    type: string
    sql: ${TABLE}.AS6 ;;
  }

  dimension: borrower_identifier {
    type: string
    sql: ${TABLE}.AS7 ;;
  }

  dimension: group_company_identifier {
    type: string
    sql: ${TABLE}.AS8 ;;
  }

  dimension: dl_code {
    type: string
    sql: ${TABLE}.dl_code ;;
  }


  measure: count {
    type: count
    drill_fields: []
  }

  measure: tot_credit_limit_granted_to_loan_min {
    type:  min
    sql: ${tot_credit_limit_granted_to_loan} ;;
  }

  measure: tot_credit_limit_granted_to_loan_max {
    type:  max
    sql: ${tot_credit_limit_granted_to_loan} ;;
  }

  measure: tot_credit_limit_granted_to_loan_avg {
    type:  average
    sql: ${tot_credit_limit_granted_to_loan} ;;
  }

  measure: tot_credit_limit_used_min {
    type:  min
    sql: ${tot_credit_limit_used} ;;
  }

  measure: tot_credit_limit_used_max {
    type:  max
    sql: ${tot_credit_limit_used} ;;
  }

  measure: tot_credit_limit_used_avg {
    type:  average
    sql: ${tot_credit_limit_used} ;;
  }

  measure: bank_internal_LGD_estimate_min {
    type:  min
    sql: ${bank_internal_LGD_estimate} ;;
  }

  measure: bank_internal_LGD_estimate_max {
    type:  max
    sql: ${bank_internal_LGD_estimate} ;;
  }

  measure: bank_internal_LGD_estimate_avg {
    type:  average
    sql: ${bank_internal_LGD_estimate} ;;
  }

  measure: bank_internal_LGD_estimate_downturn_min {
    type:  min
    sql: ${bank_internal_LGD_estimate_downturn} ;;
  }

  measure: bank_internal_LGD_estimate_downturn_max {
    type:  max
    sql: ${bank_internal_LGD_estimate_downturn} ;;
  }

  measure: bank_internal_LGD_estimate_downturn_avg {
    type:  average
    sql: ${bank_internal_LGD_estimate_downturn} ;;
  }

  measure: borrower_deposit_amount_min {
    type:  min
    sql: ${borrower_deposit_amount} ;;
  }

  measure: borrower_deposit_amount_max {
    type:  max
    sql: ${borrower_deposit_amount} ;;
  }

  measure: borrower_deposit_amount_avg {
    type:  average
    sql: ${borrower_deposit_amount} ;;
  }

  set: source {
    fields: [dl_code,
      pool_cutoff_date,
      pool_identifier,
      loan_identifier,
      originator,
      service_identifier,
      servicer_name,
      borrower_identifier,
      group_company_identifier]
  }
}
