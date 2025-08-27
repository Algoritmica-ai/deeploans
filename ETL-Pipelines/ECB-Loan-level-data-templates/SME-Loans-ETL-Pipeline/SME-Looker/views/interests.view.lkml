view: interests {
  sql_table_name: `sme_test.deeploans_sme_silver_interests`
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

  dimension: pool_identifier {
    type: string
    sql: ${TABLE}.AS2 ;;
  }

  dimension: loan_identifier {
    type: string
    primary_key: yes
    sql: ${TABLE}.AS3 ;;
  }

  dimension: originator {
    type: string
    sql: ${TABLE}.AS4 ;;
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

  dimension: current_interest_rate {
    type: number
    sql: ${TABLE}.AS80 ;;
  }

  dimension: interest_cap_rate {
    type: number
    sql: ${TABLE}.AS81 ;;
  }

  dimension: interest_floor_rate {
    type: number
    sql: ${TABLE}.AS82 ;;
  }

  dimension: interest_rate_type {
    type: string
    case: {
      when: {
        sql: ${TABLE}.AS83 = "1" ;;
        label: "floating rate loan"
      }
      when: {
        sql: ${TABLE}.AS83 = "2" ;;
        label: "floating rate loan linked to Libor, Euribor, BoE reverting to the Bank's SVR, ECB reverting to Bank’s SVR"
      }
      when: {
        sql: ${TABLE}.AS83 = "3" ;;
        label: "fixed rate loan"
      }
      when: {
        sql: ${TABLE}.AS83 = "4" ;;
        label: "fixed with future periodic resets"
      }
      when: {
        sql: ${TABLE}.AS83 = "5" ;;
        label: "fixed rate loan with compulsory future switch to floating"
      }
      when: {
        sql: ${TABLE}.AS83 = "6" ;;
        label: "capped"
      }
      when: {
        sql: ${TABLE}.AS83 = "7" ;;
        label: "discount"
      }
      when: {
        sql: ${TABLE}.AS83 = "8" ;;
        label: "switch optionality"
      }
      when: {
        sql: ${TABLE}.AS83 = "9" ;;
        label: "borrower swapped"
      }
      when: {
        sql: ${TABLE}.AS83 = "10" ;;
        label: "other"
      }
      else: "unknown"
    }
  }

  dimension: current_interest_rate_index {
    type: string
    case: {
      when: {
        sql: ${TABLE}.AS84 = "1" ;;
        label: "1 month LIBOR"
      }
      when: {
        sql: ${TABLE}.AS84 = "2" ;;
        label: "1 month EURIBOR"
      }
      when: {
        sql: ${TABLE}.AS84 = "3" ;;
        label: "3 months LIBOR"
      }
      when: {
        sql: ${TABLE}.AS84 = "4" ;;
        label: "3 months EURIBOR"
      }
      when: {
        sql: ${TABLE}.AS84 = "5" ;;
        label: "6 months LIBOR"
      }
      when: {
        sql: ${TABLE}.AS84 = "6" ;;
        label: "6 months EURIBOR"
      }
      when: {
        sql: ${TABLE}.AS84 = "7" ;;
        label: "12 months LIBOR"
      }
      when: {
        sql: ${TABLE}.AS84 = "8" ;;
        label: "12 months EURIBOR"
      }
      when: {
        sql: ${TABLE}.AS84 = "9" ;;
        label: "BoE base rate"
      }
      when: {
        sql: ${TABLE}.AS84 = "10" ;;
        label: "ECB base rate"
      }
      when: {
        sql: ${TABLE}.AS84 = "11" ;;
        label: "standard variable rate"
      }
      when: {
        sql: ${TABLE}.AS84 = "12" ;;
        label: "other"
      }
      else: "unknown"
    }
  }

  dimension: current_interest_rate_margin {
    type: number
    sql: ${TABLE}.AS85 ;;
  }

  dimension: revision_margin_1 {
    type: number
    sql: ${TABLE}.AS86 ;;
  }

  dimension_group: interest_revision_date_1 {
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.AS87 ;;
  }

  dimension: revision_margin_2 {
    type: number
    sql: ${TABLE}.AS88 ;;
  }

  dimension_group: interest_revision_date_2 {
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.AS89 ;;
  }

  dimension: revision_margin_3 {
    type: number
    sql: ${TABLE}.AS90 ;;
  }

  dimension_group: interest_revision_date_3 {
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.AS91 ;;
  }

  dimension: revised_interest_index {
    type: string
    sql: ${TABLE}.AS92 ;;
  }

  dimension: final_margin {
    type: number
    sql: ${TABLE}.AS93 ;;
  }

  dimension: interest_rate_period {
    type: string
    case: {
      when: {
        sql: ${TABLE}.AS94 = "1" ;;
        label: "annual"
      }
      when: {
        sql: ${TABLE}.AS94 = "2" ;;
        label: "semi-annual"
      }
      when: {
        sql: ${TABLE}.AS94 = "3" ;;
        label: "quarterly"
      }
      when: {
        sql: ${TABLE}.AS94 = "4" ;;
        label: "monthly"
      }
      when: {
        sql: ${TABLE}.AS94 = "5" ;;
        label: "not apply"
      }
      when: {
        sql: ${TABLE}.AS94 = "6" ;;
        label: "other"
      }
      else: "unknown"
    }
  }

  dimension: dl_code {
    type: string
    sql: ${TABLE}.dl_code ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }

  measure: current_interest_rate_min {
    type:  min
    sql: ${current_interest_rate} ;;
  }

  measure: current_interest_rate_max {
    type:  max
    sql: ${current_interest_rate} ;;
  }

  measure: current_interest_rate_avg {
    type:  average
    sql: ${current_interest_rate} ;;
  }

  measure: interest_cap_rate_min {
    type:  min
    sql: ${interest_cap_rate} ;;
  }

  measure: interest_cap_rate_max {
    type:  max
    sql: ${interest_cap_rate} ;;
  }

  measure: interest_cap_rate_avg {
    type:  average
    sql: ${interest_cap_rate} ;;
  }

  measure: interest_floor_rate_min {
    type:  min
    sql: ${interest_floor_rate} ;;
  }

  measure: interest_floor_rate_max {
    type:  max
    sql: ${interest_floor_rate} ;;
  }

  measure: interest_floor_rate_avg {
    type:  average
    sql: ${interest_floor_rate} ;;
  }

  measure: current_interest_rate_margin_min {
    type:  min
    sql: ${current_interest_rate_margin} ;;
  }

  measure: current_interest_rate_margin_max {
    type:  max
    sql: ${current_interest_rate_margin} ;;
  }

  measure: current_interest_rate_margin_avg {
    type:  average
    sql: ${current_interest_rate_margin} ;;
  }

  measure: final_margin_min {
    type:  min
    sql: ${final_margin} ;;
  }

  measure: final_margin_max {
    type:  max
    sql: ${final_margin} ;;
  }

  measure: final_margin_avg {
    type:  average
    sql: ${final_margin} ;;
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
