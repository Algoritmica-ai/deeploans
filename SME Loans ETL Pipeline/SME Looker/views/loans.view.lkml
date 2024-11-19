view: loans {
  sql_table_name: `sme_test.deeploans_sme_silver_loans`
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

#  dimension_group: loan_origination {
#    type: time
#    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
#    sql: TIMESTAMP(${TABLE}.AS50) ;;
#  }

  dimension: loan_origination {
    type: string
    #timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.AS50 ;;
  }

  dimension_group: final_maturity {
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.AS51 ;;
  }

  dimension: loan_denomination_currency {
    type: string
    sql: ${TABLE}.AS52 ;;
  }

  dimension: loan_hedged {
    type: yesno
    sql: ${TABLE}.AS53 ;;
  }

  dimension: original_loan_balance {
    type: number
    sql: ${TABLE}.AS54 ;;
  }

  dimension: current_balance {
    type: number
    sql: ${TABLE}.AS55 ;;
  }

  dimension: securitised_loan_amount {
    type: number
    sql: ${TABLE}.AS56 ;;
  }

  dimension: purpose {
    type: string
    case: {
      when: {
        sql: ${TABLE}.AS57 = "1" ;;
        label: "purchase"
      }
      when: {
        sql: ${TABLE}.AS57 = "2" ;;
        label: "re-mortgage"
      }
      when: {
        sql: ${TABLE}.AS57 = "3" ;;
        label: "renovation"
      }
      when: {
        sql: ${TABLE}.AS57 = "4" ;;
        label: "equity release"
      }
      when: {
        sql: ${TABLE}.AS57 = "5" ;;
        label: "construction real estate"
      }
      when: {
        sql: ${TABLE}.AS57 = "6" ;;
        label: "construction other"
      }
      when: {
        sql: ${TABLE}.AS57 = "7" ;;
        label: "debt consolidation"
      }
      when: {
        sql: ${TABLE}.AS57 = "8" ;;
        label: "re-mortgage with equity release"
      }
      when: {
        sql: ${TABLE}.AS57 = "9" ;;
        label: "re-mortgage on different terms"
      }
      when: {
        sql: ${TABLE}.AS57 = "10" ;;
        label: "combination mortgage"
      }
      when: {
        sql: ${TABLE}.AS57 = "11" ;;
        label: "investment mortgage"
      }
      when: {
        sql: ${TABLE}.AS57 = "12" ;;
        label: "working capital"
      }
      when: {
        sql: ${TABLE}.AS57 = "13" ;;
        label: "other"
      }

      else: "unknown"
    }
  }

  dimension: principal_payment_frequency {
    type: string
    case: {
      when: {
        sql: ${TABLE}.AS58 = "1" ;;
        label: "monthly"
      }
      when: {
        sql: ${TABLE}.AS58 = "2" ;;
        label: "quarterly"
      }
      when: {
        sql: ${TABLE}.AS58 = "3" ;;
        label: "semi annual"
      }
      when: {
        sql: ${TABLE}.AS58 = "4" ;;
        label: "annual"
      }
      when: {
        sql: ${TABLE}.AS58 = "5" ;;
        label: "bullet"
      }
      when: {
        sql: ${TABLE}.AS58 = "6" ;;
        label: "other"
      }
      else: "unknown"
    }
  }

  dimension: interest_payment_frequency {
    type: string
        case: {
          when: {
            sql: ${TABLE}.AS59 = "1" ;;
            label: "monthly"
          }
          when: {
            sql: ${TABLE}.AS59 = "2" ;;
            label: "quarterly"
          }
          when: {
            sql: ${TABLE}.AS59 = "3" ;;
            label: "semi annual"
          }
          when: {
            sql: ${TABLE}.AS59 = "4" ;;
            label: "annual"
          }
          when: {
            sql: ${TABLE}.AS59 = "5" ;;
            label: "bullet"
          }
          when: {
            sql: ${TABLE}.AS59 = "6" ;;
            label: "other"
          }
          else: "unknown"
        }
  }

  dimension: servicer_name {
    type: string
    sql: ${TABLE}.AS6 ;;
  }

  dimension: max_balance {
    type: number
    sql: ${TABLE}.AS60 ;;
  }

  dimension: weighted_avg_life {
    type: number
    sql: ${TABLE}.AS61 ;;
  }

  dimension: amortisation_type {
    type: string
    case: {
      when: {
        sql: ${TABLE}.AS62 = "1" ;;
        label: "linear"
      }
      when: {
        sql: ${TABLE}.AS62 = "2" ;;
        label: "french"
      }
      when: {
        sql: ${TABLE}.AS62 = "3" ;;
        label: "fix amortisation schedule"
      }
      when: {
        sql: ${TABLE}.AS62 = "4" ;;
        label: "bullet"
      }
      when: {
        sql: ${TABLE}.AS62 = "5" ;;
        label: "partial bullet"
      }
      when: {
        sql: ${TABLE}.AS62 = "6" ;;
        label: "revolving"
      }
      when: {
        sql: ${TABLE}.AS62 = "7" ;;
        label: "other"
      }
      else: "unknown"
    }
  }

  dimension: regular_principal_instalment {
    type: number
    sql: ${TABLE}.AS63 ;;
  }

  dimension: regular_interest_instalment {
    type: number
    sql: ${TABLE}.AS64 ;;
  }

  dimension: type_of_loan {
    type: string
    case: {
      when: {
        sql: ${TABLE}.AS65 = "1" ;;
        label: "term"
      }
      when: {
        sql: ${TABLE}.AS65 = "2" ;;
        label: "revolving credit line"
      }
      when: {
        sql: ${TABLE}.AS65 = "3" ;;
        label: "other"
      }
      else: "unknown"
    }
  }

  dimension: balloon_amount {
    type: number
    sql: ${TABLE}.AS66 ;;
  }

  dimension_group: next_payment {
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.AS67 ;;
  }

  dimension: payment_type {
    type: string
    case: {
      when: {
        sql: ${TABLE}.AS68 = "1" ;;
        label: "direct debit"
      }
      when: {
        sql: ${TABLE}.AS68 = "2" ;;
        label: "standing order"
      }
      when: {
        sql: ${TABLE}.AS68 = "3" ;;
        label: "cheque"
      }
      when: {
        sql: ${TABLE}.AS68 = "4" ;;
        label: "cash"
      }
      when: {
        sql: ${TABLE}.AS68 = "5" ;;
        label: "other"
      }
      else: "unknown"
    }
  }

  dimension: prepayment_penalty {
    type: number
    sql: ${TABLE}.AS69 ;;
  }

  dimension: borrower_identifier {
    type: string
    sql: ${TABLE}.AS7 ;;
  }

  dimension_group: principal_grace_period_end {
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.AS70 ;;
  }

  dimension_group: interest_grace_period_end {
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.AS71 ;;
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

  measure: original_loan_balance_min {
    type:  min
    sql: ${original_loan_balance} ;;
  }

  measure: original_loan_balance_max {
    type:  max
    sql: ${original_loan_balance} ;;
  }

  measure: original_loan_balancen_avg {
    type:  average
    sql: ${original_loan_balance} ;;
  }

  measure: current_balance_min {
    type:  min
    sql: ${current_balance} ;;
  }

  measure: current_balance_max {
    type:  max
    sql: ${current_balance} ;;
  }

  measure: current_balance_avg {
    type:  average
    sql: ${current_balance} ;;
  }

  measure: securitised_loan_amount_min {
    type:  min
    sql: ${securitised_loan_amount} ;;
  }

  measure: securitised_loan_amount_max {
    type:  max
    sql: ${securitised_loan_amount} ;;
  }

  measure: securitised_loan_amount_avg {
    type:  average
    sql: ${securitised_loan_amount} ;;
  }
  measure: max_balance_min {
    type:  min
    sql: ${max_balance} ;;
  }

  measure: max_balance_max {
    type:  max
    sql: ${max_balance} ;;
  }

  measure: max_balance_avg {
    type:  average
    sql: ${max_balance} ;;
  }
  measure: weighted_avg_life_min {
    type:  min
    sql: ${weighted_avg_life} ;;
  }

  measure: weighted_avg_life_max {
    type:  max
    sql: ${weighted_avg_life} ;;
  }

  measure: weighted_avg_life_avg {
    type:  average
    sql: ${weighted_avg_life} ;;
  }
  measure: regular_principal_instalment_min {
    type:  min
    sql: ${regular_principal_instalment} ;;
  }

  measure: regular_principal_instalment_max {
    type:  max
    sql: ${regular_principal_instalment} ;;
  }

  measure: regular_principal_instalment_avg {
    type:  average
    sql: ${regular_principal_instalment} ;;
  }
  measure: regular_interest_instalment_min {
    type:  min
    sql: ${regular_interest_instalment} ;;
  }

  measure: regular_interest_instalment_max {
    type:  max
    sql: ${regular_interest_instalment} ;;
  }

  measure: regular_interest_instalment_avg {
    type:  average
    sql: ${regular_interest_instalment} ;;
  }
  measure: balloon_amount_min {
    type:  min
    sql: ${balloon_amount} ;;
  }

  measure: balloon_amount_max {
    type:  max
    sql: ${balloon_amount} ;;
  }

  measure: balloon_amount_avg {
    type:  average
    sql: ${balloon_amount} ;;
  }
  measure: prepayment_penalty_min {
    type:  min
    sql: ${prepayment_penalty} ;;
  }

  measure: prepayment_penalty_max {
    type:  max
    sql: ${prepayment_penalty} ;;
  }

  measure: prepayment_penalty_avg {
    type:  average
    sql: ${prepayment_penalty} ;;
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
