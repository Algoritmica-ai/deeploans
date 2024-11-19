view: performances {
  sql_table_name: `sme_test.deeploans_sme_silver_performances`
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

  dimension: interest_arrears_amount {
    type: number
    sql: ${TABLE}.AS115 ;;
  }

  dimension: days_in_interest_arrears {
    type: number
    sql: ${TABLE}.AS116 ;;
  }

  dimension: principal_arrears_amount {
    type: number
    sql: ${TABLE}.AS117 ;;
  }

  dimension: days_in_principal_arrears {
    type: number
    sql: ${TABLE}.AS118 ;;
  }

  dimension: loan_entered_arrears {
    type: number
    sql: ${TABLE}.AS119 ;;
  }

  dimension: days_in_arrears_prior {
    type: number
    sql: ${TABLE}.AS120 ;;
  }

  dimension: default_or_foreclosure_on_loan_per_transaction_definition {
    type: yesno
    sql: ${TABLE}.AS121 ;;
  }

  dimension: default_or_foreclosure_on_loan_per_basel_III_definition{
    type: yesno
    sql: ${TABLE}.AS122 ;;
  }

  dimension: reason_for_default_basel_II {
    type: string
    case: {
      when: {
        sql: ${TABLE}.AS123 = "1" ;;
        label: "bankruptcy/insolvency"
      }
      when: {
        sql: ${TABLE}.AS123 = "2" ;;
        label: "failure to pay"
      }
      when: {
        sql: ${TABLE}.AS123 = "3" ;;
        label: "breach of terms"
      }
      when: {
        sql: ${TABLE}.AS123 = "4" ;;
        label: "other"
      }
      else: "unknown"
    }
  }

  dimension_group: default {
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.AS124 ;;
  }

  dimension: default_amount {
    type: number
    sql: ${TABLE}.AS125 ;;
  }

  dimension: bank_internal_rating_prior_default {
    type: number
    sql: ${TABLE}.AS126 ;;
  }

  dimension_group: legal_proceedings_start {
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.AS127 ;;
  }

  dimension: cumulative_recovery {
    type: number
    sql: ${TABLE}.AS128 ;;
  }

  dimension: recovery_source {
    type: string
    case: {
      when: {
        sql: ${TABLE}.AS129 = "1" ;;
        label: "liquidation of collateral"
      }
      when: {
        sql: ${TABLE}.AS129 = "2" ;;
        label: "enforcements of guarantees"
      }
      when: {
        sql: ${TABLE}.AS129 = "3" ;;
        label: "additional lending"
      }
      when: {
        sql: ${TABLE}.AS129 = "4" ;;
        label: "cash recoveries"
      }
      when: {
        sql: ${TABLE}.AS129 = "5" ;;
        label: "mixed"
      }
      when: {
        sql: ${TABLE}.AS129 = "6" ;;
        label: "other"
      }
      else: "unknown"
    }
  }

  dimension_group: workout_process_started {
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.AS130 ;;
  }

  dimension: workout_process_completed {
    type: yesno
    sql: ${TABLE}.AS131 ;;
  }

  dimension: allocated_losses {
    type: number
    sql: ${TABLE}.AS132 ;;
  }

  dimension_group: redemption {
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.AS133 ;;
  }

  dimension_group: date_loss_allocated {
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.AS134 ;;
  }

  dimension: real_estate_sale_price {
    type: number
    sql: ${TABLE}.AS135 ;;
  }

  dimension: tot_proceeds_from_other_collateral_or_guarantees {
    type: number
    sql: ${TABLE}.AS136 ;;
  }

  dimension_group: date_of_end_of_workout{
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.AS137 ;;
  }

  dimension: foreclosure_cost {
    type: number
    sql: ${TABLE}.AS138 ;;
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

  dimension: dl_code {
    type: string
    sql: ${TABLE}.dl_code ;;
  }


  measure: count {
    type: count
    drill_fields: []
  }

  measure: interest_arrears_amount_min {
    type:  min
    sql: ${interest_arrears_amount} ;;
  }

  measure: interest_arrears_amount_max {
    type:  max
    sql: ${interest_arrears_amount} ;;
  }

  measure: interest_arrears_amount_avg {
    type:  average
    sql: ${interest_arrears_amount} ;;
  }

  measure: days_in_interest_arrears_min {
    type:  min
    sql: ${days_in_interest_arrears} ;;
  }

  measure: days_in_interest_arrears_max {
    type:  max
    sql: ${days_in_interest_arrears} ;;
  }

  measure: days_in_interest_arrears_avg {
    type:  average
    sql: ${days_in_interest_arrears} ;;
  }

  measure: principal_arrears_amount_min {
    type:  min
    sql: ${principal_arrears_amount} ;;
  }

  measure: principal_arrears_amount_max {
    type:  max
    sql: ${principal_arrears_amount} ;;
  }

  measure: principal_arrears_amount_avg {
    type:  average
    sql: ${principal_arrears_amount} ;;
  }
  measure: days_in_principal_arrears_min {
    type:  min
    sql: ${days_in_principal_arrears} ;;
  }

  measure: days_in_principal_arrears_max {
    type:  max
    sql: ${days_in_principal_arrears} ;;
  }

  measure: days_in_principal_arrears_avg {
    type:  average
    sql: ${days_in_principal_arrears} ;;
  }
  measure: loan_entered_arrears_min {
    type:  min
    sql: ${loan_entered_arrears} ;;
  }

  measure: loan_entered_arrears_max {
    type:  max
    sql: ${loan_entered_arrears} ;;
  }

  measure: loan_entered_arrears_avg {
    type:  average
    sql: ${loan_entered_arrears} ;;
  }
  measure: days_in_arrears_prior_min {
    type:  min
    sql: ${days_in_arrears_prior} ;;
  }

  measure: days_in_arrears_prior_max {
    type:  max
    sql: ${days_in_arrears_prior} ;;
  }

  measure: days_in_arrears_prior_avg {
    type:  average
    sql: ${days_in_arrears_prior} ;;
  }
  measure: default_amount_min {
    type:  min
    sql: ${default_amount} ;;
  }

  measure: default_amount_max {
    type:  max
    sql: ${default_amount} ;;
  }

  measure: default_amount_avg {
    type:  average
    sql: ${default_amount} ;;
  }
  measure: cumulative_recovery_min {
    type:  min
    sql: ${cumulative_recovery} ;;
  }

  measure: cumulative_recovery_max {
    type:  max
    sql: ${cumulative_recovery} ;;
  }

  measure: cumulative_recovery_avg {
    type:  average
    sql: ${cumulative_recovery} ;;
  }
  measure: allocated_losses_min {
    type:  min
    sql: ${allocated_losses} ;;
  }

  measure: allocated_losses_max {
    type:  max
    sql: ${allocated_losses} ;;
  }

  measure: allocated_losses_avg {
    type:  average
    sql: ${allocated_losses} ;;
  }
  measure: real_estate_sale_price_min {
    type:  min
    sql: ${real_estate_sale_price} ;;
  }

  measure: real_estate_sale_price_max {
    type:  max
    sql: ${real_estate_sale_price} ;;
  }

  measure: real_estate_sale_price_avg {
    type:  average
    sql: ${real_estate_sale_price} ;;
  }
  measure: tot_proceeds_from_other_collateral_or_guarantees_min {
    type:  min
    sql: ${tot_proceeds_from_other_collateral_or_guarantees} ;;
  }

  measure: tot_proceeds_from_other_collateral_or_guarantees_max {
    type:  max
    sql: ${tot_proceeds_from_other_collateral_or_guarantees} ;;
  }

  measure: tot_proceeds_from_other_collateral_or_guarantees_avg {
    type:  average
    sql: ${tot_proceeds_from_other_collateral_or_guarantees} ;;
  }
  measure: foreclosure_cost_min {
    type:  min
    sql: ${foreclosure_cost} ;;
  }

  measure: foreclosure_cost_max {
    type:  max
    sql: ${foreclosure_cost} ;;
  }

  measure: foreclosure_cost_avg {
    type:  average
    sql: ${foreclosure_cost} ;;
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
