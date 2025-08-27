view: bond_tranches {
  sql_table_name: `deeploans_sme_silver.bond_tranches`
    ;;

  dimension: primary_key {
    hidden:  yes
    primary_key: yes
    sql: CONCAT(${report_date},${issuer}) ;;
  }

  dimension_group: report {
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
    sql: ${TABLE}.BS1 ;;
  }

  dimension: issuer {
    type: string
    sql: ${TABLE}.BS2 ;;
  }

  dimension: bond_class_name {
    type: string
    sql: ${TABLE}.BS25 ;;
  }

  dimension: international_securities_identification_number {
    type: string
    sql: ${TABLE}.BS26 ;;
  }

  dimension_group: interest_payment {
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.BS27 ;;
  }

  dimension_group: principal_payment {
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.BS28 ;;
  }

  dimension: bond_currency {
    type: string
    sql: ${TABLE}.BS29 ;;
  }

  dimension: original_principal_balance {
    type: number
    sql: ${TABLE}.BS30 ;;
  }

  dimension: tot_ending_balance_subsequent_to_payment {
    type: number
    sql: ${TABLE}.BS31 ;;
  }

  dimension: reference_rate {
    type: string
    sql: ${TABLE}.BS32 ;;
  }

  dimension: relevant_margin {
    type: number
    sql: ${TABLE}.BS33 ;;
  }

  dimension: cupon_reference_rate {
    type: number
    sql: ${TABLE}.BS34 ;;
  }

  dimension: current_coupon {
    type: number
    sql: ${TABLE}.BS35 ;;
  }

  dimension: cum_interest_shortfalls {
    type: number
    sql: ${TABLE}.BS36 ;;
  }

  dimension: cum_principal_shortfalls {
    type: number
    sql: ${TABLE}.BS37 ;;
  }

  dimension_group: legal_maturity {
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.BS38 ;;
  }

  dimension_group: bond_issue {
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.BS39 ;;
  }

  dimension: dl_code {
    type: string
    sql: ${TABLE}.dl_code ;;
  }


  measure: count {
    type: count
    drill_fields: []
  }

  measure: original_principal_balance_min {
    type:  min
    sql: ${original_principal_balance} ;;
  }

  measure: original_principal_balance_max {
    type:  max
    sql: ${original_principal_balance} ;;
  }

  measure: original_principal_balance_avg {
    type:  average
    sql: ${original_principal_balance} ;;
  }

  measure: tot_ending_balance_subsequent_to_payment_min {
    type:  min
    sql: ${tot_ending_balance_subsequent_to_payment} ;;
  }

  measure: tot_ending_balance_subsequent_to_payment_max {
    type:  max
    sql: ${tot_ending_balance_subsequent_to_payment} ;;
  }

  measure: tot_ending_balance_subsequent_to_payment_avg {
    type:  average
    sql: ${tot_ending_balance_subsequent_to_payment} ;;
  }
}
