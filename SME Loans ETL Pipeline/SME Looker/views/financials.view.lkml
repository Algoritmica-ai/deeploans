view: financials {
  sql_table_name: `sme_test.deeploans_sme_silver_financials`
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

  dimension: turnover_of_obligor {
    type: number
    sql: ${TABLE}.AS100 ;;
  }

  dimension: equity {
    type: number
    sql: ${TABLE}.AS101 ;;
  }

  dimension: total_liability {
    type: number
    sql: ${TABLE}.AS102 ;;
  }

  dimension: short_term_financial_debt {
    type: number
    sql: ${TABLE}.AS103 ;;
  }

  dimension: commercial_liabilities {
    type: number
    sql: ${TABLE}.AS104 ;;
  }

  dimension: long_term_debt {
    type: number
    sql: ${TABLE}.AS105 ;;
  }

  dimension: financial_expenses {
    type: number
    sql: ${TABLE}.AS106 ;;
  }

  dimension: EBITDA {
    type: number
    sql: ${TABLE}.AS107 ;;
  }

  dimension: EBIT {
    type: number
    sql: ${TABLE}.AS108 ;;
  }

  dimension: net_profit {
    type: number
    sql: ${TABLE}.AS109 ;;
  }

  dimension: number_of_employees {
    type: number
    sql: ${TABLE}.AS110 ;;
  }

  dimension: currency_of_financials {
    type: string
    sql: ${TABLE}.AS111 ;;
  }

  dimension_group: date_of_financials {
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
    sql: ${TABLE}.AS112 ;;
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

  measure: turnover_of_obligor_min {
    type:  min
    sql: ${turnover_of_obligor} ;;
  }

  measure: turnover_of_obligor_max {
    type:  max
    sql: ${turnover_of_obligor} ;;
  }

  measure: turnover_of_obligor_avg {
    type:  average
    sql: ${turnover_of_obligor} ;;
  }

  measure: equity_min {
    type:  min
    sql: ${equity} ;;
  }

  measure: equity_max {
    type:  max
    sql: ${equity} ;;
  }

  measure: equity_avg {
    type:  average
    sql: ${equity} ;;
  }

  measure: total_liability_min {
    type:  min
    sql: ${total_liability} ;;
  }

  measure: total_liability_max {
    type:  max
    sql: ${total_liability} ;;
  }

  measure: total_liability_avg {
    type:  average
    sql: ${total_liability} ;;
  }
  measure: short_term_financial_debt_min {
    type:  min
    sql: ${short_term_financial_debt} ;;
  }

  measure: short_term_financial_debt_max {
    type:  max
    sql: ${short_term_financial_debt} ;;
  }

  measure: short_term_financial_debt_avg {
    type:  average
    sql: ${short_term_financial_debt} ;;
  }
  measure: commercial_liabilities_min {
    type:  min
    sql: ${commercial_liabilities} ;;
  }

  measure: commercial_liabilities_max {
    type:  max
    sql: ${commercial_liabilities} ;;
  }

  measure: commercial_liabilities_avg {
    type:  average
    sql: ${commercial_liabilities} ;;
  }
  measure: long_term_debt_min {
    type:  min
    sql: ${long_term_debt} ;;
  }

  measure: long_term_debt_max {
    type:  max
    sql: ${long_term_debt} ;;
  }

  measure: long_term_debt_avg {
    type:  average
    sql: ${long_term_debt} ;;
  }
  measure: financial_expenses_min {
    type:  min
    sql: ${financial_expenses} ;;
  }

  measure: financial_expenses_max {
    type:  max
    sql: ${financial_expenses} ;;
  }

  measure: financial_expenses_avg {
    type:  average
    sql: ${financial_expenses} ;;
  }
  measure: EBITDA_min {
    type:  min
    sql: ${EBITDA} ;;
  }

  measure: EBITDA_max {
    type:  max
    sql: ${EBITDA} ;;
  }

  measure: EBITDA_avg {
    type:  average
    sql: ${EBITDA} ;;
  }
  measure: EBIT_min {
    type:  min
    sql: ${EBIT} ;;
  }

  measure: EBIT_max {
    type:  max
    sql: ${EBIT} ;;
  }

  measure: EBIT_avg {
    type:  average
    sql: ${EBIT} ;;
  }
  measure: net_profit_min {
    type:  min
    sql: ${net_profit} ;;
  }

  measure: net_profity_max {
    type:  max
    sql: ${net_profit} ;;
  }

  measure: net_profit_avg {
    type:  average
    sql: ${net_profit} ;;
  }
  measure: number_of_employees_min {
    type:  min
    sql: ${number_of_employees} ;;
  }

  measure: number_of_employees_max {
    type:  max
    sql: ${number_of_employees} ;;
  }

  measure: number_of_employees_avg {
    type:  average
    sql: ${number_of_employees} ;;
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
