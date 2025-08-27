view: bond_info {
  sql_table_name: `deeploans_sme_silver.bond_info`
    ;;

    drill_fields: [source*]

  dimension: primary_key {
    hidden:  yes
    primary_key: yes
    sql: CONCAT(${report_date},${issuer}) ;;
  }

  dimension_group: report {
    hidden: yes
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
    hidden:  yes
    type: string
    sql: ${TABLE}.BS2 ;;
  }

  dimension: ending_reserve_account_balance {
    type: number
    sql: ${TABLE}.BS3 ;;
  }

  dimension: target_reserve_account_balance {
    type: number
    sql: ${TABLE}.BS4 ;;
  }

  dimension: drawings_under_liquidity_facility {
    type: yesno
    sql: ${TABLE}.BS5 ;;
  }

  dimension: currency_of_reserve_account_balance {
    type: string
    sql: ${TABLE}.BS6 ;;
  }

  dimension: dl_code {
    hidden: yes
    type: string
    sql: ${TABLE}.dl_code ;;
  }


  measure: count {
    type: count
    drill_fields: []
  }

  measure: ending_reserve_account_balance_min {
    type:  min
    sql: ${ending_reserve_account_balance} ;;
  }

  measure: ending_reserve_account_balance_max {
    type:  max
    sql: ${ending_reserve_account_balance} ;;
  }

  measure: ending_reserve_account_balance_avg {
    type:  average
    sql: ${ending_reserve_account_balance} ;;
  }

  measure: target_reserve_account_balance_min {
    type:  min
    sql: ${target_reserve_account_balance} ;;
  }

  measure: target_reserve_account_balance_max {
    type:  max
    sql: ${target_reserve_account_balance} ;;
  }

  measure: target_reserve_account_balance_avg {
    type:  average
    sql: ${target_reserve_account_balance} ;;
  }

  set: source {
    fields: [dl_code,
      report_date,
      issuer]
  }
}
