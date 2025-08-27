view: bond_collaterals {
  sql_table_name: `deeploans_sme_silver.bond_collaterals`
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

  dimension: excess_spread_amount {
    type: number
    sql: ${TABLE}.BS11 ;;
  }

  dimension: trigger_measurement_ratio {
    type: yesno
    sql: ${TABLE}.BS12 ;;
  }

  dimension: avg_constant_pre_prayment_rate {
    type: number
    sql: ${TABLE}.BS13 ;;
  }

  dimension: issuer {
    type: string
    sql: ${TABLE}.BS2 ;;
  }

  dimension: dl_code {
    type: string
    sql: ${TABLE}.dl_code ;;
  }


  measure: count {
    type: count
    drill_fields: []
  }

  measure: excess_spread_amount_min {
    type:  min
    sql: ${excess_spread_amount} ;;
  }

  measure: excess_spread_amount_max {
    type:  max
    sql: ${excess_spread_amount} ;;
  }

  measure: excess_spread_amount_avg {
    type:  average
    sql: ${excess_spread_amount} ;;
  }

  measure: avg_constant_pre_prayment_rate_min {
    type:  min
    sql: ${avg_constant_pre_prayment_rate} ;;
  }

  measure: avg_constant_pre_prayment_rate_max {
    type:  max
    sql: ${avg_constant_pre_prayment_rate} ;;
  }

  measure: avg_constant_pre_prayment_rate_avg {
    type:  average
    sql: ${avg_constant_pre_prayment_rate} ;;
  }

}
