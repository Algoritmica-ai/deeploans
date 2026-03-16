view: loan_collaterals {
  sql_table_name: `deeploans_sme_silver.loan_collaterals`
    ;;

    drill_fields: [source*]


  dimension: collateral_id {
    type: string
    primary_key: yes
    sql: ${TABLE}.CS1 ;;
  }

  dimension: original_valuation_amount {
    type: number
    sql: ${TABLE}.CS10 ;;
  }

  dimension_group: original_valuation {
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.CS11 ;;
  }

  dimension_group: current_valuation {
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.CS12 ;;
  }

  dimension: original_valuation_type {
    type: string
    case: {
      when: {
        sql: ${TABLE}.CS13 = "1" ;;
        label: "full"
      }
      when: {
        sql: ${TABLE}.CS13 = "2" ;;
        label: "drive by"
      }
      when: {
        sql: ${TABLE}.CS13 = "3" ;;
        label: "AVM"
      }
      when: {
        sql: ${TABLE}.CS13 = "4" ;;
        label: "indexed"
      }
      when: {
        sql: ${TABLE}.CS13 = "5" ;;
        label: "desktop"
      }
      when: {
        sql: ${TABLE}.CS13 = "6" ;;
        label: "managing/estate agent"
      }
      when: {
        sql: ${TABLE}.CS13 = "7" ;;
        label: "purchase price"
      }
      when: {
        sql: ${TABLE}.CS13 = "8" ;;
        label: "hair cut"
      }
      when: {
        sql: ${TABLE}.CS13 = "9" ;;
        label: "other"
      }
      else: "unknown"
    }
  }

  dimension: ranking {
    type: string
    case: {
      when: {
        sql: ${TABLE}.CS14 = "1" ;;
        label: "1st lien"
      }
      when: {
        sql: ${TABLE}.CS14 = "2" ;;
        label: "2nd lien"
      }
      when: {
        sql: ${TABLE}.CS14 = "3" ;;
        label: "other"
      }
      else: "unknown"
    }
  }

  dimension: prior_balances {
    type: number
    sql: ${TABLE}.CS15 ;;
  }

  dimension: property_postcode {
    type: string
    sql: ${TABLE}.CS16 ;;
  }

  dimension: geo_region {
    type: string
    sql: ${TABLE}.CS17 ;;
  }

  dimension: unconditional_personal_guarantee_amount {
    type: number
    sql: ${TABLE}.CS18 ;;
  }

  dimension: unconditional_corporate_guarantee_amount {
    type: number
    sql: ${TABLE}.CS19 ;;
  }

  dimension: loan_identifier {
    type: string
    sql: ${TABLE}.CS2 ;;
  }

  dimension: corporate_guarantor_identifier {
    type: string
    sql: ${TABLE}.CS20 ;;
  }

  dimension: corporate_guarantor_1year_pd {
    type: number
    sql: ${TABLE}.CS21 ;;
  }

  dimension_group: corporate_guarantor_last_internal_rating_review {
    type: time
    timeframes: [time,  day_of_month,week, month, year, quarter_of_year]
    sql: ${TABLE}.CS22 ;;
  }

  dimension: originator_channel {
    type: string
    case: {
      when: {
        sql: ${TABLE}.CS23 = "1" ;;
        label: "office network"
      }
      when: {
        sql: ${TABLE}.CS23 = "2" ;;
        label: "broker"
      }
      when: {
        sql: ${TABLE}.CS23 = "3" ;;
        label: "internet"
      }
      when: {
        sql: ${TABLE}.CS23 = "4" ;;
        label: "other"
      }
      else: "unknown"
    }
  }

  dimension: collateral_currency {
    type: string
    sql: ${TABLE}.CS24 ;;
  }

  dimension: personal_guarantee_currency {
    type: string
    sql: ${TABLE}.CS25 ;;
  }

  dimension: corporate_guarantee_currency {
    type: string
    sql: ${TABLE}.CS26 ;;
  }

  dimension: prior_balance_currency {
    type: string
    sql: ${TABLE}.CS27 ;;
  }

  dimension: num_collateral_items_securing_loan {
    type: number
    sql: ${TABLE}.CS28 ;;
  }

  dimension: security_type {
    type: string
    case: {
      when: {
        sql: ${TABLE}.CS3 = "1" ;;
        label: "liquidation of collateral"
      }
      when: {
        sql: ${TABLE}.CS3 = "2" ;;
        label: "enforcements of guarantees"
      }
      when: {
        sql: ${TABLE}.CS3 = "3" ;;
        label: "additional lending"
      }
      when: {
        sql: ${TABLE}.CS3 = "4" ;;
        label: "cash recoveries"
      }
      when: {
        sql: ${TABLE}.CS3 = "5" ;;
        label: "other"
      }
      else: "unknown"
    }
  }

  dimension: collateral_value {
    type: number
    sql: ${TABLE}.CS4 ;;
  }

  dimension: collateralisation_ratio {
    type: number
    sql: ${TABLE}.CS5 ;;
  }

  dimension: collateral_type {
    type: string
    case: {
      when: {
        sql: ${TABLE}.CS6 = "1" ;;
        label: "auto vehicles"
      }
      when: {
        sql: ${TABLE}.CS6 = "2" ;;
        label: "industrial vehicles"
      }
      when: {
        sql: ${TABLE}.CS6 = "3" ;;
        label: "commercial trucks"
      }
      when: {
        sql: ${TABLE}.CS6 = "4" ;;
        label: "rail vehicles"
      }
      when: {
        sql: ${TABLE}.CS6 = "5" ;;
        label: "nautical commercial vehicles"
      }
      when: {
        sql: ${TABLE}.CS6 = "6" ;;
        label: "nautical leisure vehicles"
      }
      when: {
        sql: ${TABLE}.CS6 = "7" ;;
        label: "aeroplanes"
      }
      when: {
        sql: ${TABLE}.CS6 = "8" ;;
        label: "machine tools"
      }
      when: {
        sql: ${TABLE}.CS6 = "9" ;;
        label: "industrial equipment"
      }
      when: {
        sql: ${TABLE}.CS6 = "10" ;;
        label: "office equipment"
      }
      when: {
        sql: ${TABLE}.CS6 = "11" ;;
        label: "medical equipment"
      }
      when: {
        sql: ${TABLE}.CS6 = "12" ;;
        label: "energy related equipment"
      }
      when: {
        sql: ${TABLE}.CS6 = "13" ;;
        label: "commercial building"
      }
      when: {
        sql: ${TABLE}.CS6 = "14" ;;
        label: "residential building"
      }
      when: {
        sql: ${TABLE}.CS6 = "15" ;;
        label: "industrial building"
      }
      when: {
        sql: ${TABLE}.CS6 = "16" ;;
        label: "other vehicles"
      }
      when: {
        sql: ${TABLE}.CS6 = "17" ;;
        label: "other equipment"
      }
      when: {
        sql: ${TABLE}.CS6 = "18" ;;
        label: "other real estate"
      }
      when: {
        sql: ${TABLE}.CS6 = "19" ;;
        label: "securities"
      }
      when: {
        sql: ${TABLE}.CS6 = "20" ;;
        label: "third party guarantee"
      }
      when: {
        sql: ${TABLE}.CS6 = "21" ;;
        label: "unsecured guarantee"
      }
      when: {
        sql: ${TABLE}.CS6 = "22" ;;
        label: "other financial assets"
      }
      when: {
        sql: ${TABLE}.CS6 = "23" ;;
        label: "no collateral"
      }
      else: "unknown"
    }
  }

  dimension: is_property_finished {
    type: yesno
    sql: ${TABLE}.CS7 ;;
  }

  dimension: is_property_licensed {
    type: yesno
    sql: ${TABLE}.CS8 ;;
  }

  dimension: asset_insurance {
    type: yesno
    sql: ${TABLE}.CS9 ;;
  }

  dimension: dl_code {
    type: string
    sql: ${TABLE}.dl_code ;;
  }

  dimension: month {
    hidden:  yes
    type: string
    sql: ${TABLE}.month ;;
  }


  dimension: year {
    hidden:  yes
    type: string
    sql: ${TABLE}.year ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
  measure: collateral_value_min {
    type:  min
    sql: ${collateral_value} ;;
  }

  measure: collateral_value_max {
    type:  max
    sql: ${collateral_value} ;;
  }

  measure: collateral_value_avg {
    type:  average
    sql: ${collateral_value} ;;
  }

  measure: collateralisation_ratio_min {
    type:  min
    sql: ${collateralisation_ratio} ;;
  }

  measure: collateralisation_ratio_max {
    type:  max
    sql: ${collateralisation_ratio} ;;
  }

  measure: collateralisation_ratio_avg {
    type:  average
    sql: ${collateralisation_ratio} ;;
  }
  measure: original_valuation_amount_min {
    type:  min
    sql: ${original_valuation_amount} ;;
  }

  measure: original_valuation_amount_max {
    type:  max
    sql: ${original_valuation_amount} ;;
  }

  measure: original_valuation_amount_avg {
    type:  average
    sql: ${original_valuation_amount} ;;
  }
  measure: prior_balances_min {
    type:  min
    sql: ${prior_balances} ;;
  }

  measure: prior_balances_max {
    type:  max
    sql: ${prior_balances} ;;
  }

  measure: prior_balances_avg {
    type:  average
    sql: ${prior_balances} ;;
  }
  measure: unconditional_personal_guarantee_amount_min {
    type:  min
    sql: ${unconditional_personal_guarantee_amount} ;;
  }

  measure: unconditional_personal_guarantee_amount_max {
    type:  max
    sql: ${unconditional_personal_guarantee_amount} ;;
  }

  measure: unconditional_personal_guarantee_amount_avg {
    type:  average
    sql: ${unconditional_personal_guarantee_amount} ;;
  }
  measure: unconditional_corporate_guarantee_amount_min {
    type:  min
    sql: ${unconditional_corporate_guarantee_amount} ;;
  }

  measure: unconditional_corporate_guarantee_amount_max {
    type:  max
    sql: ${unconditional_corporate_guarantee_amount} ;;
  }

  measure: unconditional_corporate_guarantee_amount_avg {
    type:  average
    sql: ${unconditional_corporate_guarantee_amount} ;;
  }
  measure: corporate_guarantor_1year_pd_min {
    type:  min
    sql: ${corporate_guarantor_1year_pd} ;;
  }

  measure: corporate_guarantor_1year_pd_max {
    type:  max
    sql: ${corporate_guarantor_1year_pd} ;;
  }

  measure: corporate_guarantor_1year_pd_avg {
    type:  average
    sql: ${corporate_guarantor_1year_pd} ;;
  }
  measure: num_collateral_items_securing_loan_min {
    type:  min
    sql: ${num_collateral_items_securing_loan} ;;
  }

  measure: num_collateral_items_securing_loan_max {
    type:  max
    sql: ${num_collateral_items_securing_loan} ;;
  }

  measure: num_collateral_items_securing_loan_avg {
    type:  average
    sql: ${num_collateral_items_securing_loan} ;;
  }
  set: source {
    fields: [dl_code,
      collateral_id,
      loan_identifier]
  }
}
