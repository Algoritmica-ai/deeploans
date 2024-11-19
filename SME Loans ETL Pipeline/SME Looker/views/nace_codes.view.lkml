view: nace_codes {
  sql_table_name: `your_PROJECT_ID.deeploans_sme_silver.nace_codes`
    ;;

  dimension: code {
    type: string
    primary_key: yes
    sql: ${TABLE}.code ;;
  }

  dimension: description {
    type: string
    label: "Nace Industry"
    sql: ${TABLE}.description ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
