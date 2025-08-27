view: deals {
  sql_table_name: `deeploans_sme_silver.deals`
    ;;

   dimension: Asset_ID {
    type: string
    sql: ${TABLE}.AssetClassCode ;;
  }

  dimension: Asset_Name {
    type: string
    sql: ${TABLE}.AssetClassName ;;
  }

  dimension: Country_Code {
    type: string
    sql: ${TABLE}.ContactInformation ;;
  }

  .
  .
  .
  ... and so on.
}

