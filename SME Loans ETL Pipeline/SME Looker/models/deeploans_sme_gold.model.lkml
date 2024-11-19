connection: "partner_deeploans"
label: "SME"

# include all the views
include: "/views/**/*.view"

datagroup: demo_deeploans_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: demo_deeploans_default_datagroup

#------------------- Direct views

# explore: bond_collaterals {}

# explore: loans {}

# explore: performances {}

# explore: interests {}

# explore: bond_tranches {}

# explore: financials {}

# explore: obligors {}

explore: bond_info {
  join: bond_collaterals {
    sql_on: (${bond_info.report_date} = ${bond_collaterals.report_date}) AND
      (${bond_info.report_date} = ${bond_collaterals.report_date}) ;;
    relationship: one_to_one
  }
  join: bond_tranches {
    sql_on: (${bond_info.report_date} = ${bond_tranches.report_date}) AND
      (${bond_info.report_date} = ${bond_tranches.report_date}) ;;
    relationship: one_to_one
  }
}

explore: collaterals {
  view_name: loan_collaterals
}

explore: deals {}

explore: loans {
  join: performances {
    sql_on: (${loans.loan_identifier} = ${performances.loan_identifier}) AND
      (${loans.dl_code} = ${performances.dl_code}) ;;
    relationship: one_to_one
  }
  join: financials {
    sql_on: (${loans.loan_identifier} = ${financials.loan_identifier}) AND
      (${loans.dl_code} = ${financials.dl_code}) ;;
    relationship: one_to_one
  }
  join: interests {
    sql_on: (${loans.loan_identifier} = ${interests.loan_identifier}) AND
      (${loans.dl_code} = ${interests.dl_code}) ;;
    relationship: one_to_one
  }
  join: obligors {
    sql_on: (${loans.loan_identifier} = ${obligors.loan_identifier}) AND
      (${loans.dl_code} = ${obligors.dl_code}) ;;
    relationship: one_to_one
  }
  join: nace_codes {
    sql_on: (${obligors.nace_industry_code} = ${nace_codes.code});;
    relationship: one_to_one
  }
}


explore: nace_codes {}
