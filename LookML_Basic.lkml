connection: "bigquery_public_data_looker"

named_value_format: custom_thousands{
  value_format: "0.0,\" K\""
}

view:sales{
  derived_table: {
    sql: select "1" as transaction_id,  date("2020-06-06") as date,  "6" as product_id,  "65" as salesperson_id,  "17" as customer_id,  "6" as office_id,  5 as hours,  9740 as sales,  "Florida" as transaction_state, union all
        select "2" as transaction_id,  date("2021-07-30") as date,  "2" as product_id,  "30" as salesperson_id,  "8" as customer_id,  "4" as office_id,  62 as hours,  98146 as sales,  "Iowa" as transaction_state, union all
        select "3" as transaction_id,  date("2021-01-05") as date,  "5" as product_id,  "15" as salesperson_id,  "5" as customer_id,  "3" as office_id,  3 as hours,  4899 as sales,  "New York" as transaction_state, union all
        select "4" as transaction_id,  date("2020-08-19") as date,  "8" as product_id,  "2" as salesperson_id,  "19" as customer_id,  "2" as office_id,  5 as hours,  6625 as sales,  "Utah" as transaction_state, union all
        select "5" as transaction_id,  date("2020-02-27") as date,  "8" as product_id,  "48" as salesperson_id,  "18" as customer_id,  "10" as office_id,  3 as hours,  3975 as sales,  "Alabama" as transaction_state, union all
        select "6" as transaction_id,  date("2021-06-24") as date,  "3" as product_id,  "20" as salesperson_id,  "1" as customer_id,  "3" as office_id,  19 as hours,  36176 as sales,  "Missouri" as transaction_state, union all
        select "7" as transaction_id,  date("2021-10-23") as date,  "5" as product_id,  "13" as salesperson_id,  "3" as customer_id,  "3" as office_id,  53 as hours,  86549 as sales,  "New York" as transaction_state, union all
        select "8" as transaction_id,  date("2020-01-21") as date,  "5" as product_id,  "63" as salesperson_id,  "16" as customer_id,  "1" as office_id,  15 as hours,  24495 as sales,  "California" as transaction_state, union all
        select "9" as transaction_id,  date("2021-09-18") as date,  "11" as product_id,  "73" as salesperson_id,  "17" as customer_id,  "1" as office_id,  52 as hours,  63284 as sales,  "New York" as transaction_state, union all
        select "10" as transaction_id,  date("2020-10-16") as date,  "11" as product_id,  "99" as salesperson_id,  "1" as customer_id,  "9" as office_id,  95 as hours,  115615 as sales,  "California" as transaction_state;;
  }
  
  set: transaction_related_fields{
    fields: [transaction_id, transaction_date, is_expensive, state]
  }
  
  # These are the fields when users click agg results to pop up details
  drill_fields: [transaction_related_fields*, total_sales]
  
  dimension: transaction_id{
    type: string
    sql: ${TABLE}.transaction_id;;
  }
  
  dimension: transaction_date{
    type: date
    sql: ${TABLE}.date ;;
    label: "Sales Date - Purchased" #rename the dimension but it does not change the chianing name
  }
  
  dimension_group: transaction_date_group{
    type: time
    timeframes: [date, week, month, year, raw]
    datatype: date
    sql: ${transaction_date} ;;
  }
  
  dimension: sales{
    type: number
    sql: ${TABLE}.sales ;;
  }
  
  dimension: hours {
    type: number
    sql: ${TABLE}.hours ;;
  }
  
  dimension: sales_with_tax{
    type: number
    sql: ${sales} * 1.13 ;;
  }
  
  dimension: sales_with_tax_after_employee_spend{
    type: number
    sql: ${sales_with_tax} - (0.2*${TABLE}.sales) ;;
  }
  
  dimension: is_expensive{
    type: yesno
    sql: ${sales_with_tax_after_employee_spend} > 25000 ;;
    description: "A sale is expensive when the total sales amt is greater than 25000"
  }
  
  dimension: sales_groups{
    type: bin
    sql: ${TABLE}.sales ;;
    style: integer
    bins: [0,100000,200000,300000]
  }
  
  dimension: state{
    type: string
    sql: ${TABLE}.transaction_state ;;
    group_label: "Location"
    map_layer_name: us_states
  }
  
  dimension: state_groups{
    type: string
    case: {
      when: {
        sql: ${TABLE}.transaction_state in ('Florida','Alabama','Missouri');;
        label: "Southern States"
      }
      when: {
        sql: ${TABLE}.transaction_state in ('California') ;;
        label: "Western States"
      }
      else: "Other States"
    }
    group_label: "Location"
  }
  
  dimension: state_gropuping_sql{
    type: string
    sql: case when ${TABLE}.transaction_state in ('California') then "Western States" else 'Other States' end  ;;
  }
  
  dimension: customer_id{
    type: string
    sql: ${TABLE}.customer_id ;;
    
    action: {
      label: "send customer a poke"
      url: "https://example.com"
      
      param: {
        name: "customer_id"
        value: "{{value}}"
      }
      
      form_param: {
        name: "message"
        type: string
        label: "Message"
        description: "This is the message that you want to send to the customer"
      }
    }
    
  }
  
  
  measure: count{
    type: count
    drill_fields: [transaction_id]
  }
  
  measure: total_sales{
    type: sum
    sql: ${TABLE}.sales ;;
    value_format_name: custom_thousands
  }
  
  measure: avg_sales{
    type: average
    sql: ${TABLE}.sales ;;
    value_format_name: usd_0
  }
  
  # the list_field input variable name has no {}!
  measure: list_of_transactions{
    type: list
    list_field: transaction_id
    # view_label: "Office"
  }
  
  # post aggregation operations
  measure: pct_of_total_sales{
    type: percent_of_total
    sql: ${total_sales} ;;
  }
  
  
  # filter field will only be used as a filter but cannot be used as a dimension
  # filter: office_id{
  #   type: string
  #   sql: ${TABLE}.office_id ;;
  #   view_label: "Office"
  # }
  
  dimension: office_id{
    type: string
    sql: ${TABLE}.office_id ;;
    # view_label: "Office"
  }
  
  
  
}

view: product {
  derived_table: {
    sql:  select "1" as product_id,  "Back Truck" as product_name,  "Medium" as product_category,  1238 as product_hourly_price, union all
      select "2" as product_id,  "Bulldozer" as product_name,  "Medium" as product_category,  1583 as product_hourly_price, union all
      select "3" as product_id,  "Compactor" as product_name,  "Light" as product_category,  1904 as product_hourly_price, union all
      select "4" as product_id,  "Crawler" as product_name,  "Light" as product_category,  1735 as product_hourly_price, union all
      select "5" as product_id,  "Dragline" as product_name,  "Light" as product_category,  1633 as product_hourly_price, union all
      select "6" as product_id,  "Dump Truck" as product_name,  "Heavy" as product_category,  1948 as product_hourly_price, union all
      select "7" as product_id,  "Excavator" as product_name,  "Heavy" as product_category,  1979 as product_hourly_price, union all
      select "8" as product_id,  "Grader" as product_name,  "Heavy" as product_category,  1325 as product_hourly_price, union all
      select "9" as product_id,  "Scraper" as product_name,  "Heavy" as product_category,  1894 as product_hourly_price, union all
      select "10" as product_id,  "Skid-Steer" as product_name,  "Medium" as product_category,  1506 as product_hourly_price, union all
      select "11" as product_id,  "Trencher" as product_name,  "Medium" as product_category,  1217 as product_hourly_price;;
  }
  
  dimension: product_id {
    type:  string
    sql: ${TABLE}.product_id ;;
  }
  
  dimension: product_name {
    type: string
    sql: ${TABLE}.product_name ;;
  }
  
  dimension: product_hourly_price {
    type: string
    value_format_name: usd_0
    sql: ${TABLE}.product_hourly_price ;;
  }
  
  measure: count {
    type:  count
  }
  
}

view: office {
  derived_table: {
    sql:  select "1" as office_id,  "New York City" as office_name,  "10009" as office_zipcode,  "1" as head_salesperson_id, union all
      select "2" as office_id,  "Dallas" as office_name,  "75001" as office_zipcode,  "2" as head_salesperson_id, union all
      select "3" as office_id,  "Houston" as office_name,  "77001" as office_zipcode,  "3" as head_salesperson_id, union all
      select "4" as office_id,  "Detroit" as office_name,  "48127" as office_zipcode,  "4" as head_salesperson_id, union all
      select "5" as office_id,  "Miami" as office_name,  "33101" as office_zipcode,  "5" as head_salesperson_id, union all
      select "6" as office_id,  "Orlando" as office_name,  "32789" as office_zipcode,  "6" as head_salesperson_id, union all
      select "7" as office_id,  "Seattle" as office_name,  "98101" as office_zipcode,  "7" as head_salesperson_id, union all
      select "8" as office_id,  "San Francisco" as office_name,  "94016" as office_zipcode,  "8" as head_salesperson_id, union all
      select "9" as office_id,  "Los Angeles" as office_name,  "90005" as office_zipcode,  "9" as head_salesperson_id, union all
      select "10" as office_id,  "Austin" as office_name,  "73301" as office_zipcode,  "10" as head_salesperson_id
      ;;
  }
  
  dimension: office_id {
    type:  string
    sql:  ${TABLE}.office_id ;;
  }
  
  dimension: office_name {
    type:  string
    sql:  ${TABLE}.office_name ;;
    
    link: {
      label: "Get more information"
      url: "http://www.google.com/search?q={{ value }}"
      icon_url: "http://google.com/favicon.ico"
    }
    
    link: {
      label: "Mail Office"
      url: "mailto:{{value}}@elementrental.com"
    }
    
    # suggestions: ["Austin","Dallas"]
    
    
  }
  
  dimension: office_zip_code {
    type: zipcode
    sql: ${TABLE}.office_zipcode ;;
  }
  
  measure: count {
    type:  count
  }
  
}


view: office_2 {
  
  extends: [office]
  
  # add an dimension 
  dimension: head_salesperson{
    type: string
    sql: ${TABLE}.head_salesperson_id ;;
  }
}

explore: sales_explore{
  view_name: sales
  label: "Transactions"
  group_label: "Element Rental"
  description: "This is a explore with element rental sales info"
  
  join: office {
    type: left_outer
    sql_on: ${sales.office_id} = ${office.office_id} ;;
    relationship: many_to_one
    fields: [office.office_id, office.office_name, office.office_zip_code] #only keep needed fields from joined table
    # sql_where: ${office.office_id} in ('1','2','3','4','5') ;;
  }
  
  # Quick start query/reuslts for users
  query: sales_by_office{
    dimensions: [office.office_name]
    measures: [sales.total_sales]
    description: "This analysis is sales by office name"
  }
  
}

# Create another explore based on an existing explore
explore: Transactions_California{
  
  extends: [sales_explore]
  
  label: "Transaction California"
  sql_always_where: ${sales.state} = 'California' ;;
  
}
 

# Unit test to make sure the source data is good
test: transaction_id_is_unique{
  
  explore_source: sales_explore {
    column: transaction_id {}
    column: count{}
    sorts: [sales.count: desc]
    limit: 1
  }
  assert: transaction_id_is_indeed_unique{
    expression: ${sales.count} = 1;;
  }
  
}
