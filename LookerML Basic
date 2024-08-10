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
  
  measure: count{
    type: count
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
    view_label: "Office"
  }
  
  # post aggregation operations
  measure: pct_of_total_sales{
    type: percent_of_total
    sql: ${total_sales} ;;
  }
  
  # filter field will only be used as a filter but cannot be used as a dimension 
  filter: office_id{
    type: string
    sql: ${TABLE}.office_id ;;
    view_label: "Office"
  }
  
}

explore: sales_explore{
  view_name: sales
  label: "Transactions"
  group_label: "Element Rental"
  description: "This is a explore with element rental sales info"
}
