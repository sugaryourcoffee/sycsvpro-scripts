require 'syctimeleap/time_leap'

# Acts like a README by printing out the sequence of method invocations to
# achieve a certain result.
# 
# :call-seq:
#   sycsvpro execute machine_age.rb readme
#
def readme
  puts
  puts "Usage of machine_age.rb"
  puts "======================="
  puts
  puts "1. Machine analysis"
  puts "-------------------"
  puts "1.0 clean_ib_source (INFILE EUNA download)"
  puts "1.1 abc_analysis (INFILE: EUNA download)"
  puts "1.2 machine_age (INFILE: EUNA downlaod"
  puts "1.3 machine_count_top (INFILE: see note)"
  puts "    Note: use the resulting file from 'machine_age'"
  puts
  puts "2. Spares and Repairs analysis"
  puts "------------------------------"
  puts "2.1 extract_regional_data (INFILE: DWH download)"
  puts "    Note: Only if you want to analyze a specific country and extract "
  puts "          it from a world file"
  puts "2.2 insert_customer_data (INFILE: result from 2.1 or DWH download"
  puts "2.3 region_revenue (INFILE: result from 2.2 or DWH download)"
  puts "2.4 customer_revenue (INFILE: result from 2.2)"  
  puts
  puts "3. Conduct complete analysis in one swoop"
  puts "-----------------------------------------"
  puts "3.1 machine_analysis (includes 1.1 - 1.3, INFILE: EUNA downlaod)"
  puts "3.2 spares_and_repairs_analysis_complete (includes 2.1 - 2.4,"
  puts "    INFILE: DWH download)"
  puts "3.3 spares_and_repairs_analysis (includes 2.3 - 2.4,"
  puts "    INFILE: DWH downlaod)"
end

# Clean IB source
# Clean IB source by removing leading 0 from IDs
#
# :call-seq:
#   sycsvpro execute machine_age.rb clean_ib_source INFILE
#
# INFILE:: input csv-file sperated with colons (;) to operate on (EUNA downlaod)
#
# Result is in the file 'INFILE_BASE_NAME-clean.csv'.
# Result is in the file 'INFILE_BASE_NAME-clean.csv'
def clean_ib_source
  infile, result, *others = params
  outfile = "#{File.basename(infile, '.*')}-clean.csv"

  puts; print "Cleaning #{infile}"

  cols = "14:s14.scan(/^0*(\\d+)/).flatten[0],"+
         "38:s38.scan(/^0*(\\d+)/).flatten[0],"+
         "46:s46.scan(/^0*(\\d+)/).flatten[0]"

  calculator = Sycsvpro::Calculator.new(infile:  infile,
                                        outfile: outfile,
                                        header:  "*",
                                        rows:    "1-#{result.row_count}",
                                        cols:    cols).execute
                                        header:  '*',
                                        rows:    "1-#{result.row_count}",
                                        cols:    cols).execute

  puts; puts "You can find the result in '#{outfile}'"
end

# Conducts a ABC analysis based on machine count
#
# :call-seq:
#   sycsvpro execute machine_age.rb abc_analysis INFILE COUNTRY_NAME
#
# INFILE:: input csv-file sperated with colons (;) to operate on (EUNA downlaod)
# COUNTRY_NAME:: country-identifier for the resulting files
#
# Result of ABC analysis is in the file 'ABC-analysis-COUNTRY_NAME.csv'. If no
# COUNTRY_NAME is given the result is in 'ABC-analysis-infile_basename.csv' 
def abc_analysis
  infile, result, *others = params
  abc_filename = "ABC-analysis-#{others[0] || File.basename(infile, '.*')}.csv"

  puts; print "Assigning machine count to customers..."

  aggregator = Sycsvpro::Aggregator.new(infile:  infile, 
                                        outfile: "aggregate.csv", 
                                        cols:    "46,45", 
                                        cols:    "45", 
                                        sum:     "Total:1,Machines")

  aggregator.execute

  puts; print "Sort customers based on machine count descending..."

  sorter = Sycsvpro::Sorter.new(infile:  "aggregate.csv",
                                outfile: "sort.csv",
                                cols:    "n:1",
                                df:      "%Y-%m-%d",
                                desc:    "d")
  
  sorter.execute

  puts; print "Categorize customers in regard to machine count..."

  counter = Sycsvpro::Counter.new(infile: "sort.csv",
                                  outfile: "count.csv",
                                  rows:    "1-#{result.row_count}",
                                  key:     "0:customer,1:machines",
                                  cols:    "1:<10,1:10-50,1:>50",
                                  sort:    false)

  counter.execute

  puts; print "Conducting ABC-analysis..."

  calculator = Sycsvpro::Calculator.new(infile: "count.csv",
                                    outfile: abc_filename,
                                    header:  "*,A,B,C",
                                    rows:    "2-#{result.row_count}",
                                    cols:    "5:c4*c1,6:c3*c1,7:c2*c1",
                                    sum:     true)
  
  calculator.execute

  clean_up(["aggregate.csv", "sort.csv", "count.csv"])

  puts; puts "You can find the result of the ABC Analysis in '#{abc_filename}'"

end

# Builds histogram data for machine count regarding machine ages
#
# :call-seq:
#   sycsvpro execute machine_age.rb machine_age INFILE COUNTRY_NAME
#
# INFILE:: input csv-file sperated with colons (;) to operate on (EUNA download)
# COUNTRY_NAME:: country-identifier for the resulting files
#   
# Result of machine ages is in the file 'machine-ages-COUNTRY_NAME.csv'. If no
# COUNTRY_NAME is given the result is in 'machine-ages-infile_basename.csv' 
def machine_age

  infile, result, *others = params
  ages_filename = "machine-ages-#{others[0] || File.basename(infile, '.*')}.csv"

  puts; print "Extracting date columns for #{infile}..."
  puts; print "Extracting date columns from #{infile}..."

  Sycsvpro::Extractor.new(infile: infile,
                          outfile: "extract.csv",
                          cols:    "45,10-12,84").execute

  puts; print "Determine the machine ages based on the oldest date..."

  Sycsvpro::Calculator.new(infile:  "extract.csv",
                           outfile: "calc.csv",
                           header:  "*,Age",
                           cols:    "5:[d1,d2,d3,d4].compact.min",
                           df:      "%d.%m.%Y").execute

  puts; print "Create histogram for machine ages..."

  timeleap = SycTimeleap::TimeLeap.new
  o10y   = "<#{timeleap.b10y}"
  b10a7y = "#{timeleap.b10y}-#{timeleap.b7y}"
  b7a2y  =  "#{timeleap.b7y+1}-#{timeleap.b2y}"
  y2y    = ">#{timeleap.b2y}"

  Sycsvpro::Counter.new(infile:  "calc.csv",
                        outfile: "count.csv",
                        rows:    "1-#{result.row_count}",
                        key:     "0:customer",
                        cols:    "5:#{o10y},5:#{b10a7y},5:#{b7a2y},5:#{y2y}",
                        df:      "%Y-%m-%d",
                        sum:     "Total:1,Sum",
                        sort:    false).execute

  puts; print "Calculate count of machines older than 7 years per customer..."

  Sycsvpro::Calculator.new(infile:  "count.csv",
                           outfile: "calc.csv",
                           header:  "*,Older7Years",
                           cols:    "6:c1+c2").execute

  puts; print "Sorting customers based on machine count and age"

  Sycsvpro::Sorter.new(infile: "calc.csv",
                       outfile: ages_filename,
                       cols:    "n:5,n:6",
                       start:   "1",
                       desc:    true).execute

  clean_up(["extract.csv", "calc.csv", "count.csv"])

  puts; 
  puts "You can find the result of the histogram data in '#{ages_filename}'"

end

=======
# List all customers with machine count per year. Sum
# up the machine count.
#
# :call-seq:
#   sycsvpro execute machine_age.rb machine_count_per_year INFILE COUNTRY_NAME
#
# INFILE:: input csv-file sperated with colons (;) to operate on (see note)
#          Note: Use the resulting file from #machine_age method invocation
# COUNTRY_NAME:: country-identifier for the resulting files
#
# Result is in the file 'COUNTRY_NAME-count-per-year.csv', 
def machine_count_per_year
  infile, result, *others = params
  outfile = "#{others[0] || File.basename(infile, '.*')}-count-per-year.csv"

  puts; print "Determine the machine ages based on the oldest date..."

  Sycsvpro::Calculator.new(infile:  infile,
                           outfile: "calc.csv",
                           header:  "*,Age",
                           cols:    "#{result.col_count}:[d10,d11,d12,d84].compact.min",
                           df:      "%d.%m.%Y").execute

  puts; print "Create table with machine ages per year"
  
  header = "c45,BEGINc#{result.col_count}=~/(\\d{4})-\\d{2}-\\d{2}/END"
  cols   =          "c#{result.col_count}=~/(\\d{4})-\\d{2}-\\d{2}/:+1"
  sum    =     "BEGINc#{result.col_count}=~/(\\d{4})-\\d{2}-\\d{2}/END"

  Sycsvpro::Table.new(infile:  "calc.csv",
                      outfile: outfile,
                      header:  header,
                      cols:    cols,
                      key:     "c45",
                      sum:     "top:#{sum}",
                      sort:    "1").execute
  
  clean_up(["calc.csv"])

  puts; puts "You can find the result in #{outfile}"
 
end

# Extracts the top customers based on machine count and age
#
# :call-seq:
#   sycsvpro execute machine_age.rb machine_count_top INFILE COUNTRY_NAME COUNT
#   
# INFILE:: input csv-file sperated with colons (;) to operate on (see note)
#          Note: Use the resulting file from #machine_age method invocation
# COUNTRY_NAME:: country-identifier for the resulting files
# COUNT:: select rows only with machine count >= COUNT
# 
# Result is in the file 'A-customers-NAME.csv', 
# 'A-customers-count-COUNTRY_NAME.csv' and 'A-customers-age-COUNTRY_NAME.csv'. 
# If no COUNTRY_NAME is given the result is in 
# 'A-customers-xxx-infile_basename.csv' 
def machine_count_top
  infile, result, *others = params
  a_filename = "A-customers-#{others[0] || File.basename(infile, '.*')}.csv"
  count_filename = "A-customers-count-#{others[0] || File.basename(infile, '.*')}.csv"
  age_filename = "A-customers-age-#{others[0] || File.basename(infile, '.*')}.csv"
  count = others[1] || 50

  puts; print "Extracting customers with more than #{count} machines"

  Sycsvpro::Extractor.new(infile:  infile,
                          outfile: a_filename,
                          rows:    "0,1,BEGINn5>=#{count}END").execute

  puts; print "Extract customer name and machine count"

  Sycsvpro::Extractor.new(infile:  a_filename,
                          outfile: count_filename,
                          cols:    "0,5").execute

  puts; print "Extract customer name, machine count and age older than 7 years"

  Sycsvpro::Extractor.new(infile:  a_filename,
                          outfile: age_filename,
                          cols:    "0,5,6").execute

  puts;
  puts "You can find the result in '#{a_filename}', '#{count_filename}' "+
       "and '#{age_filename}'"
  
end

# Extracts the rows for region out of the infile
# :call-seq:
#   sycsvpro execute machine_age.rb extract_region INFILE REGION COUNTRY_NAME
#
# INFILE:: input csv-file sperated with colons (;) to operate on (DWH download)
# REGION:: filter for the rows and region-identifier for the resulting file
# COUNTRY_NAME:: country-identifier for the resulting (optional)
# 
# Result is in the file 'COUNTRY_NAME-REGION-spare-and-repairs.csv'
# If no country/region name is given the result is in
# 'spares-and-repairs.csv'
def extract_regional_data
  infile, result, *others = params

  country_part = ""
  country_part << "#{others[0]}-" if others[0]
  country_part << "#{others[1]}-" if others[1]
  out_file_name = "#{country_part}spares-and-repairs.csv"

  puts; print "Extracting rows of #{country_part.chop}"

  row_filter = "BEGINs18=='#{others[0]}'||s18=='RE_GEBIET'END"

  Sycsvpro::Extractor.new(infile:  infile,
                          outfile: out_file_name,
                          rows:    row_filter).execute

  puts; puts "You can find the result in #{out_file_name}"
end

# Inserts the customer data into the region file
# :call-seq:
#   sycsvpro execute machine_age.rb insert_customer_data INFILE CUSTOMERS
#
# INFILE:: input csv-file sperated with colons (;) to operate on (DWH download
#          or result from #extract_regional_data
# CUSTOMERS:: file that contains customer data to be inserted into INFILE
# 
# Result is in the file 'INFILE_BASE_NAME-with-customers.csv'
def insert_customer_data
  infile, result, *others = params

  outfile = "#{File.basename(infile, '.*')}-with-customers.csv"
  source = others[0]

  puts; print "Inserting customers from #{source} into #{infile}"

  Sycsvpro::Join.new(infile:        infile,
                     outfile:       outfile,
                     source:        source,
                     cols:          "0,3;0,3",
                     joins:         "4=19;4=20",
                     cols:          "1,2;1,2",
                     joins:         "0=19;0=20",
                     pos:           "20,21;23;24",
                     insert_header: "OI_EK_NAME,OI_EK_LAND;OI_AG_NAME,OI_AG_LAND").execute

  puts; puts "You can find the result in #{outfile}"
end

# Collects the countries and regions contained in the infile. This is just for
# information purposes to know for which countries and regions data is
# available
#
# :call-seq:
#   sycsvpro execute machine_age.rb extract_countries_and_regions INFILE
#
# INFILE:: input csv-file sperated with colons (;) to operate on (DWH download
#          or result from #extract_regional_data
# 
# Result is in the file 'countries_and_regions.csv'
def extract_countries_and_regions
  infile, result, *others = params
  out_file_name = "countries_and_regions.csv"

  puts; print "Extracting countries and regions from #{infile}"

  col_filter = "COUNTRIES:4-6+REGIONS:18"

  Sycsvpro::Collector.new(infile:  infile,
                          outfile: out_file_name,
                          cols:    col_filter).execute

  puts; puts "You can find the collection result in #{out_file_name}"
end

# Extracts country and region combinations from the infile. This is just for
# information purposes and can be used to extract country rows from the 
# file with `extract_regional_data`
#
# Creates result as
#  
#     REGION;COUNTRIES
#     SDW;DE;AT
#
# :call-seq:
#   sycsvpro execute country_region_combination infile
#
# INFILE:: input csv-file sperated with colons (;) to operate on (DWH download
#          or result from #extract_regional_data
def country_region_combination
  infile, result, *others = params
  outfile = "country-region-combinations.csv"

  puts; print "Extracting country-region combinations from #{infile}"

  Sycsvpro::Allocator.new(infile: infile,
                          outfile: outfile,
                          key:     "18",
                          cols:    "4-6").execute

  puts; puts "The result can be found in '#{outfile}'"

end

# Calculates the revenue per region per year separated to SP, RP and Total and
# creates a file of the form
#
#     Year | SP      | RP     | Total   | SP-Orders | RP-Orders | Orders |
#     ---- | ------- | ------ | ------- | --------- | --------- | ------ |
#          | 3500.50 | 300.30 | 3600.80 | 100       | 50        | 150    |
#     2013 | 2200.50 | 200.20 | 2400.70 |  80       | 40        | 120    |
#     2014 | 1300.00 | 100.10 | 1200.10 |  20       | 10        |  30    |
#
# :call-seq:
#   sycsvpro execute machine_age.rb region_revenue INFILE COUNTRY_NAME REGION
#   
# INFILE:: input csv-file sperated with colons (;) to operate on (DWH download
#          or result from #extract_regional_data or #insert_customer_data)
# COUNTRY_NAME:: country-identifier for the resulting file
# REGION:: region-identifier for the resulting file
# 
# Result is in the file 'COUNTRY_NAME-REGION-spare-and-repairs-revenues.csv'
# If no country/region name is given the result is in
# 'spares-and-repairs-revenues.csv'
def region_revenue
  infile, result, *others = params

  country_part = ""
  country_part << "#{others[0]}-" if others[0]
  country_part << "#{others[1]}-" if others[1]
  out_file_name = "#{country_part}spares-and-repairs-revenues.csv"

  puts; print "Creating table from spares and repairs revenue for country "+
              "#{country_part.chop}"

  rp_order_type = %w{ ZRN ZRK }
  sp_order_type = %w{ ZE ZEI ZO ZOI ZG ZGNT ZRE ZGUP }
  order_type = sp_order_type + rp_order_type

  Sycsvpro::Table.new(infile: infile,
                      outfile: out_file_name,
                      header:  "Year,SP,RP,Total,SP-Orders,RP-Orders,Orders",
                      key:     "c0=~/\\d+\\.\\d+\\.(\\d{4})/",
                      cols:    "BEGINSP:+n10 if #{sp_order_type}.index(c1)END,"+
                               "BEGINRP:+n10 if #{rp_order_type}.index(c1)END,"+
                               "BEGINTotal:+n10 if #{order_type}.index(c1)END,"+
                               "BEGINSP-Orders:+1 if #{sp_order_type}.index(c1)END,"+
                               "BEGINRP-Orders:+1 if #{rp_order_type}.index(c1)END,"+
                               "BEGINOrders:+1 if #{order_type}.index(c1)END",
                      nf:      "DE",
                      sum:     "top:SP,RP,Total,SP-Orders,RP-Orders,Orders").execute

  puts; puts "You can find the result in #{out_file_name}"
end

# Analyze the customer revenue for spares and repairs per year
#
# | Customer | YEAR-SP-R | YEAR-RP-R | YEAR-R | YEAR-SP-O | YEAR-RP-O | YEAR-O |
# |          | 3500      | 1300      | 4800   | 80        | 40        | 120    |
# | Mia      | 1500      |  300      | 1800   | 30        | 10        |  40    |
# | Hank     | 2000      | 1000      | 3000   | 50        | 30        |  80    |
#
# The columns may appear in a different sequence. O = Orders, R = Revenue
#
# :call-seq:
#   sycsvpro execute machine_age.rb customer_revenue INFILE COUNTRY_NAME REGION
# 
# INFILE:: input csv-file sperated with colons (;) to operate on (result from 
#          #insert_customer_data)
# COUNTRY_NAME:: country-identifier for the resulting file
# REGION:: region-identifier for the resulting file
#
# The result will be in 
# 'COUNTRY_NAME-REGION-customer-revenues-per-year-and-type.csv' if no
# COUNTRY_NAME or REGION is given the result is in
# 'customer-revenues-per-year-and-type.csv'
def customer_revenue
  infile, result, *others = params

  country_part = ""
  country_part << "#{others[0]}-" if others[0]
  country_part << "#{others[1]}-" if others[1]
  out_file_name = "#{country_part}customer-revenue-per-year-and-type.csv"

  puts; print "Creating table from spares and repairs revenue for customers "+
              "in #{country_part.chop}"

  rp_order_type = %w{ ZRN ZRK }
  sp_order_type = %w{ ZE ZEI ZO ZOI ZG ZGNT ZRE ZGUP }
  order_type = sp_order_type + rp_order_type

  header = "c19,c20,BEGIN(c0.scan(/\\d+\\.\\d+\\.(\\d{4})/).flatten[0]||'')+"+
                      "'-SP-R'END,"+
                   "BEGIN(c0.scan(/\\d+\\.\\d+\\.(\\d{4})/).flatten[0]||'')+"+
                      "'-RP-R'END,"+
                   "BEGIN(c0.scan(/\\d+\\.\\d+\\.(\\d{4})/).flatten[0]||'')+"+
                      "'-R'END,"+
                   "BEGIN(c0.scan(/\\d+\\.\\d+\\.(\\d{4})/).flatten[0]||'')+"+
                      "'-SP-O'END,"+
                   "BEGIN(c0.scan(/\\d+\\.\\d+\\.(\\d{4})/).flatten[0]||'')+"+
                      "'-RP-O'END,"+
                   "BEGIN(c0.scan(/\\d+\\.\\d+\\.(\\d{4})/).flatten[0]||'')+"+
                      "'-O'END"

  cols = "BEGIN(c0.scan(/\\d+\\.\\d+\\.(\\d{4})/).flatten[0]||'')+"+
            "'-SP-R':+n10 if #{sp_order_type}.index(c1)END,"+
         "BEGIN(c0.scan(/\\d+\\.\\d+\\.(\\d{4})/).flatten[0]||'')+"+
                 "'-RP-R':+n10 if #{rp_order_type}.index(c1)END,"+
         "BEGIN(c0.scan(/\\d+\\.\\d+\\.(\\d{4})/).flatten[0]||'')+"+
                 "'-R':+n10 if #{order_type}.index(c1)END,"+
         "BEGIN(c0.scan(/\\d+\\.\\d+\\.(\\d{4})/).flatten[0]||'')+"+
                 "'-SP-O':+1 if #{sp_order_type}.index(c1)END,"+
         "BEGIN(c0.scan(/\\d+\\.\\d+\\.(\\d{4})/).flatten[0]||'')+"+
                 "'-RP-O':+1 if #{rp_order_type}.index(c1)END,"+
         "BEGIN(c0.scan(/\\d+\\.\\d+\\.(\\d{4})/).flatten[0]||'')+"+
                 "'-O':+1 if #{order_type}.index(c1)END"

  sum = "BEGIN(c0.scan(/\\d+\\.\\d+\\.(\\d{4})/).flatten[0]||'')+'-SP-R'END,"+
        "BEGIN(c0.scan(/\\d+\\.\\d+\\.(\\d{4})/).flatten[0]||'')+'-RP-R'END,"+
        "BEGIN(c0.scan(/\\d+\\.\\d+\\.(\\d{4})/).flatten[0]||'')+'-R'END,"+
        "BEGIN(c0.scan(/\\d+\\.\\d+\\.(\\d{4})/).flatten[0]||'')+'-SP-O'END,"+
        "BEGIN(c0.scan(/\\d+\\.\\d+\\.(\\d{4})/).flatten[0]||'')+'-RP-O'END,"+
        "BEGIN(c0.scan(/\\d+\\.\\d+\\.(\\d{4})/).flatten[0]||'')+'-O'END"

  Sycsvpro::Table.new(infile: infile,
                      outfile: out_file_name,
                      header:  header,
                      key:     "c19,c20",
                      cols:    cols,
                      nf:      "DE",
                      sum:     "top:#{sum}",
                      sort:    "2").execute

  puts; puts "You can find the result in #{out_file_name}"
end
