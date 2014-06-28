require 'syctimeleap/time_leap'

# Conducts a ABC analysis based on machine count
#
# :call-seq:
#   sycsvpro execute machine_age.rb abc_analysis INFILE COUNTRY_NAME
#
# INFILE:: input csv-file sperated with colons (;) to operate on
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
                                    cols:    "5:A=c4*c1,6:B=c3*c1,7:C=c2*c1",
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
# INFILE:: input csv-file sperated with colons (;) to operate on
# COUNTRY_NAME:: country-identifier for the resulting files
#   
# Result of machine ages is in the file 'machine-ages-COUNTRY_NAME.csv'. If no
# COUNTRY_NAME is given the result is in 'machine-ages-infile_basename.csv' 
def machine_age

  infile, result, *others = params
  ages_filename = "machine-ages-#{others[0] || File.basename(infile, '.*')}.csv"

  puts; print "Extracting date columns for #{infile}..."

  Sycsvpro::Extractor.new(infile: infile,
                          outfile: "extract.csv",
                          cols:    "45,10-12,84").execute

  puts; print "Determine the machine ages based on the oldest date..."

  Sycsvpro::Calculator.new(infile:  "extract.csv",
                           outfile: "calc.csv",
                           header:  "*,Age",
                           cols:    "5:Age=[d1,d2,d3,d4].compact.min",
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
                           cols:    "6:Older7Years=c1+c2").execute

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

# Extracts the top customers based on machine count and age
#
# :call-seq:
#   sycsvpro execute machine_age.rb machine_count_top INFILE COUNTRY_NAME COUNT
#   
# INFILE:: input csv-file sperated with colons (;) to operate on
#          Use the resulting file from #machine_age method invocation
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
# INFILE:: input csv-file sperated with colons (;) to operate on
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
# INFILE:: input csv-file sperated with colons (;) to operate on
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
# INFILE:: input csv-file sperated with colons (;) to operate on
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
# INFILE:: input csv-file sperated with colons (;) to operate on
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
#     Year | SP      | RP     | Total
#     ---- | ------- | ------ | -------
#          | 3500.50 | 300.30 | 3600.80
#     2013 | 2200.50 | 200.20 | 2400.70
#     2014 | 1300.00 | 100.10 | 1200.10
#
# :call-seq:
#   sycsvpro execute machine_age.rb region_revenue INFILE COUNTRY_NAME REGION
#   
# INFILE:: input csv-file sperated with colons (;) to operate on
#          Use the resulting file from #machine_age method invocation
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

  sp_order_type = %w{ ZRN ZRK }
  rp_order_type = %w{ ZE ZEI ZO ZOI ZG ZGNT ZRE ZGUP }

  Sycsvpro::Table.new(infile: infile,
                      outfile: out_file_name,
                      header:  "Year,SP,RP,Total",
                      key:     "c0=~/\\d+\\.\\d+\\.(\\d{4})/",
                      cols:    "SP:+n10 if #{sp_order_type}.index(c1),"+
                               "RP:+n10 if #{rp_order_type}.index(c1),"+
                               "Total:+n10 if #{sp_order_type}.index(c1) || "+
                                            "#{rp_order_type}.index(c1)",
                      nf:      "DE",
                      sum:     "top:SP,RP,Total").execute

  puts; puts "You can find the result in #{out_file_name}"
end
