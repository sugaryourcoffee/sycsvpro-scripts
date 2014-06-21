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
# COUNTRY_NAME is given the result is in 'ABC-analysis-infile_base_name.csv' 
def abc_analysis
  infile, result, *others = params
  abc_filename = "ABC-analysis-#{others[0] || File.base_name(infile, '.*')}.csv"

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
# COUNTRY_NAME is given the result is in 'machine-ages-infile_base_name.csv' 
def machine_age

  infile, result, *others = params
  ages_filename = "machine-ages-#{others[0] || File.base_name(infile, '.*')}.csv"

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
# 'A-customers-xxx-infile_base_name.csv' 
def machine_count_top
  infile, result, *others = params
  a_filename = "A-customers-#{others[0] || File.base_name(infile, '.*')}.csv"
  count_filename = "A-customers-count-#{others[0] || File.base_name(infile, '.*')}.csv"
  age_filename = "A-customers-age-#{others[0] || File.base_name(infile, '.*')}.csv"
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

