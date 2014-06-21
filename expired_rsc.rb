require 'syctimeleap/time_leap'

# Extract customers with RSCs and categorize regarding expiration date
#
# :call-seq:
#   sycsvpro execute expired_rsc.rb expired_rsc INFILE COUNTRY_NAME
#
# INFILE:: input csv-file sperated with colons (;) to operate on
# COUNTRY_NAME:: country-identifier for the resulting files
#
# Expects the insert file ~/.syc/sycsvpro/scripts/active_expired_RSC.ins
# If active_expired_RSC.ins is not available you can create it with
#
#     sycsvpro edit -s active_expired_RSC.ins
#
# and add following content
#
#     ;=b5;=c5;=d5;=e5
#     ;=sum(b3:b4);=sum(c3:c4);=sum(d3:d4);=sum(e3:e4)
#     active;=sum(c4:e4);=sum(d4:e4);=e4
#     expired;=b6;=c6;=d6;=e6     
#
# The result is in the file 'country_name-ibase.csv' if country_name is given
# otherwise the result is in infile_base_name-ibase.csv
def expired_rsc
  infile, result, *others = params

  result_file_name = "#{others[0] || File.base_name(infile, '.*')}-ibase.csv"

  puts; print "Extracting RSCs and categorize regarding expiration"

  df = "%d.%m.%Y"

  timeleap  = SycTimeleap::TimeLeap.new
  e2ya      = "<#{timeleap.b2y.strftime(df)}"
  e2ya1ya   = "#{timeleap.b2y.strftime(df)}-#{timeleap.b1y.strftime(df)}"
  e1yatoday = "#{(timeleap.b1y + 1).strftime(df)}-#{timeleap.tod.strftime(df)}"
  eatoday   = ">#{timeleap.tod.strftime(df)}"

  puts; puts "#{e2ya} #{e2ya1ya} #{e1yatoday} #{eatoday}"

  Sycsvpro::Counter.new(
              infile:  infile,
              outfile: "country-ibase.csv",
              rows:    "1-#{result.row_count}",
              key:     "45:Customer",
              cols:    "11:#{e2ya},11:#{e2ya1ya},11:#{e1yatoday},11:#{eatoday}",
              df:      "%d.%m.%Y",
              sort:    false,
              sum:     "Total:1,Sum").execute

  puts; print "Sorting customers based on count of RSCs"

  Sycsvpro::Sorter.new(infile:  "country-ibase.csv",
                       outfile: "country-ibase-sort.csv",
                       desc:    true,
                       cols:    "n:5").execute


  puts; print "Inserting calculation scheme for expired RSC histogram"

  insert_file = File.expand_path("~/.syc/sycsvpro/scripts/active_expired_RSC.ins")

  Sycsvpro::Inserter.new(infile:   "country-ibase-sort.csv",
                         outfile:  result_file_name,
                         insert:   insert_file,
                         position: "top").execute

  clean_up(["country-ibase.csv", "country-ibase-sort.csv"])

  puts; puts "You can find the result of the expired RSC statistics in "+
             "#{result_file_name}"

end
