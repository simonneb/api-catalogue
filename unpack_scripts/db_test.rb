require 'sqlite3'
require 'csv'

def schema
    query = <<-SQL
    CREATE TABLE catalogue (
      url text PRIMARY KEY,
      name text NOT NULL,
      description text NOT NULL,
      documentation text NOT NULL,
      license text,
      maintainer text,
      provider text,
      areaServed text,
      startDate date,
      endDate date
    );
    SQL

    query
end

begin
    db = SQLite3::Database.new ":memory:"
    db.execute(schema)

    catalogue = CSV.read('unpack_scripts/apic.csv', headers: true).map(&:to_h)

    # insert_query = <<-SQL
    # INSERT INTO catalogue VALUES (
    #   @url,
    #   @name,
    #   @description,
    #   @documentation,
    #   @license,
    #   @maintainer,
    #   @provider,
    #   @areaServed,
    #   @startDate,
    #   @endDate
    # );
    # SQL
    insert_query = <<-SQL
    INSERT INTO catalogue (url) VALUES (
      @url
    );
    SQL

    catalogue.each do |record|
      db.execute(insert_query, {'url'=> record['url']})
    end
	
	#dateadded = row['dateadded']
	#dateupdated = row['dateupdated']
    #uuid = row['uuid']
    #url = row['url']
    #name = row['name']
    #documentation = row['documentation']
    #license = row['license']
    #maintainer = row['maintainer']
    #provider = row['provider']
    #areaServed = row['areaServed']
    #startDate = row['startDate']
    #endDate = row['provider']
    #organisation = row['organisation']
	#typeof = row['endDate']
	
##	fields = File.join("#{dateadded}", "#{dateupdated}", "#{uuid}", "#{url}", "#{name}", "#{documentation}","#{license}", "#{maintainer}", "#{provider}", "#{areaServed}", "#{startDate}", "#{endDate}", "#{organisation}", "#{typeof}")
	
	#db.execute("INSERT INTO apic (dateadded, dateupdated, uuid, url, name, documentation, license, maintainer, provider, areaServed, startDate, endDate, organisation, typeof), VALUES (""#{dateadded}", "#{dateupdated}", "#{uuid}", "#{url}", "#{name}", "#{documentation}","#{license}", "#{maintainer}", "#{provider}", "#{areaServed}", "#{startDate}", "#{endDate}", "#{organisation}", "#{typeof}" ")")
    
    #id = db.last_insert_row_id
    #puts "The last id of the inserted row is #{id}"
        
    #end
        
rescue SQLite3::Exception => e 
    
    puts "Exception occurred"
    puts e
    
ensure
    db.close if db
end
