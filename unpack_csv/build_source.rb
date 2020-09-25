require 'sqlite3'
require 'csv'
require 'FileUtils'

def schema
  query = <<-SQL
    CREATE TABLE catalogue (
      dateadded date,
      dateupdated date,
      id text PRIMARY KEY,
      url text,
      name text NOT NULL,
      description text,
      documentation text,
      license text,
      maintainer text,
      provider text,
      areaServed text,
      startDate date,
      endDate date,
      organisation text
    );
  SQL

  query
end

def load_catalogue(tx, path)
  tx.execute(schema)

  catalogue = CSV.read(path, headers: true).map(&:to_h)

  insert_query = <<-SQL
    INSERT INTO catalogue VALUES (
      :dateadded,
      :dateupdated,
      :id,
      :url,
      :name,
      :description,
      :documentation,
      :license,
      :maintainer,
      :provider,
      :areaServed,
      :startDate,
      :endDate,
      :organisation
    );
  SQL
  stmt = tx.prepare(insert_query)

  catalogue.each do |record|
    stmt.execute(record)
  end


  
  # build the source folder and the department folder structure
  FileUtils.mkdir_p "../source"
  FileUtils.mkdir_p "../source/javascripts"
  FileUtils.mkdir_p "../source/stylesheets"
  FileUtils.cp("top.index.erb", "../source/index.html.md")
  FileUtils.cp("accessibility.html.md", "../source/accessibility.html.md")
  FileUtils.cp("javascripts/application.js", "../source/javascripts/application.js")
  FileUtils.cp_r("stylesheets/", "../source/")
  
  
  orgs = tx.query("SELECT ROW_NUMBER() OVER (ORDER BY organisation) RowNum, organisation, count(*) AS number, provider FROM catalogue GROUP BY organisation")

  orgs.each { |row|
    
     prov = row["provider"]
     #this is a hack to get round the ridiculous weighting in TDTs
     rank = row['RowNum'] * 20
     puts "Building folder for #{row["provider"]}, #{rank}"

     folder_path = File.join("..", "source", "#{prov}")
     FileUtils.mkdir_p folder_path

     File.open "#{folder_path}/index.html.md", 'w' do |file|
     
     #build headers for the deapartment index
     file.write "---\ntitle: #{row["organisation"]}\nweight: #{rank}\n---\n\n# #{row["organisation"]} APIs\n\nThe API catalogue contains the following #{row["number"]} #{row["organisation"]} (#{row["provider"]}) APIs:\n\n"

     #put each record in as a link
     provs = tx.query("SELECT name, maintainer, provider FROM catalogue WHERE provider = ? GROUP BY name", prov)
        provs.each { |nm| 
        link = nm['name'].gsub(" ","_") #replace spaces with underscores for links
        link = link.gsub("/", "_")
        
          file.write "- [#{nm['name']}](#{link}/)\n"
          
          # now build the folder and index for each API  
          api_pr = nm["provider"]
          api_name = nm["name"]
          api_link = nm['name'].gsub(" ","_") #replace spaces with underscores for links
		  # now get each API
          apis = tx.query("SELECT ROW_NUMBER() OVER (ORDER BY name) RowNum, name, url, description, documentation, license, maintainer, provider, areaServed, startDate, endDate, organisation FROM catalogue WHERE provider = ? AND name = ? GROUP BY name", api_pr, api_name)
              apis.each { |api|
              folder_path = File.join("source", "#{api_pr}", "#{api_link}")
              FileUtils.mkdir_p folder_path               
                puts "- #{api_pr} - Building folder for #{api["name"]} - #{folder_path}"
                File.open "#{folder_path}/index.html.md", 'w' do |file|
                     file.write "---\ntitle: #{api["name"]}\nweight: #{api["RowNum"] * 10}\n---\n\n# #{api["name"]}\n\n"
                     endpoint = api['url']
                     if !endpoint.nil?
                     file.write "## Endpoint URL:\n - [#{api['url']}](#{api['url']})\n\n"
                     end
  
                     documentation = api['documentation']
                     if !documentation.nil?
                     file.write "## Documentation URL:\n - [#{api['documentation']}](#{api['documentation']})\n\n"
                     end
  
                     contact = api['maintainer']
                     if !contact.nil?
                     file.write "## Contact:\n - [#{api['maintainer']}](mailto:#{api['maintainer']})\n\n"
                     end
    
                     file.write "## Description:\n#{api['description']}\n\n"

                     license = api['license']
                     if !license.nil?
                     file.write "## License:\n - #{api['license']}\n\n"
                     end
  
                     area = api['areaServed']
                     if !area.nil?
                     file.write "## Geographic Area:\n - #{api['areaServed']}\n\n"
                     end

                     start = api['startDate']
                     if !start.nil?
                     file.write "## Start Date:\n - #{api['startDate']}\n\n"
                     end

                     enddate = api['endDate']
                     if !enddate.nil?
                     file.write "## Expiry Date:\n - #{api['startDate']}\n\n"
                      end
                file.close
                end
            }
          
        }
    file.close
    end
     }

    
  #build the dashboard
  FileUtils.mkdir_p "../source/dashboard"
  
  File.open "../source/dashboard/index.html.md", 'w' do |file|
  
  count = tx.query("SELECT count(*) FROM catalogue").next
  number_of_depts = tx.query("SELECT count(distinct provider) FROM catalogue").next
  org_count = tx.query("SELECT organisation, count(*) AS number, provider, min(dateadded) AS firstadded, max(dateupdated) AS mostrecent FROM catalogue GROUP BY provider ORDER BY count(*) DESC")
  
  file.write  "---\ntitle: API Catalogue Contents\nweight: 1000\nhide_in_navigation: true\n---\n\n"
  file.write  "## API Catalogue Contents\n\n<div style=\"height:1px;font-size:1px;\">&nbsp;</div>\n\n|Total APIs:|Departments Represented:|\n|:---|:---|\n"
  file.write  "|#{count["count(*)"]}|#{number_of_depts["count(distinct provider)"]}|\n"
  file.write  "\n<div style=\"height:1px;font-size:1px;\">&nbsp;</div>\n\n|Department:|Number of APIs:|dateCreated:|dateUpdated:|URL:|\n|:---|:---|:---|:---|:---|\n"
  org_count.each { |org| 
    file.write  "|#{org['organisation']}|#{org['number']}|#{org['firstadded']}|#{org['mostrecent']}|[#{org['provider']}](/#{org['provider']}/)|\n"
   }
   
   file.close
   end
    
end

def load_all
  db = SQLite3::Database.new ":memory:"
  db.results_as_hash = true
  db.transaction do |tx|
    load_catalogue(tx, "data/apic.csv")
  end
rescue SQLite3::Exception => e
  puts "Exception occurred"
  puts e
end


# Main
load_all
