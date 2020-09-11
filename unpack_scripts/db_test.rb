require 'sqlite3'
require 'csv'

def schema
  query = <<-SQL
    CREATE TABLE catalogue (
      id text PRIMARY KEY,
      url text UNIQUE,
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

def load_catalogue(tx, path)
  tx.execute(schema)

  catalogue = CSV.read(path, headers: true).map(&:to_h)

  insert_query = <<-SQL
    INSERT INTO catalogue VALUES (
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
      :endDate
    );
  SQL
  stmt = tx.prepare(insert_query)

  catalogue.each do |record|
    stmt.execute(record)
  end

  count = tx.query("SELECT count(*) FROM catalogue").next

  puts count
end

def load_all
  db = SQLite3::Database.new ":memory:"
  db.transaction do |tx|
    load_catalogue(tx, "data/catalogue.csv")
  end
rescue SQLite3::Exception => e 
  puts "Exception occurred"
  puts e
end


# Main
load_all
