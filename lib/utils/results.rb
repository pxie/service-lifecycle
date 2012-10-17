require "sqlite3"

module Utils
  module Results
    module_function

    RESULTS_TABLE = "results"

    def get_db()
      @db = SQLite3::Database.open(":memory:")
      @db.execute("create table if not exists #{RESULTS_TABLE}(id integer primary key," +
                      " time TIMESTAMP, worker text, op text, result text);")
    end

    def insert_result(time, worker, operation, result)
      @lock ||= Mutex.new
      @lock.synchronize do
        @db.execute("insert into results (time, worker, operation, result)" +
                        " values ('#{time}', '#{worker}', '#{operation}', '#{result}')")
      end
    end

    def print_result()
      puts "\tOperation\t\tError Rate\t\n"
      ops = @db.execute("select op from #{RESULTS_TABLE} group by op")
      $log.debug("SQL: select op from #{RESULTS_TABLE} group by op, result: #{ops.inspect}")
      ops.each do |op|
        failures = @db.execute("select count(op) from #{RESULTS_TABLE} where op = #{op} and result = 'fail'")
        total = @db.execute("select count(op) from #{RESULTS_TABLE} where op = #{op}")
        puts "\t#{op}\t\t#{failures/total * 100}%\t\n"
      end

    end

    private
  end
end