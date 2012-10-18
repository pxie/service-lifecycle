require "sqlite3"

module Utils
  module Results
    module_function

    RESULTS_TABLE = "results"

    def create_db()
      $db = SQLite3::Database.open(":memory:")
      $db.execute("create table if not exists #{RESULTS_TABLE}(id integer primary key," +
                      " time TIMESTAMP, worker text, op text, result text);")
      $log.debug("create results db. db: #{$db.inspect}")
    end

    def insert_result(worker, operation, result)
      time = Time.now
      $log.debug("insert result. time: #{time}, worker: #{worker}, operation: #{operation}, result: #{result}")
      @lock ||= Mutex.new
      @lock.synchronize do
        $db.execute("insert into results (time, worker, op, result)" +
                        " values ('#{time}', '#{worker}', '#{operation}', '#{result}')")
      end
    end

    def print_result()
      puts "\tOperation\t\tError Rate\t\n"
      $log.info("Operation,Error Rate")
      ops = $db.execute("select op from #{RESULTS_TABLE} group by op")
      $log.debug("SQL: select op from #{RESULTS_TABLE} group by op, result: #{ops.inspect}")
      ops.each do |op|
        op = op.first
        failures = $db.execute("select count(op) from #{RESULTS_TABLE} where op = '#{op}' and result = 'fail'").first.first
        $log.debug("failures: #{failures.class}, #{failures.inspect}")
        total = $db.execute("select count(op) from #{RESULTS_TABLE} where op = '#{op}'").first.first
        $log.debug("total: #{total.class}, #{total.inspect}")
        puts "\t#{op}\t\t#{failures * 100.0 / total}%\t\n"
        $log.info("#{op},#{failures * 100.0 / total}%")
      end

    end

    private
  end
end