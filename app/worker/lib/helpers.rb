require 'digest/sha1'

module Worker
  module Helper
    MILLION = 1 * 1024 * 1024

    def provision_data(size)
      raise RuntimeError, "data size cannot be greater than 5 MB" if size > 5

      data = Random.new(Time.now.usec).bytes(size * MILLION)
      # remove specific charactor: \', \"
      data = data.gsub(/['"]/, "0")
      [data, sha1sum(data)]
    end

    def sha1sum(data)
      Digest::SHA1.hexdigest(data)
    end

    def think(thinktime)
      sleep(rand(20) / 20 * thinktime)
    end
  end
end