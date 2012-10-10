require 'digest/sha1'

module Worker
  module Helper
    MILLION = 1 * 1000 * 1000

    def provision_data(size, seed = nil)
      seed = rand(2 ** 32).to_s(36) unless seed
      size = size * MILLION
      data = seed * (size / seed.length) + seed[1..(size % seed.length)]
      [seed, data]
    end

    def sha1sum(data)
      Digest::SHA1.hexdigest(data)
    end
  end
end