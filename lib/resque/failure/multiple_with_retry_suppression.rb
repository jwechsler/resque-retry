require 'resque/failure/multiple'

module Resque
  module Failure

    # A multiple failure backend, with retry suppression
    #
    # For example: if you had a job that could retry 5 times, your failure
    # backends are not notified unless the _final_ retry attempt also fails.
    #
    # Example:
    #
    #   require 'resque-retry'
    #   require 'resque/failure/redis'
    #
    #   Resque::Failure::MultipleWithRetrySuppression.classes = [Resque::Failure::Redis]
    #   Resque::Failure.backend = Resque::Failure::MultipleWithRetrySuppression
    #
    class MultipleWithRetrySuppression < Multiple

      # Called when the job fails
      #
      # If the job will retry, suppress the failure from the other backends.
      # Store the lastest failure information in redis, used by the web
      # interface.
      #
      # @api private
      def save
        if !(retryable? && retrying?)
          cleanup_retry_failure_log!
          super
        elsif retry_delay > 0
          data = {
            :failed_at => Time.now.strftime("%Y/%m/%d %H:%M:%S"),
            :payload   => payload,
            :exception => exception.class.to_s,
            :error     => exception.to_s,
            :backtrace => Array(exception.backtrace),
            :worker    => worker.to_s,
            :queue     => queue
          }

          redis.setex(failure_key, 2*retry_delay, encode(data))
        end
      end

      # Expose this for the hook's use
      #
      # @api public
      def self.failure_key(retry_key)
        'failure-' + retry_key
      end

      protected

      # pulled from Resque::Helpers
      # I believe we can just use string.constantize in rails 3, but this isn't a rails-only gem...

      def classify(dashed_word)
        dashed_word.split('-').each { |part| part[0] = part[0].chr.upcase }.join
      end

      def constantize(camel_cased_word)
        camel_cased_word = camel_cased_word.to_s

        if camel_cased_word.include?('-')
          camel_cased_word = classify(camel_cased_word)
        end

        names = camel_cased_word.split('::')
        names.shift if names.empty? || names.first.empty?

        constant = Object
        names.each do |name|
          args = Module.method(:const_get).arity != 1 ? [false] : []

          if constant.const_defined?(name, *args)
            constant = constant.const_get(name)
          else
            constant = constant.const_missing(name)
          end
        end
        constant
      end

      # Return the class/module of the failed job.
      def klass
        constantize(payload['class'])
      end

      def retry_delay
        klass.retry_delay
      end

      def retry_key
        klass.redis_retry_key(*payload['args'])
      end

      def failure_key
        self.class.failure_key(retry_key)
      end

      def retryable?
        klass.respond_to?(:redis_retry_key)
      rescue NameError
        false
      end

      def retrying?
        redis.exists(retry_key)
      end

      def cleanup_retry_failure_log!
        redis.del(failure_key) if retryable?
      end
    end
  end
end
