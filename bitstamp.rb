require 'eventmachine'
require 'orderbook'
require 'json'
require 'openssl'

module Arke::Exchange
  class Bitstamp < Base
    attr_reader :last_update_id
    attr_accessor :orderbook

    def initialize(opts)
      super

      @url = "wss://ws.bitstamp.net"
      @connection = Faraday.new("https://www.bitstamp.net") do |builder|
        builder.adapter :em_synchrony
      end

      @conn2 = Faraday.new('https://www.bitstamp.net')

      @orderbook = Arke::Orderbook.new(@market)
      @last_update_id = 0
      @rest_api_connection = Faraday.new("https://#{opts['host']}") do |builder|
        builder.adapter :em_synchrony
        builder.headers['Content-Type'] = 'application/x-www-form-urlencoded'
      end

      @limit_order_count = 30
      @origin_depth = opts['depth']
      @max_amount_per_order = opts['max_amount_per_order'] || 0
      @spread = opts['spread'] || 0.05
    end

    # TODO: remove EM (was used only for debug)
    def start
      get_snapshot

      Thread.new do
        loop do
          sleep 60
          get_snapshot_2
        end
      end

      EM.run do
        @ws = Faye::WebSocket::Client.new(@url)

        @ws.on :open do |e|
          #p [:connected]
          message = {
              event: "bts:subscribe",
              data: {
                  channel: "diff_order_book_#{@market.downcase}"
              },
          }

          EM.next_tick {
            @ws.send(JSON.generate(message))
          }
        end

        @ws.on :message do |e|
          #p [:message, e.data]
          data = JSON.parse(e.data)
          if data['event'] == "data"
            on_message e
          elsif data['event'] == "bts:request_reconnect"
             @ws = nil
             @ws = Faye::WebSocket::Client.new(@url)
          end
        end

        @ws.on :error do |e|
          p [:error, e.inspect]
        end

        @ws.on :close do |e|
          p [:close, e.code, e.reason]

          @ws = nil
          @ws = Faye::WebSocket::Client.new(@url)
        end
      end

    end

    def on_message(mes)
      data = JSON.parse(mes.data)['data']
      return if @last_update_id >= data['microtimestamp'].to_i

      @last_update_id = data['microtimestamp'].to_i

      # Arke::Log.info "Process Bitstamp order Bids: #{data['bids'].length} Asks: #{data['asks'].length}"

      process(data['bids'], :buy) unless data['bids'].empty?
      process(data['asks'], :sell) unless data['asks'].empty?
    end

    def process(data, side, from_snapshot = false)
      best_price = data.first[0].to_f

      data.each do |order|
        if order[1].to_f < 10 ** (-8)
          @orderbook.delete(build_order(order, side))
          next
        end

        if from_snapshot && !contains_in_spread(side, best_price, order[0].to_f)
          @orderbook.delete(build_order(order, side))
          next
        end

        if order[0].to_f.zero?
          next
        end

        orderbook.update(
          build_order(order, side)
        )
      end

      if from_snapshot && @orderbook.book[side].size > 0
        @orderbook.book[side].reject! {|price, amount| !contains_in_spread(side, best_price, price) }
      end

      
      # if orderbook.book[side].size > @limit_order_count
      #   total = orderbook.book[side].size
      #   i = @limit_order_count
      #   while i < total
      #     orderbook.book[side].delete orderbook.book[side].last[0]
      #     i += 1
      #   end
      #
      #   Arke::Log.info " <======= Process Bitstamp #{side} last order Delete : #{total} -  #{i - @limit_order_count} "
      # end
    end

    def build_order(data, side)
      Arke::Order.new(
        @market,
        data[0].to_f,
        [data[1].to_f, @max_amount_per_order].min,
        side
      )
    end

    def get_snapshot
      snapshot = JSON.parse(@connection.get("api/v2/order_book/#{@market.downcase}").body)
      @last_update_id = snapshot['microtimestamp'].to_i
      process(snapshot['bids'][0...@limit_order_count], :buy, true)
      process(snapshot['asks'][0...@limit_order_count], :sell, true)
    end

    def get_snapshot_2
      snapshot = JSON.parse(@conn2.get("api/v2/order_book/#{@market.downcase}").body)
      @last_update_id = snapshot['microtimestamp'].to_i

      Arke::Log.info "Process Snapshot Bids: #{snapshot['bids'].length} Asks: #{snapshot['asks'].length}"

      process(snapshot['bids'][0...10], :buy, true)
      process(snapshot['asks'][0...10], :sell, true)
    end

    def contains_in_spread(side, best_price, price)
      if best_price <= 0.0
        true
      else
        return  best_price * (1 - @spread) <= price && price <= best_price if side === :buy
        return  best_price <= price && price <= best_price * (1 + @spread) if side === :sell
      end
    end

    def create_order(order)
      timestamp = Time.now.to_i * 1000
      body = {
        symbol: @market.upcase,
        side: order.side.upcase,
        type: 'LIMIT',
        timeInForce: 'GTC',
        quantity: order.amount.to_f,
        price: order.price.to_f,
        recvWindow: '5000',
        timestamp: timestamp
      }

      post('api/v3/order', body)
    end

    def generate_signature(data, timestamp)
      query = ""
      data.each { |key, value| query << "#{key}=#{value}&" }
      OpenSSL::HMAC.hexdigest(OpenSSL::Digest.new('sha256'), @secret, query.chomp('&'))
    end

    private

    def post(path, params = nil)
      request = @rest_api_connection.post(path) do |req|
        req.headers['X-MBX-APIKEY'] = @api_key
        req.body = URI.encode_www_form(generate_body(params))
      end

      Arke::Log.fatal(build_error(request)) if request.env.status != 200
      request
    end

    def generate_body(data)
      query = ""
      data.each { |key, value| query << "#{key}=#{value}&" }
      sig = OpenSSL::HMAC.hexdigest(OpenSSL::Digest.new('sha256'), @secret, query.chomp('&'))
      data.merge(signature: sig)
    end
  end
end
