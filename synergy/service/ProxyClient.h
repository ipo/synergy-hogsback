#ifndef PROXYSTREAM_H
#define PROXYSTREAM_H

#include <string>
#include <stdexcept>
#include <type_traits>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/post.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket/teardown.hpp>
#include <boost/algorithm/string/join.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <synergy/service/ServiceLogs.h>

template <typename NextLayer>
class ProxyClient {
public:
    using next_layer_type = NextLayer;
    using lowest_layer_type = typename next_layer_type::lowest_layer_type;
    using executor_type = typename next_layer_type::executor_type;

    template <typename NextLayerInit>
    explicit ProxyClient (NextLayerInit&&, std::string host = "",
                          int port = 80);

    bool enabled() const noexcept;
    next_layer_type& next_layer() noexcept;
    lowest_layer_type& lowest_layer() noexcept;
    executor_type get_executor() noexcept;

    template <typename Handler>
    auto
    async_connect (boost::asio::ip::tcp::resolver::iterator, Handler&&) ->
        typename boost::asio::async_result<
            std::decay_t<Handler>,
            void(boost::system::error_code)>::return_type;


    template <typename Buffer>
    std::size_t
    read_some (Buffer&&, boost::system::error_code& ec);

    template <typename Buffer>
    std::size_t
    write_some (Buffer const&, boost::system::error_code& ec);

    template <typename Buffer, typename Handler>
    void async_read_some (Buffer&&, Handler&&);

    template <typename Buffer, typename Handler>
    void async_write_some (Buffer const&, Handler&&);

    bool setProxy (std::string host, int port = 80);

private:
    next_layer_type m_nextLayer;
    boost::asio::ip::tcp::resolver m_resolver;
    std::string m_host;
    int m_port = 80;
    bool m_connected = false;
};

template<typename NextLayer> inline
bool
ProxyClient<NextLayer>::enabled() const noexcept {
    return !m_host.empty();
}

template<typename NextLayer> inline
typename ProxyClient<NextLayer>::next_layer_type&
ProxyClient<NextLayer>::next_layer() noexcept {
    return m_nextLayer;
}

template<typename NextLayer> inline
typename ProxyClient<NextLayer>::lowest_layer_type&
ProxyClient<NextLayer>::lowest_layer() noexcept {
    return m_nextLayer.lowest_layer();
}

template<typename NextLayer> inline
typename ProxyClient<NextLayer>::executor_type
ProxyClient<NextLayer>::get_executor() noexcept {
    return m_nextLayer.get_executor();
}

template<typename NextLayer>
bool
ProxyClient<NextLayer>::setProxy(std::string host, int port) {
    if ((port < 0) || (port > 65535)) {
        return false;
    } else if (!port) {
        port = 80;
    }
    bool const changed = (std::tie (host, port) != std::tie (m_host, m_port));
    if (changed) {
        m_host = std::move (host);
        m_port = port;
        if (m_nextLayer.is_open()) {
            boost::system::error_code ec;
            m_nextLayer.close(ec);
        }
        m_connected = false;
    }
    return changed;
}

template <typename NextLayer>
template <typename NextLayerInit> inline
ProxyClient<NextLayer>::ProxyClient (NextLayerInit&& nextLayer,
                                     std::string host, int const port):
    m_nextLayer (std::forward<NextLayerInit>(nextLayer)),
    m_resolver (m_nextLayer.get_executor()),
    m_host (std::move (host)),
    m_port (port) {
}

template <typename NextLayer>
template <typename Handler> inline
auto
ProxyClient<NextLayer>::async_connect
(boost::asio::ip::tcp::resolver::iterator serverItr, Handler&& handler) ->
    typename boost::asio::async_result<
        std::decay_t<Handler>,
        void(boost::system::error_code)>::return_type
{
    boost::asio::async_completion<Handler, void(boost::system::error_code)>
        init(handler);
    auto completionHandler = std::move(init.completion_handler);

    if (m_connected) {
        throw std::runtime_error ("connect() called on a proxy client that's "
                                  "already connected");
    }

    if (serverItr == boost::asio::ip::tcp::resolver::iterator()) {
        boost::asio::post (get_executor(),
            [handler = std::move(completionHandler)]() mutable {
                handler (boost::asio::error::host_not_found);
            });
    } else if (this->enabled()) {
        auto target = fmt::format ("{}:{}", serverItr->host_name(),
                                   serverItr->service_name());

        // TODO: Using synchronous read and write operations in this handler
        //       is a bad idea. To maintain the illusion of a regular stream
        //       these should be properly composed async operations with timeouts
        auto connect_handler = [this, target, completionHandler](auto ec)
                mutable {
            if (!ec) {
                boost::beast::http::request<boost::beast::http::empty_body> req;
                req.version (11);
                req.method (boost::beast::http::verb::connect);
                req.target (target);

                boost::beast::http::write (m_nextLayer, req, ec);
                if (ec) {
                    completionHandler (ec);
                    return;
                }

                boost::beast::multi_buffer mb;
                boost::beast::http::response_parser<boost::beast::http::empty_body> resp;
                resp.skip (true);
                boost::beast::http::read (m_nextLayer, mb, resp, ec);
                if (!ec) {
                    m_connected = true;
                }
            }
            completionHandler (ec);
        };

        // Test if the proxy hostname is an IP string or a hostname by
        // attempting a conversion.
        boost::system::error_code ec;
        auto ip = boost::asio::ip::address_v4::from_string(m_host, ec);

        if (ec) {
            // If we're dealing with a hostname we need to resolve before we can
            // continue. Ideally we'd want to do this in setProxy() but then
            // we'd somehow need to block any connect() call until the result
            // was in.
            using query_type = typename decltype(m_resolver)::query;

            serviceLog()->warn("Resolving proxy host name '{}'...", m_host);
            m_resolver.async_resolve (query_type(m_host, std::to_string(m_port),
                                      query_type::flags::numeric_service
                                    | query_type::flags::address_configured),
            [this, connect_handler](auto ec, auto proxyIt) mutable {
                std::vector<std::string> addresses;

                std::transform (proxyIt, decltype(proxyIt)(),
                                std::back_inserter(addresses), [](auto& re) {
                    return re.endpoint().address().to_string();
                });

                serviceLog()->warn("Proxy hostname ('{}') resolved to {}. Connecting...",
                                   m_host, boost::algorithm::join (addresses, ", "));

                if (ec) {
                    connect_handler (ec);
                } else {
                    boost::asio::async_connect (m_nextLayer, proxyIt,
                        [connect_handler](auto ec, auto) mutable {
                            connect_handler (ec);
                        });
                }
            });
        } else {
            serviceLog()->warn("Connecting to proxy at {}...", m_host);
            auto proxy = boost::asio::ip::tcp::endpoint (ip, m_port);
            m_nextLayer.async_connect (proxy, std::move (connect_handler));
        }
    } else {
        boost::asio::async_connect (m_nextLayer, serverItr,
                                    [this, completionHandler](auto ec, auto) mutable {
            if (!ec) {
                this->m_connected = true;
            }
            completionHandler (ec);
        });
    }

    return init.result.get();
}

template <typename NextLayer>
template <typename Buffer> inline
std::size_t
ProxyClient<NextLayer>::read_some (Buffer&& buffer,
                                   boost::system::error_code& ec) {
    return m_nextLayer.read_some (buffer, ec);
}

template <typename NextLayer>
template <typename Buffer, typename Handler> inline
void
ProxyClient<NextLayer>::async_read_some (Buffer&& buffer, Handler&& handler) {
    return m_nextLayer.async_read_some (std::forward<Buffer>(buffer),
                                        std::forward<Handler>(handler));
}

template <typename NextLayer>
template <typename Buffer, typename Handler> inline
void
ProxyClient<NextLayer>::async_write_some (Buffer const& buffer,
                                          Handler&& handler) {
    return m_nextLayer.async_write_some (buffer,
                                         std::forward<Handler>(handler));
}

template <typename NextLayer>
template <typename Buffer> inline
std::size_t
ProxyClient<NextLayer>::write_some (Buffer const& buffer,
                                    boost::system::error_code& ec) {
    return m_nextLayer.write_some (buffer, ec);
}

template <typename NextLayer> inline
void
beast_close_socket(ProxyClient<NextLayer>& stream)
{
    boost::beast::beast_close_socket(stream.next_layer());
}

template <typename NextLayer> inline
void
teardown(boost::beast::role_type role,
         ProxyClient<NextLayer>& stream,
         boost::beast::error_code& ec)
{
    using boost::beast::websocket::teardown;
    teardown(role, stream.next_layer(), ec);
}

template <typename NextLayer, typename TeardownHandler> inline
void
async_teardown(boost::beast::role_type role,
               ProxyClient<NextLayer>& stream,
               TeardownHandler&& handler)
{
    using boost::beast::websocket::async_teardown;
    async_teardown(role, stream.next_layer(),
                   std::forward<TeardownHandler>(handler));
}

#endif // PROXYSTREAM_H
