
#include <cmath>
#include "seastar/core/reactor.hh"
#include "seastar/core/app-template.hh"
#include "seastar/rpc/rpc.hh"
#include "seastar/core/sleep.hh"
#include "seastar/rpc/lz4_compressor.hh"
#include <time.h>

using namespace seastar;

struct serializer {
};

template <typename T, typename Output>
inline
void write_arithmetic_type(Output& out, T v) {
    static_assert(std::is_arithmetic<T>::value, "must be arithmetic type");
    return out.write(reinterpret_cast<const char*>(&v), sizeof(T));
}

template <typename T, typename Input>
inline
T read_arithmetic_type(Input& in) {
    static_assert(std::is_arithmetic<T>::value, "must be arithmetic type");
    T v;
    in.read(reinterpret_cast<char*>(&v), sizeof(T));
    return v;
}

template <typename Output>
inline void write(serializer, Output& output, int32_t v) { return write_arithmetic_type(output, v); }
template <typename Output>
inline void write(serializer, Output& output, uint32_t v) { return write_arithmetic_type(output, v); }
template <typename Output>
inline void write(serializer, Output& output, int64_t v) { return write_arithmetic_type(output, v); }
template <typename Output>
inline void write(serializer, Output& output, uint64_t v) { return write_arithmetic_type(output, v); }
template <typename Output>
inline void write(serializer, Output& output, double v) { return write_arithmetic_type(output, v); }
template <typename Input>
inline int32_t read(serializer, Input& input, rpc::type<int32_t>) { return read_arithmetic_type<int32_t>(input); }
template <typename Input>
inline uint32_t read(serializer, Input& input, rpc::type<uint32_t>) { return read_arithmetic_type<uint32_t>(input); }
template <typename Input>
inline uint64_t read(serializer, Input& input, rpc::type<uint64_t>) { return read_arithmetic_type<uint64_t>(input); }
template <typename Input>
inline uint64_t read(serializer, Input& input, rpc::type<int64_t>) { return read_arithmetic_type<int64_t>(input); }
template <typename Input>
inline double read(serializer, Input& input, rpc::type<double>) { return read_arithmetic_type<double>(input); }

template <typename Output>
inline void write(serializer, Output& out, const sstring& v) {
    write_arithmetic_type(out, uint32_t(v.size()));
    out.write(v.c_str(), v.size());
}

template <typename Input>
inline sstring read(serializer, Input& in, rpc::type<sstring>) {
    auto size = read_arithmetic_type<uint32_t>(in);
    sstring ret(sstring::initialized_later(), size);
    in.read(ret.begin(), size);
    return ret;
}

namespace bpo = boost::program_options;
using namespace std::chrono_literals;

class mycomp : public rpc::compressor::factory {
    const sstring _name = "LZ4";
public:
    virtual const sstring& supported() const override {
        fmt::print("supported called\n");
        return _name;
    }
    virtual std::unique_ptr<rpc::compressor> negotiate(sstring feature, bool is_server) const override {
        fmt::print("negotiate called with {}\n", feature);
        return feature == _name ? std::make_unique<rpc::lz4_compressor>() : nullptr;
    }
};

int main(int ac, char** av) {
    app_template app;

    std::cout << "start ";
    rpc::protocol<serializer> myrpc(serializer{});
    static std::unique_ptr<rpc::protocol<serializer>::client> client;
    static double x = 30.0;

    myrpc.set_logger([] (const sstring& log) {
        fmt::print("{}", log);
        std::cout << std::endl;
    });

    return app.run_deprecated(ac, av, [&] {

        bool compress = false;
        static mycomp mc;
        std::cout << "client" << std::endl;
        auto c = myrpc.make_client<void (sstring tableKey)>(1);
        auto r = myrpc.make_client<sstring (sstring tableKey, sstring keyData)>(2);
        auto i = myrpc.make_client<void (sstring tableKey, sstring keyData, sstring value)>(3);
        auto u = myrpc.make_client<void (sstring tableKey, sstring keyData, sstring value)>(4);
        auto dd = myrpc.make_client<void (sstring tableKey, sstring keyData)>(5);
        auto dt = myrpc.make_client<void (sstring tableKey)>(6);

        rpc::client_options co;
        client = std::make_unique<rpc::protocol<serializer>::client>(myrpc, co, ipv4_addr{"127.0.0.1",9999});


        //test

        c(*client, "table1").then([]() {
            std::cout << "create table1" << std::endl;
       });

        c(*client, "table2").then([]() {
            std::cout << "create table2" << std::endl;
        });

        i(*client, "table1", "chuong", "441998").then([]() {
            std::cout << "insert table1" << std::endl;
        });

        i(*client, "table2", "cong", "1231998").then([]() {
            std::cout << "insert table2" << std::endl;
        });

        r(*client, "table1", "chuong").then([](sstring result) {
           std::cout << result << std::endl;
        });

        u(*client, "table2", "cong", "123").then([]() {
            std::cout << "update table2" << std::endl;
        });
        i(*client, "table2", "hieu", "1231996").then([]() {
            std::cout << "update table2" << std::endl;
        });

        dd(*client, "table2", "hieu").then([]() {
             std::cout << "delete hieu in table2" << std::endl;
        });

        dt(*client, "table1").then([]() {
        std::cout << "delete table1" << std::endl;
    });

        r(*client, "table1", "chuong").then([](sstring result) {
            std::cout << result << std::endl;
        });

        r(*client, "table2", "cong").then([](sstring result) {
            std::cout << result << std::endl;
        });


    });

}
