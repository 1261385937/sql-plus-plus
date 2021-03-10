#pragma once
#include <ctime>
#include <cstring>
#include <string>
#include <vector>
#include <tuple>
#include <array>
#include <typeinfo>
#include <chrono>
#include <mutex>
#include <atomic>
#include <functional>
#include <cassert>
#include "mysql.h"
#include "db_meta.hpp"
#include "exception.hpp"
#include "reflection.hpp"
#include "db_common.h"

namespace sqlcpp::mysql {
	struct mysql_timestamp {
		MYSQL_TIME mt{};
		mysql_timestamp(uint64_t timestamp) {
			time_t ts = static_cast<time_t>(timestamp);
			auto s = localtime(&ts);
			mt.year = (unsigned int)s->tm_year + 1900;
			mt.month = (unsigned int)s->tm_mon + 1;
			mt.day = (unsigned int)s->tm_mday;
			mt.hour = (unsigned int)s->tm_hour;
			mt.minute = (unsigned int)s->tm_min;
			mt.second = (unsigned int)s->tm_sec;
		}
	};

	struct mysql_mediumtext {
		std::string content;
		mysql_mediumtext() = default;
		mysql_mediumtext(std::string&& con) {
			content = std::move(con);
		}
	};

	class connection {
	private:
		std::string ip_;
		bool is_health_ = false;
		MYSQL* ctx_ = nullptr;
		MYSQL_STMT* smt_ctx_ = nullptr;
		scope_guard<std::function<void()>> deleter_{};
		inline static std::mutex mtx_{};
		inline static std::atomic<int> conn_count_ = 0;

	public:
		connection(const connection&) = delete;
		connection& operator=(const connection&) = delete;

		connection(const connection_options& opt)
			:ip_(opt.ip)
		{
			deleter_.set_releaser([this]() {
				if (smt_ctx_) {
					mysql_stmt_close(smt_ctx_);
				}
				if (ctx_) {
					mysql_close(ctx_);
				}
			});

			std::unique_lock<std::mutex> lock(mtx_);
			ctx_ = mysql_init(nullptr);
			lock.unlock();
			assert(ctx_);

			connect(opt);
			smt_ctx_ = mysql_stmt_init(ctx_);
			assert(smt_ctx_);
			is_health_ = true;
			conn_count_++;
			printf("mysql create conn <%s>, count:%d\n", ip_.c_str(), conn_count_.load());
		}

		~connection()
		{
			conn_count_--;
			printf("mysql release conn <%s>, count:%d\n", ip_.c_str(), conn_count_.load());
		}

		std::string& get_ip() {
			return ip_;
		}

		bool ping() {
			return mysql_ping(ctx_) == 0;
		}

		void execute(const std::string& sql) {
			auto ret = mysql_query(ctx_, sql.c_str());
			if (ret != 0) {
				is_health_ = false;
				auto error_msg = std::string("Failed to excute sql<") + sql + ">: " + mysql_error_msg();
				throw except::mysql_exception(std::move(error_msg));
			}
		}

		void begin_transaction() {
			execute("START TRANSACTION");
		}

		void commit_transaction() {
			execute("COMMIT");
		}

		void rollback() {
			execute("ROLLBACK");
		}

		uint64_t get_last_insert_id() {
			return mysql_stmt_insert_id(smt_ctx_);
		}

		bool is_health() {
			return is_health_;
		}

		auto get_conn_count() {
			return conn_count_.load();
		}

		// this query has data back from mysql
		template<typename ReturnType, typename... Args>
		std::enable_if_t<is_tuple_v<ReturnType> || reflection::is_reflection_v<ReturnType>, std::vector<ReturnType>>
			query(std::string_view statement_sql, Args&&...args) {
			before_execute<ReturnType>(statement_sql, std::forward<Args>(args)...);
			//execute
			auto ret = mysql_stmt_execute(smt_ctx_);
			if (ret != 0) {
				is_health_ = false;
				auto error_msg = std::string("Failed to stmt_execute : ") + mysql_error_msg();
				throw except::mysql_exception(std::move(error_msg));
			}

			if constexpr (is_tuple_v<ReturnType>) {
				return after_execute<std::tuple_size_v<ReturnType>, ReturnType>();
			}
			else {
				return after_execute<ReturnType::args_size_t::value, ReturnType>();
			}
		}

		// this query has single column data back from mysql
		template<typename ReturnType, typename... Args>
		std::enable_if_t<!is_tuple_v<ReturnType> && !reflection::is_reflection_v<ReturnType> && !std::is_same_v<ReturnType, void>,
			std::vector<ReturnType>> query(std::string_view statement_sql, Args&&...args) {
			before_execute<ReturnType>(statement_sql, std::forward<Args>(args)...);

			//execute
			auto ret = mysql_stmt_execute(smt_ctx_);
			if (ret != 0) {
				is_health_ = false;
				auto error_msg = std::string("failed to stmt_execute : ") + mysql_error_msg();
				throw except::mysql_exception(std::move(error_msg));
			}

			return after_execute<1, ReturnType>();
		}

		// this query has no data back from mysql
		template<typename ReturnType, typename... Args>
		std::enable_if_t<std::is_same_v<ReturnType, void>>
			query(std::string_view statement_sql, Args&&...args) {
			before_execute<void>(statement_sql, std::forward<Args>(args)...);
			auto ret = mysql_stmt_execute(smt_ctx_);
			if (ret != 0) {
				is_health_ = false;
				auto error_msg = std::string("failed to stmt_execute : ") + mysql_error_msg();
				throw except::mysql_exception(std::move(error_msg));
			}
		}

	private:
		std::string mysql_error_msg() {
			return std::string(mysql_error(ctx_));
		}

		void connect(const connection_options& opt) {
			int timeout = 3; //3s
			mysql_options(ctx_, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
			char value = 1; //yes
			mysql_options(ctx_, MYSQL_OPT_RECONNECT, &value);

			auto ret = mysql_real_connect(ctx_, opt.ip.c_str(), opt.user.c_str(), opt.passwd.c_str(),
				nullptr, (unsigned int)std::atoi(opt.port.c_str()), nullptr, 0);
			if (!ret) {
				auto error_msg = std::string("Failed to connect to database: ") + mysql_error_msg();
				throw except::mysql_exception(std::move(error_msg));
			}
		}

		template <typename T>
		constexpr std::enable_if_t<is_optional_v<std::decay_t<T>>> build_bind_param(MYSQL_BIND& param, T&& t) {
			using U = typename std::remove_cv_t<std::remove_reference_t<decltype(t)>>::value_type;
			if (!t.has_value()) {
				param.buffer_type = MYSQL_TYPE_NULL;
				return;
			}

			if constexpr (std::is_arithmetic_v<U>) { //built-in types
				param.buffer = const_cast<U*>(&t.value());
				param.buffer_type = mysql_type_map(t.value()).first;
				if constexpr (!std::is_same_v<U, float> && !std::is_same_v<U, double>) {
					param.is_unsigned = mysql_type_map(t.value()).second;
				}
			}
			else if constexpr (is_char_array_v<U>) {
				/*param.buffer = const_cast<char*>(&t[0]);
				param.buffer_length = sizeof(t);*/
				param.buffer = const_cast<char*>(t.value());
				param.buffer_length = (decltype(param.buffer_length))strlen(t.value());
				param.buffer_type = mysql_type_map(t.value()).first;
			}
			else if constexpr (is_char_pointer_v<U>) {
				param.buffer = const_cast<char*>(t.value());
				param.buffer_length = (decltype(param.buffer_length))strlen(t.value());
				param.buffer_type = mysql_type_map(t.value()).first;
			}
			else if constexpr (std::is_convertible_v<U, std::string> || std::is_same_v<U, std::string_view>) {
				std::string_view str{ t.value().data(),t.value().length() };
				param.buffer = (char*)(str.data());
				param.buffer_length = (decltype(param.buffer_length))str.length();
				param.buffer_type = mysql_type_map(t.value()).first;
			}
			else if constexpr (std::is_same_v<U, mysql_timestamp>) {
				param.buffer = const_cast<MYSQL_TIME*>(&t.value().mt);
				param.buffer_type = mysql_type_map(t.value()).first;
			}
			else if constexpr (std::is_same_v<U, mysql_mediumtext>) {
				std::string_view str{ t.value().content.data(),t.value().content.length() };
				param.buffer = (char*)(str.data());
				param.buffer_length = (decltype(param.buffer_length))str.length();
				param.buffer_type = mysql_type_map(t).first;
			}
			else {
				static_assert(always_false_v<U>, "type do not match");
			}
		}

		template <typename T>
		constexpr std::enable_if_t<!is_optional_v<std::decay_t<T>>> build_bind_param(MYSQL_BIND& param, T&& t) {
			using U = std::remove_cv_t<std::remove_reference_t<decltype(t)>>;
			if constexpr (std::is_arithmetic_v<U>) { //built-in types
				param.buffer = const_cast<U*>(&t);
				param.buffer_type = mysql_type_map(t).first;
				if constexpr (!std::is_same_v<U, float> && !std::is_same_v<U, double>) {
					param.is_unsigned = mysql_type_map(t).second;
				}
			}
			else if constexpr (is_char_array_v<U>) {
				/*param.buffer = const_cast<char*>(&t[0]);
				param.buffer_length = sizeof(t);*/
				param.buffer = const_cast<char*>(t);
				param.buffer_length = (decltype(param.buffer_length))strlen(t);
				param.buffer_type = mysql_type_map(t).first;
			}
			else if constexpr (is_char_pointer_v<U>) {
				param.buffer = const_cast<char*>(t);
				param.buffer_length = (decltype(param.buffer_length))strlen(t);
				param.buffer_type = mysql_type_map(t).first;
			}
			else if constexpr (std::is_convertible_v<U, std::string> || std::is_same_v<U, std::string_view>) {
				std::string_view str(std::forward<T>(t));
				param.buffer = (char*)(str.data());
				param.buffer_length = (decltype(param.buffer_length))str.length();
				param.buffer_type = mysql_type_map(t).first;
			}
			else if constexpr (std::is_same_v<U, mysql_timestamp>) {
				param.buffer = const_cast<MYSQL_TIME*>(&t.mt);
				param.buffer_type = mysql_type_map(t).first;
			}
			else if constexpr (std::is_same_v<U, mysql_mediumtext>) {
				std::string_view str(t.content);
				param.buffer = (char*)(str.data());
				param.buffer_length = (decltype(param.buffer_length))str.length();
				param.buffer_type = mysql_type_map(t).first;
			}
			else {
				static_assert(always_false_v<U>, "type do not match");
			}
		}

		template <typename T>
		std::enable_if_t<is_optional_v<std::decay_t<T>>>
			build_result_param(std::vector<std::pair<std::vector<char>, unsigned long>>& buf, MYSQL_BIND& param, T&& t, bool* is_null) {
			using U = typename std::remove_cv_t<std::remove_reference_t<decltype(t)>>::value_type;
			if constexpr (std::is_arithmetic_v<U>) { //built-in types
				t.emplace(U{});
				param.buffer = &t.value();
				param.buffer_type = this->mysql_type_map(U{}).first;
				param.is_null = is_null;
			}
			else if constexpr (is_char_pointer_v<U> || is_char_array_v<U>) {
				static_assert(always_false_v<U>, "use std::string instead of char pointer or char array");
			}
			else if constexpr (std::is_convertible_v<U, std::string>) {
				std::vector<char> tmp(65536, 0);
				buf.emplace_back(std::move(tmp), -1);
				param.buffer = &(buf.back().first[0]);
				param.buffer_length = 65536;
				param.buffer_type = mysql_type_map(U{}).first;
				param.length = &(buf.back().second);
				param.is_null = is_null;
			}
			else if constexpr (std::is_same_v<U, mysql_mediumtext>) {
				constexpr size_t size = 16 * 1024 * 1024;
				std::vector<char> tmp(size, 0);
				buf.emplace_back(std::move(tmp), -1);
				param.buffer = &(buf.back().first[0]);
				param.buffer_length = size;
				param.buffer_type = mysql_type_map(U{}).first;
				param.length = &(buf.back().second);
				param.is_null = is_null;
			}
			else {
				static_assert(always_false_v<U>, "type do not match");
			}
		}

		template <typename T>
		std::enable_if_t<!is_optional_v<std::decay_t<T>>>
			build_result_param(std::vector<std::pair<std::vector<char>, unsigned long>>& buf, MYSQL_BIND& param, T&& t, bool* = nullptr) {
			using U = std::remove_cv_t<std::remove_reference_t<decltype(t)>>;
			if constexpr (std::is_arithmetic_v<U>) { //built-in types
				param.buffer = &t;
				param.buffer_type = mysql_type_map(t).first;
			}
			/*else if constexpr (is_char_array_v<U>) {
				param.buffer = &t[0];
				param.buffer_length = sizeof(t);
				param.buffer_type = mysql_type_map(t).first;
			}*/
			else if constexpr (is_char_pointer_v<U> || is_char_array_v<U>) {
				static_assert(always_false_v<U>, "use std::string instead of char pointer or char array");
			}
			else if constexpr (std::is_convertible_v<U, std::string>) {
				std::vector<char> tmp(65536, 0);
				buf.emplace_back(std::move(tmp), -1);
				param.buffer = &(buf.back().first[0]);
				param.buffer_length = 65536;
				param.buffer_type = mysql_type_map(t).first;
				param.length = &(buf.back().second);
			}
			else if constexpr (std::is_same_v<U, mysql_mediumtext>) {
				constexpr size_t size = 16 * 1024 * 1024;
				std::vector<char> tmp(size, 0);
				buf.emplace_back(std::move(tmp), -1);
				param.buffer = &(buf.back().first[0]);
				param.buffer_length = size;
				param.buffer_type = mysql_type_map(U{}).first;
				param.length = &(buf.back().second);
			}
			else {
				static_assert(always_false_v<U>, "type do not match");
			}
		}

		template <typename T>
		static constexpr auto mysql_type_map(T) {
			if constexpr (std::is_same_v<int8_t, T>) { //signed char
				return std::pair{ MYSQL_TYPE_TINY ,false };
			}
			else if constexpr (std::is_same_v<uint8_t, T>) { //unsigned char
				return std::pair{ MYSQL_TYPE_TINY ,true };
			}
			else if constexpr (std::is_same_v<int16_t, T>) { //short int
				return std::pair{ MYSQL_TYPE_SHORT ,false };
			}
			else if constexpr (std::is_same_v<uint16_t, T>) { //unsigned short int
				return std::pair{ MYSQL_TYPE_SHORT ,true };
			}
			else if constexpr (std::is_same_v<int32_t, T>) { //int
				return std::pair{ MYSQL_TYPE_LONG ,false };
			}
			else if constexpr (std::is_same_v<uint32_t, T>) {//unsigned int
				return std::pair{ MYSQL_TYPE_LONG ,true };
			}
			else if constexpr (std::is_same_v<float, T>) {// float
				return std::pair{ MYSQL_TYPE_FLOAT ,false };
			}
			else if constexpr (std::is_same_v<double, T>) {// double
				return std::pair{ MYSQL_TYPE_DOUBLE ,false };
			}
			else if constexpr (std::is_same_v<int64_t, T>) {//long long int
				return std::pair{ MYSQL_TYPE_LONGLONG ,false };
			}
			else if constexpr (std::is_same_v<uint64_t, T>) {//unsigned long long int
				return std::pair{ MYSQL_TYPE_LONGLONG ,true };
			}
			else if constexpr (std::is_convertible_v<T, std::string> || is_char_array_v<T>
				|| std::is_same_v<T, std::string_view>) { //str
				return std::pair{ MYSQL_TYPE_STRING ,false };
			}
			else if constexpr (std::is_same_v<mysql_timestamp, T>) {
				return std::pair{ MYSQL_TYPE_TIMESTAMP ,false };
			}
			else if constexpr (std::is_same_v<mysql_mediumtext, T>) {
				return std::pair{ MYSQL_TYPE_MEDIUM_BLOB ,false };
			}
			/*else if constexpr (std::is_same_v<void, T>) {
				return std::pair{ MYSQL_TYPE_NULL ,false };
			}*/
			else {
				static_assert(always_false_v<T>, "can not map to mysql type");
			}
		}

		template<typename ReturnType, typename... Args>
		void before_execute(std::string_view statement_sql, Args&&...args) {
			//last_active_ = std::chrono::steady_clock::now();
			//prepare
			auto ret = mysql_stmt_prepare(smt_ctx_, statement_sql.data(), (unsigned long)statement_sql.length());
			if (ret != 0) {
				is_health_ = false;
				auto error_msg = std::string("Failed to stmt_prepare sql<") + std::string(statement_sql) + ">: " + mysql_error_msg();
				throw except::mysql_exception(std::move(error_msg));
			}

			//check input size match
			auto placeholder_size = mysql_stmt_param_count(smt_ctx_);
			constexpr auto args_size = sizeof...(args);
			if (placeholder_size != args_size) {
				throw except::mysql_exception("param size do not match placeholder size");
			}

			//check output size match
			if constexpr (!std::is_same_v<ReturnType, void>) { //tuple or reflect struct or single type
				auto meta_result = std::unique_ptr<MYSQL_RES, void(*)(MYSQL_RES*)>(mysql_stmt_result_metadata(smt_ctx_), [](MYSQL_RES* p) {if (p) mysql_free_result(p); });
				if (!meta_result) {
					auto error_msg = std::string("Failed to stmt_result_metadata : ") + mysql_error_msg();
					throw except::mysql_exception(std::move(error_msg));
				}

				auto column_count = mysql_num_fields(meta_result.get());
				if constexpr (is_tuple_v<ReturnType>) {
					if (column_count != std::tuple_size_v<ReturnType>) {
						throw except::mysql_exception("columns in the query do not match tuple element size");
					}
				}
				else if constexpr (reflection::is_reflection_v<ReturnType>) {
					if (column_count != ReturnType::args_size_t::value) {
						throw except::mysql_exception("columns in the query do not match struct element size");
					}
				}
				else {
					if (column_count != 1) { //single type
						throw except::mysql_exception("columns size in the query must be 1");
					}
				}
			}

			if constexpr (args_size > 0) {
				//initialize
				std::array<MYSQL_BIND, args_size> param_binds{};
				auto param_tup = std::forward_as_tuple(std::forward<Args>(args)...);
				for_each_tuple([&param_tup, &param_binds, this](auto index) {
					this->build_bind_param(param_binds[index], std::get<index>(param_tup));
				}, std::make_index_sequence<args_size>());

				//bind
				ret = mysql_stmt_bind_param(smt_ctx_, &param_binds[0]);
				if (ret != 0) {
					auto error_msg = std::string("Failed to stmt_bind_param : ") + mysql_error_msg();
					throw except::mysql_exception(std::move(error_msg));
				}
			}
		}

		template<typename Element, typename BufIter>
		void assign_result(bool is_field_null, Element&& e, BufIter&& iter) {
			using T = std::decay_t<Element>;
			if constexpr (is_optional_v<T>) {
				if constexpr (std::is_arithmetic_v<typename T::value_type>) {
					if (is_field_null) { //table filed is null, use empty optional instead of default value optional
						T temp{};
						e.swap(temp);
					}
				}
				else { //std::optional<std::string> or std::optional<mysql_mediumtext>
					if (iter->second != (unsigned long)-1) {
						if constexpr (std::is_same_v<typename T::value_type, mysql_mediumtext>) {
							e = mysql_mediumtext{ std::string(iter->first.data(), iter->second) };
						}
						else if constexpr (std::is_same_v<typename T::value_type, std::string>) {
							e = std::string(iter->first.data(), iter->second);
						}
					}
					iter++;
				}
			}
			else {
				if constexpr (std::is_same_v<T, std::string> || std::is_same_v<T, mysql_mediumtext>) {
					if (iter->second != (unsigned long)-1) {
						e = std::string(iter->first.data(), iter->second);
					}
					iter++;
				}
			}
		}

		template<size_t ElementSize, typename ReturnType>
		auto after_execute() {
			//initialize results bind
			std::array<bool, ElementSize> is_null{};
			std::vector<std::pair<std::vector<char>, unsigned long>> buf_keeper; buf_keeper.reserve(ElementSize);
			std::array<MYSQL_BIND, ElementSize> param_binds{};
			ReturnType r{};
			if constexpr (is_tuple_v<ReturnType>) {
				for_each_tuple([&r, &buf_keeper, &param_binds, &is_null, this](auto index) {
					this->build_result_param(buf_keeper, param_binds[index], std::get<index>(r), &is_null[index]);
				}, std::make_index_sequence<ElementSize>());
			}
			else if constexpr (reflection::is_reflection_v<ReturnType>) { //reflect
				constexpr auto address = ReturnType::elements_address();
				for_each_tuple([&r, &buf_keeper, &address, &param_binds, &is_null, this](auto index) {
					this->build_result_param(buf_keeper, param_binds[index], r.*std::get<index>(address), &is_null[index]);
				}, std::make_index_sequence<ElementSize>());
			}
			else { //single type	
				this->build_result_param(buf_keeper, param_binds[0], r, &is_null[0]);
			}

			//bind
			auto ret = mysql_stmt_bind_result(smt_ctx_, &(param_binds[0]));
			if (ret != 0) {
				auto error_msg = std::string("Failed to stmt_bind_param : ") + mysql_error_msg();
				throw except::mysql_exception(std::move(error_msg));
			}

			//buffer all results to client
			auto r_ret = mysql_stmt_store_result(smt_ctx_);
			if (r_ret != 0) {
				auto error_msg = std::string("Failed to stmt_store_result : ") + mysql_error_msg();
				throw except::mysql_exception(std::move(error_msg));
			}

			//get back data
			auto row_count = mysql_stmt_num_rows(smt_ctx_);
			std::vector<ReturnType> back_data{};
			back_data.reserve((std::size_t)row_count);

			while (!mysql_stmt_fetch(smt_ctx_)) {
				auto iter = buf_keeper.begin();
				if constexpr (is_tuple_v<ReturnType>) {
					for_each_tuple([&r, &iter, &is_null, this](auto index) {
						this->assign_result(is_null[index], std::get<index>(r), iter);
					}, std::make_index_sequence<ElementSize>());
				}
				else if constexpr (reflection::is_reflection_v<ReturnType>) {
					constexpr auto address = ReturnType::elements_address();
					for_each_tuple([&r, &address, &iter, &is_null, this](auto index) {
						this->assign_result(is_null[index], r.*std::get<index>(address), iter);
					}, std::make_index_sequence<ElementSize>());
				}
				else { //single type
					this->assign_result(is_null[0], r, iter);
				}
				back_data.emplace_back(std::move(r));
			}
			return back_data;
		}
	};
}