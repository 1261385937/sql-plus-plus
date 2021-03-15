#pragma once
#include <string_view>
#include <functional>
#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif
#include <sql.h>
#include <sqlext.h>
#include "db_common.h"
#include "db_meta.hpp"
#include "exception.hpp"
#include "reflection.hpp"

namespace sqlcpp::sqlserver {
	struct sqlserver_date {
		SQL_DATE_STRUCT value{};
		sqlserver_date() = default;
		sqlserver_date(uint64_t timestamp) {
			time_t ts = static_cast<time_t>(timestamp);
			auto s = localtime(&ts);
			value.year = (SQLSMALLINT)(s->tm_year + 1900);
			value.month = (SQLUSMALLINT)(s->tm_mon + 1);
			value.day = (SQLUSMALLINT)(s->tm_mday);
		}
	};

	struct sqlserver_datetime {
		SQL_TIMESTAMP_STRUCT value{};
		sqlserver_datetime() = default;
		sqlserver_datetime(uint64_t timestamp) {
			time_t ts = static_cast<time_t>(timestamp);
			auto s = localtime(&ts);
			value.year = (SQLSMALLINT)(s->tm_year + 1900);
			value.month = (SQLUSMALLINT)(s->tm_mon + 1);
			value.day = (SQLUSMALLINT)(s->tm_mday);
			value.hour = (SQLUSMALLINT)s->tm_hour;
			value.minute = (SQLUSMALLINT)s->tm_min;
			value.second = (SQLUSMALLINT)s->tm_sec;
		}
	};

	class connection {
	private:
		bool is_health_ = false;
		inline static std::atomic<int> conn_count_ = 0;
		connection_options opt_{};
		SQLHENV env_ = nullptr;
		SQLHDBC dbc_ = nullptr;
		SQLHSTMT stmt_ = nullptr;
		scope_guard<std::function<void()>> deleter_{};

	public:
		connection(const connection&) = delete;
		connection& operator=(const connection&) = delete;

		connection(const connection_options& opt, const std::string& driver_name) {
			opt_ = opt;
			deleter_.set_releaser([this]() {
				if (stmt_ != nullptr) {
					SQLFreeHandle(SQL_HANDLE_STMT, stmt_);
				}
				if (dbc_ != SQL_NULL_HDBC) {
					SQLDisconnect(dbc_);
					SQLFreeHandle(SQL_HANDLE_DBC, dbc_);
				}
				if (env_ != SQL_NULL_HENV) {
					SQLFreeHandle(SQL_HANDLE_ENV, env_);
				}
			});

			auto retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env_);
			if (retcode != SQL_SUCCESS) {
				throw except::sqlserver_exception("SQLAllocHandle(env) error:" + sqlserver_error(env_, SQL_HANDLE_ENV));
			}

			retcode = SQLSetEnvAttr(env_, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
			if (retcode != SQL_SUCCESS) {
				throw except::sqlserver_exception("SQLSetEnvAttr(version) error:" + sqlserver_error(env_, SQL_HANDLE_ENV));
			}

			retcode = SQLAllocHandle(SQL_HANDLE_DBC, env_, &dbc_);
			if (retcode != SQL_SUCCESS) {				
				throw except::sqlserver_exception("SQLAllocHandle(dbc) error:" + sqlserver_error(env_, SQL_HANDLE_ENV));
			}

			connect(opt, driver_name);
			retcode = SQLAllocHandle(SQL_HANDLE_STMT, dbc_, &stmt_);
			if (retcode != SQL_SUCCESS) {
				throw except::sqlserver_exception("SQLAllocHandle(stmt) error:" + sqlserver_error(dbc_, SQL_HANDLE_DBC));
			}
			is_health_ = true;
			conn_count_++;
			printf("sqlserver create conn <%s>, count:%d\n", opt_.ip.c_str(), conn_count_.load());
		}

		~connection() {
			conn_count_--;
			printf("sqlserver release conn <%s>, count:%d\n", opt_.ip.c_str(), conn_count_.load());
		}

		void execute(const std::string& sql) {
			auto retcode = SQLExecDirect(stmt_, (SQLCHAR*)sql.data(), SQL_NTS);
			if (retcode != SQL_SUCCESS) {
				is_health_ = false;
				throw except::sqlserver_exception("Failed to excute sql<" + sql + ">: " + sqlserver_error(stmt_, SQL_HANDLE_STMT));
			}
			scope_guard sg([this]() {
				auto retcode = SQLFreeStmt(stmt_, SQL_CLOSE);
				if (retcode != SQL_SUCCESS) {
					is_health_ = false;
					throw except::sqlserver_exception("SQLFreeStmt error:" + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}
			});
		}

		void begin_transaction() {
			execute("begin tran");
		}

		void commit_transaction() {
			execute("commit tran");
		}

		void rollback() {
			execute("rollback tran");
		}

		bool is_health() {
			return is_health_;
		}

		// this query has data back from sqlserver
		template<typename ReturnType, typename... Args>
		std::enable_if_t<is_tuple_v<ReturnType> || reflection::is_reflection_v<ReturnType>, std::vector<ReturnType>>
			query(std::string_view statement_sql, Args&&...args) {
			before_execute<ReturnType>(statement_sql, std::forward<Args>(args)...);
			//execute
			auto retcode = SQLExecute(stmt_);
			if (retcode != SQL_SUCCESS) {
				is_health_ = false;
				auto error_msg = std::string("failed to SQLExecute : ") + sqlserver_error(stmt_, SQL_HANDLE_STMT);
				throw except::sqlserver_exception(std::move(error_msg));
			}
			scope_guard sg([this]() {
				auto retcode = SQLFreeStmt(stmt_, SQL_CLOSE);
				if (retcode != SQL_SUCCESS) {
					is_health_ = false;
					throw except::sqlserver_exception("SQLFreeStmt error:" + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}
			});

			if constexpr (is_tuple_v<ReturnType>) {
				return after_execute<std::tuple_size_v<ReturnType>, ReturnType>();
			}
			else if constexpr (reflection::is_reflection_v<ReturnType>) {
				return after_execute<ReturnType::args_size_t::value, ReturnType>();
			}
		}

		// this query has single column data back from sqlserver
		template<typename ReturnType, typename... Args>
		std::enable_if_t<!is_tuple_v<ReturnType> && !reflection::is_reflection_v<ReturnType> && !std::is_same_v<ReturnType, void>,
			std::vector<ReturnType>> query(std::string_view statement_sql, Args&&...args) {
			before_execute<ReturnType>(statement_sql, std::forward<Args>(args)...);
			//execute
			auto retcode = SQLExecute(stmt_);
			if (retcode != SQL_SUCCESS) {
				is_health_ = false;
				auto error_msg = std::string("failed to SQLExecute : ") + sqlserver_error(stmt_, SQL_HANDLE_STMT);
				throw except::sqlserver_exception(std::move(error_msg));
			}
			scope_guard sg([this]() {
				auto retcode = SQLFreeStmt(stmt_, SQL_CLOSE);
				if (retcode != SQL_SUCCESS) {
					is_health_ = false;
					throw except::sqlserver_exception("SQLFreeStmt error:" + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}
			});

			return after_execute<1, ReturnType>();
		}

		// this query has no data back from sqlserver
		template<typename ReturnType, typename... Args>
		std::enable_if_t<std::is_same_v<ReturnType, void>>
			query(std::string_view statement_sql, Args&&...args) {
			before_execute<void>(statement_sql, std::forward<Args>(args)...);

			auto retcode = SQLExecute(stmt_);
			if (retcode != SQL_SUCCESS) {
				is_health_ = false;
				auto error_msg = std::string("failed to SQLExecute : ") + sqlserver_error(stmt_, SQL_HANDLE_STMT);
				throw except::sqlserver_exception(std::move(error_msg));
			}
			scope_guard sg([this]() {
				auto retcode = SQLFreeStmt(stmt_, SQL_CLOSE);
				if (retcode != SQL_SUCCESS) {
					is_health_ = false;
					throw except::sqlserver_exception("SQLFreeStmt error:" + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}
			});
		}

	private:
		void connect(const connection_options& opt, const std::string& driver_name) {
			auto retcode = SQLSetConnectAttr(dbc_, SQL_LOGIN_TIMEOUT, (SQLPOINTER)3, 0);
			if (retcode != SQL_SUCCESS) {
				throw except::sqlserver_exception("SQLSetConnectAttr(timeout) error:" + sqlserver_error(dbc_, SQL_HANDLE_DBC));
			}
			//"DRIVER={SQL Server}"
			//"Driver=ODBC Driver 17 for SQL Server"
			std::string host = ";SERVER=" + opt.ip;
			std::string user = ";UID=" + opt.user;
			std::string pwd = ";PWD=" + opt.passwd;
			auto conn_str = driver_name + host + user + pwd;
			//auto conn_str = driver + host + user + pwd + ";charset=gb2312";
			//auto conn_str = driver + host + user + pwd + ";charset=utf8";

			SQLSMALLINT len = 0;
			retcode = SQLDriverConnect(dbc_, nullptr, (SQLCHAR*)conn_str.data(), (SQLSMALLINT)conn_str.length(), nullptr, 0, &len, SQL_DRIVER_NOPROMPT);
			if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
				throw except::sqlserver_exception("Failed to connect to database:" + sqlserver_error(dbc_, SQL_HANDLE_DBC));
			}
		}

		std::string sqlserver_error(SQLHANDLE handle, SQLSMALLINT type) {
			SQLSMALLINT msg_len = 0;
			SQLCHAR sql_state[SQL_SQLSTATE_SIZE + 1]{};
			constexpr int len = 1024;
			SQLCHAR message[len]{};
			SQLINTEGER native_error = 0;
			std::string error{};
			if (SQLGetDiagRec(type, handle, 1, sql_state, &native_error, message, len, &msg_len) == SQL_SUCCESS) {
				error = (char*)message;
			}
			return error;
		}

		template <typename T>
		static constexpr auto sqlserver_type_map(T) {
			if constexpr (std::is_same_v<int8_t, T>) { //signed char
				return std::pair{ SQL_C_STINYINT,SQL_TINYINT };
			}
			else if constexpr (std::is_same_v<uint8_t, T>) { //unsigned char
				return std::pair{ SQL_C_UTINYINT,SQL_TINYINT };
			}
			else if constexpr (std::is_same_v<int16_t, T>) { //short int
				return std::pair{ SQL_C_SSHORT,SQL_SMALLINT };
			}
			else if constexpr (std::is_same_v<uint16_t, T>) { //unsigned short int
				return std::pair{ SQL_C_USHORT,SQL_SMALLINT };
			}
			else if constexpr (std::is_same_v<int32_t, T>) { //int
				return std::pair{ SQL_C_SLONG,SQL_INTEGER };
			}
			else if constexpr (std::is_same_v<uint32_t, T>) {//unsigned int
				return std::pair{ SQL_C_ULONG,SQL_INTEGER };
			}
			else if constexpr (std::is_same_v<float, T>) {// float
				return std::pair{ SQL_C_FLOAT,SQL_REAL };
			}
			else if constexpr (std::is_same_v<double, T>) {// double
				return std::pair{ SQL_C_DOUBLE,SQL_DOUBLE };
			}
			else if constexpr (std::is_same_v<int64_t, T>) {//long long int
				return  std::pair{ SQL_C_SBIGINT,SQL_BIGINT };
			}
			else if constexpr (std::is_same_v<uint64_t, T>) {//unsigned long long int
				return std::pair{ SQL_C_UBIGINT,SQL_BIGINT };
			}
			else if constexpr (std::is_convertible_v<T, std::string> || is_char_array_v<T> || std::is_same_v<T, std::string_view>) { //str
				return std::pair{ SQL_C_CHAR,SQL_CHAR };
			}
			else if constexpr (std::is_same_v<sqlserver_date, T>) {
				return std::pair{ SQL_C_TYPE_DATE ,SQL_TYPE_DATE };
			}
			else if constexpr (std::is_same_v<sqlserver_datetime, T>) {
				return std::pair{ SQL_C_TYPE_TIMESTAMP ,SQL_TYPE_TIMESTAMP };
			}
			else {
				static_assert(always_false_v<T>, "can not map to sqlserver type");
			}
		}

		template <typename T>
		constexpr std::enable_if_t<!is_optional_v<std::decay_t<T>>> build_bind_param(SQLUSMALLINT index, T&& t) {
			using U = std::remove_cv_t<std::remove_reference_t<decltype(t)>>;
			if constexpr (std::is_arithmetic_v<U>) { //built-in types
				//SQLLEN flag = 0;
				auto retcode = SQLBindParameter(stmt_, index, SQL_PARAM_INPUT,
					(SQLSMALLINT)sqlserver_type_map(t).first, (SQLSMALLINT)sqlserver_type_map(t).second, 0, 0, &t, 0, /*&flag*/nullptr);
				if (retcode != SQL_SUCCESS) {
					throw except::sqlserver_exception("SQLBindParameter error: " + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}
			}
			else if constexpr (is_char_array_v<U>) {
				auto retcode = SQLBindParameter(stmt_, index, SQL_PARAM_INPUT,
					(SQLSMALLINT)sqlserver_type_map(t).first, (SQLSMALLINT)sqlserver_type_map(t).second, sizeof(U) - 1, 0, const_cast<char*>(t), 0, nullptr);
				if (retcode != SQL_SUCCESS) {
					throw except::sqlserver_exception("SQLBindParameter error: " + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}
			}
			else if constexpr (is_char_pointer_v<U>) {
				auto retcode = SQLBindParameter(stmt_, index, SQL_PARAM_INPUT,
					(SQLSMALLINT)sqlserver_type_map(t).first, (SQLSMALLINT)sqlserver_type_map(t).second, strlen(t), 0, const_cast<char*>(t), 0, nullptr);
				if (retcode != SQL_SUCCESS) {
					throw except::sqlserver_exception("SQLBindParameter error: " + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}
			}
			else if constexpr (std::is_convertible_v<U, std::string> || std::is_same_v<U, std::string_view>) {
				auto retcode = SQLBindParameter(stmt_, index, SQL_PARAM_INPUT,
					(SQLSMALLINT)sqlserver_type_map(t).first, (SQLSMALLINT)sqlserver_type_map(t).second, t.length(), 0, const_cast<char*>(t.data()), 0, nullptr);
				if (retcode != SQL_SUCCESS) {
					throw except::sqlserver_exception("SQLBindParameter error: " + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}
			}
			else if constexpr (std::is_same_v<U, sqlserver_date>) {
				auto retcode = SQLBindParameter(stmt_, index, SQL_PARAM_INPUT,
					(SQLSMALLINT)sqlserver_type_map(t).first, (SQLSMALLINT)sqlserver_type_map(t).second, /*sizeof(SQL_DATE_STRUCT)*/0, 0, &t.value, 0, nullptr);
				if (retcode != SQL_SUCCESS) {
					throw except::sqlserver_exception("SQLBindParameter error: " + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}
			}
			else if constexpr (std::is_same_v<U, sqlserver_datetime>) {
				auto retcode = SQLBindParameter(stmt_, index, SQL_PARAM_INPUT,
					(SQLSMALLINT)sqlserver_type_map(t).first, (SQLSMALLINT)sqlserver_type_map(t).second, /*sizeof(SQL_DATE_STRUCT)*/0, 0, &t.value, 0, nullptr);
				if (retcode != SQL_SUCCESS) {
					throw except::sqlserver_exception("SQLBindParameter error: " + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}
			}
			else {
				static_assert(always_false_v<U>, "type do not match");
			}
		}

		template <typename T>
		std::enable_if_t<is_optional_v<std::decay_t<T>>> build_bind_param(SQLUSMALLINT index, T&& t) {
			using U = typename std::remove_cv_t<std::remove_reference_t<decltype(t)>>::value_type;
			if (!t.has_value()) {
				U v{};
				static SQLLEN flag = SQL_NULL_DATA;
				auto retcode = SQLBindParameter(stmt_, index, SQL_PARAM_INPUT,
					(SQLSMALLINT)sqlserver_type_map(v).first, (SQLSMALLINT)sqlserver_type_map(v).second, 0, 0, 0, 0, &flag);
				if (retcode != SQL_SUCCESS) {
					throw except::sqlserver_exception("SQLBindParameter error: " + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}
				return;
			}

			auto& v = t.value();
			if constexpr (std::is_arithmetic_v<U>) { //built-in types
				//SQLLEN flag = 0;
				auto retcode = SQLBindParameter(stmt_, index, SQL_PARAM_INPUT,
					(SQLSMALLINT)sqlserver_type_map(v).first, (SQLSMALLINT)sqlserver_type_map(v).second, 0, 0, &v, 0, /*&flag*/nullptr);
				if (retcode != SQL_SUCCESS) {
					throw except::sqlserver_exception("SQLBindParameter error: " + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}
			}
			else if constexpr (is_char_array_v<U>) {
				auto retcode = SQLBindParameter(stmt_, index, SQL_PARAM_INPUT,
					(SQLSMALLINT)sqlserver_type_map(v).first, (SQLSMALLINT)sqlserver_type_map(v).second, sizeof(U) - 1, 0, const_cast<char*>(v), 0, nullptr);
				if (retcode != SQL_SUCCESS) {
					throw except::sqlserver_exception("SQLBindParameter error: " + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}
			}
			else if constexpr (is_char_pointer_v<U>) {
				auto retcode = SQLBindParameter(stmt_, index, SQL_PARAM_INPUT,
					(SQLSMALLINT)sqlserver_type_map(v).first, (SQLSMALLINT)sqlserver_type_map(v).second, strlen(v), 0, const_cast<char*>(v), 0, nullptr);
				if (retcode != SQL_SUCCESS) {
					throw except::sqlserver_exception("SQLBindParameter error: " + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}
			}
			else if constexpr (std::is_convertible_v<U, std::string> || std::is_same_v<U, std::string_view>) {
				auto retcode = SQLBindParameter(stmt_, index, SQL_PARAM_INPUT,
					(SQLSMALLINT)sqlserver_type_map(v).first, (SQLSMALLINT)sqlserver_type_map(v).second, v.length(), 0, const_cast<char*>(v.data()), 0, nullptr);
				if (retcode != SQL_SUCCESS) {
					throw except::sqlserver_exception("SQLBindParameter error: " + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}
			}
			else if constexpr (std::is_same_v<U, sqlserver_date>) {
				auto retcode = SQLBindParameter(stmt_, index, SQL_PARAM_INPUT,
					(SQLSMALLINT)sqlserver_type_map(v).first, (SQLSMALLINT)sqlserver_type_map(v).second, /*sizeof(SQL_DATE_STRUCT)*/0, 0, &v.value, 0, nullptr);
				if (retcode != SQL_SUCCESS) {
					throw except::sqlserver_exception("SQLBindParameter error: " + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}
			}
			else if constexpr (std::is_same_v<U, sqlserver_datetime>) {
				auto retcode = SQLBindParameter(stmt_, index, SQL_PARAM_INPUT,
					(SQLSMALLINT)sqlserver_type_map(v).first, (SQLSMALLINT)sqlserver_type_map(v).second, 0, 0, &v.value, 0, nullptr);
				if (retcode != SQL_SUCCESS) {
					throw except::sqlserver_exception("SQLBindParameter error: " + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}
			}
			else {
				static_assert(always_false_v<U>, "type do not match");
			}
		}

		template <typename T>
		std::enable_if_t<!is_optional_v<std::decay_t<T>>>
			build_result_param(SQLUSMALLINT index, std::vector<std::pair<std::vector<char>, int>>& buf, T&& t, SQLLEN& ind) {
			using U = std::decay_t<T>;
			if constexpr (std::is_arithmetic_v<U>) { //built-in types
				auto retcode = SQLBindCol(stmt_, index, (SQLSMALLINT)sqlserver_type_map(t).first, &t, sizeof(U), &ind);
				if (retcode != SQL_SUCCESS) {
					throw except::sqlserver_exception("SQLBindCol error: " + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}
			}
			else if constexpr (is_char_pointer_v<U> || is_char_array_v<U>) {
				static_assert(always_false_v<U>, "use std::string instead of char pointer or char array");
			}
			else if constexpr (std::is_convertible_v<U, std::string>) {
				std::vector<char> tmp(65536, 0);
				buf.emplace_back(std::move(tmp), -1); //(SQLLEN*)&(buf.back().second)
				auto retcode = SQLBindCol(stmt_, index, (SQLSMALLINT)sqlserver_type_map(t).first, &(buf.back().first[0]), 65536, &ind);
				if (retcode != SQL_SUCCESS) {
					throw except::sqlserver_exception("SQLBindCol error: " + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}
			}
			else {
				static_assert(always_false_v<U>, "type do not match");
			}
		}

		template <typename T>
		std::enable_if_t<is_optional_v<std::decay_t<T>>>
			build_result_param(SQLUSMALLINT index, std::vector<std::pair<std::vector<char>, int>>& buf, T&& t, SQLLEN& ind) {
			using U = typename std::decay_t<T>::value_type;
			if constexpr (std::is_arithmetic_v<U>) { //built-in types
				t.emplace(U{});
				auto retcode = SQLBindCol(stmt_, index, (SQLSMALLINT)sqlserver_type_map(U{}).first, &t.value(), sizeof(U), &ind);
				if (retcode != SQL_SUCCESS) {
					throw except::sqlserver_exception("SQLBindCol error: " + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}
			}
			else if constexpr (is_char_pointer_v<U> || is_char_array_v<U>) {
				static_assert(always_false_v<U>, "use std::string instead of char pointer or char array");
			}
			else if constexpr (std::is_convertible_v<U, std::string>) {
				std::vector<char> tmp(65536, 0);
				buf.emplace_back(std::move(tmp), -1); //(SQLLEN*)&(buf.back().second)
				auto retcode = SQLBindCol(stmt_, index, (SQLSMALLINT)sqlserver_type_map(U{}).first, &(buf.back().first[0]), 65536, &ind);
				if (retcode != SQL_SUCCESS) {
					throw except::sqlserver_exception("SQLBindCol error: " + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}
			}
			else {
				static_assert(always_false_v<U>, "type do not match");
			}
		}

		template<typename ReturnType, typename... Args>
		void before_execute(std::string_view statement_sql, Args&&...args) {
			//prepare
			auto retcode = SQLPrepare(stmt_, (SQLCHAR*)statement_sql.data(), SQL_NTS);
			if (retcode != SQL_SUCCESS) {
				is_health_ = false;
				auto error_msg = std::string("Failed to SQLPrepare sql<") + std::string(statement_sql) + ">: "
					+ sqlserver_error(stmt_, SQL_HANDLE_STMT);
				throw except::sqlserver_exception(std::move(error_msg));
			}

			//check input size match
			SQLSMALLINT placeholder_size = 0;
			retcode = SQLNumParams(stmt_, &placeholder_size);
			if (retcode != SQL_SUCCESS) {
				throw except::sqlserver_exception("SQLNumParams error: " + sqlserver_error(stmt_, SQL_HANDLE_STMT));
			}
			constexpr auto args_size = sizeof...(args);
			if (placeholder_size != args_size) {
				throw except::sqlserver_exception("param size do not match placeholder size");
			}

			//check output size match
			if constexpr (!std::is_same_v<ReturnType, void>) { //tuple or reflect struct
				SQLSMALLINT column_count = 0;
				retcode = SQLNumResultCols(stmt_, &column_count);
				if (retcode != SQL_SUCCESS) {
					throw except::sqlserver_exception("SQLNumResultCols error: " + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}

				if constexpr (is_tuple_v<ReturnType>) {
					if (column_count != std::tuple_size_v<ReturnType>) {
						throw except::sqlserver_exception("columns in the query do not match tuple element size");
					}
				}
				else if constexpr (reflection::is_reflection_v<ReturnType>) {
					if (column_count != ReturnType::args_size_t::value) {
						throw except::sqlserver_exception("columns in the query do not match struct element size");
					}
				}
				else {
					if (column_count != 1) { //single type
						throw except::sqlserver_exception("columns size in the query must be 1");
					}
				}
			}

			//bind
			if constexpr (args_size > 0) {
				auto param_tup = std::forward_as_tuple(std::forward<Args>(args)...);
				for_each_tuple([&param_tup, this](auto index) {
					this->build_bind_param((SQLUSMALLINT)(index + 1), std::get<index>(param_tup));
				}, std::make_index_sequence<args_size>());
			}
		}

		template<typename Element, typename BufIter>
		void assign_result(SQLLEN ind, Element&& e, BufIter&& iter) {
			using T = std::decay_t<Element>;
			if constexpr (is_optional_v<T>) {
				if constexpr (std::is_arithmetic_v<typename T::value_type>) {
					if (ind == SQL_NULL_DATA) { //table filed is null, use empty optional instead of default value optional
						T temp{};
						e.swap(temp);
					}
				}
				else { //std::optional<std::string>
					if (ind != SQL_NULL_DATA) {
						e = std::string(iter->first.data(), (size_t)ind);
					}
					iter++;
				}
			}
			else {
				if constexpr (std::is_same_v<T, std::string>) {
					if (ind != SQL_NULL_DATA) {
						e = std::string(iter->first.data(), (size_t)ind);
					}
					iter++;
				}
			}
		}

		template<size_t ElementSize, typename ReturnType>
		auto after_execute() {
			//initialize results bind
			std::vector<std::pair<std::vector<char>, int>> buf_keeper; buf_keeper.reserve(ElementSize);
			std::array<SQLLEN, ElementSize> ind;
			ReturnType r{};
			if constexpr (is_tuple_v<ReturnType>) {
				for_each_tuple([&r, &buf_keeper, &ind, this](auto index) {
					this->build_result_param((SQLUSMALLINT)(index + 1), buf_keeper, std::get<index>(r), ind[index]);
				}, std::make_index_sequence<ElementSize>());
			}
			else if constexpr (reflection::is_reflection_v<ReturnType>) { //reflect
				constexpr auto address = ReturnType::elements_address();
				for_each_tuple([&r, &buf_keeper, &address, &ind, this](auto index) {
					this->build_result_param((SQLUSMALLINT)(index + 1), buf_keeper, r.*std::get<index>(address), ind[index]);
				}, std::make_index_sequence<ElementSize>());
			}
			else { //single type
				this->build_result_param(1, buf_keeper, r, ind[0]);
			}

			//get back data
			std::vector<ReturnType> back_data{};
			for (;;) {
				auto retcode = SQLFetch(stmt_);
				if (retcode == SQL_NO_DATA) { //no data now
					break;
				}
				else if (retcode == SQL_ERROR) {
					is_health_ = false;
					throw except::sqlserver_exception("SQLFetch error:" + sqlserver_error(stmt_, SQL_HANDLE_STMT));
				}

				auto iter = buf_keeper.begin();
				if constexpr (is_tuple_v<ReturnType>) {
					for_each_tuple([&r, &iter, &ind, this](auto index) {
						this->assign_result(ind[index], std::get<index>(r), iter);
					}, std::make_index_sequence<ElementSize>());
				}
				else if constexpr (reflection::is_reflection_v<ReturnType>) {
					constexpr auto address = ReturnType::elements_address();
					for_each_tuple([&r, &iter, &address, &ind, this](auto index) {
						this->assign_result(ind[index], r.*std::get<index>(address), iter);
					}, std::make_index_sequence<ElementSize>());
				}
				else { //single type
					this->assign_result(ind[0], r, iter);
				}
				back_data.emplace_back(std::move(r));
			}
			return back_data;
		}
	};
}