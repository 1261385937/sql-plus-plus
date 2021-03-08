#pragma once
#ifndef PERSONAL_EXCEPTION_HAS_CODE_
#define PERSONAL_EXCEPTION_HAS_CODE_

#include <stdexcept>
#include <sstream>
#include <string>

#ifdef _WIN32
#define EXCEPTION_SRC strrchr(__FILE__, '\\') ? strrchr(__FILE__, '\\') + 1 : __FILE__
#else
#define EXCEPTION_SRC strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__
#endif
#define EXCEPTION_LINE __LINE__

namespace except
{
	class exception_base :public std::exception
	{
	public:
		virtual int get_error_code() const { return 0; };
		virtual void rethrow() const {};
		virtual ~exception_base() {}
	};

#define DECLARE_EXCEPTION(class_name, base_class)										\
	class class_name : public base_class												\
	{																					\
	public:																				\
		class_name(){};																	\
		class_name(std::string error_msg, int error_code,								\
			const std::string& src_name, int src_line)									\
			:error_msg_(std::move(error_msg)),error_code_(error_code)					\
		{																				\
			std::ostringstream oss;														\
			oss << "Exception occured at "												\
				<< src_name << ":" << src_line << ". " << error_msg_;					\
			error_msg_ = oss.str();														\
		}																				\
		class_name(std::string error_msg,												\
			const std::string& src_name, int src_line)									\
			:error_msg_(std::move(error_msg))											\
		{																				\
			std::ostringstream oss;														\
			oss << "Exception occured at "												\
				<< src_name << ":" << src_line << ". " << error_msg_;					\
			error_msg_ = oss.str();														\
		}																				\
		class_name(std::string error_msg, int error_code)								\
			:error_msg_(std::move(error_msg)), error_code_(error_code) {}				\
		class_name(std::string error_msg)												\
			:error_msg_(std::move(error_msg)) {}										\
		int get_error_code() const { return error_code_; }								\
		const char* what() const noexcept { return error_msg_.c_str(); }				\
		void rethrow() const{ throw *this; }											\
	private:																			\
		std::string error_msg_{};														\
		int error_code_= 0;																\
	}

	constexpr int BASE_ERROR = -200000000;

	//define your own exception here...........................
	constexpr int MYSQL_ERROR = BASE_ERROR - 1;
	DECLARE_EXCEPTION(deserialize_exception, exception_base);
	DECLARE_EXCEPTION(sql_exception, exception_base);
	DECLARE_EXCEPTION(mysql_exception, sql_exception);
	DECLARE_EXCEPTION(sqlserver_exception, sql_exception);
}

#endif
