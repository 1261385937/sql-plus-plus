#pragma once
#include <string>
#include <unordered_map>
#include <memory>
#include <thread>
#include <atomic>
#include <queue>
#include <memory>
#include <mutex>
#include "db_meta.hpp"
#include "sqlserver_connection.hpp"
#include "db_common.h"

namespace sqlcpp::sqlserver {
	template<model Model>
	class connection_pool {
	public:
		using general_pool = std::queue<std::unique_ptr<connection>>;
	private:
		//single mode
		std::mutex mtx_;
		node_info node_;
		general_pool pool_;
		std::string user_;
		std::string passwd_;
		std::string drive_name_;
	public:
		connection_pool(const connection_pool&) = delete;
		connection_pool& operator=(const connection_pool&) = delete;

		connection_pool(std::vector<node_info> nodes, std::string global_user, std::string global_passwd, std::string driver_name) {
			static_assert(Model == model::single, "sqlserver cluster model not support now");
		}

		connection_pool(node_info node, std::string user, std::string passwd, std::string driver_name)
			:node_(std::move(node)), user_(std::move(user)), passwd_(std::move(passwd)), drive_name_(std::move(driver_name))
		{	
		}

		template<conn_type Type>
		decltype(auto) get_connection() {
			std::unique_ptr<connection> conn;
			node_info node;

			if constexpr (Type == conn_type::slave) {
				static_assert(Type == conn_type::general, "sqlserver conn_type:slave not support now");
			}
			else if constexpr (Type == conn_type::master) {
				static_assert(Type == conn_type::general, "sqlserver conn_type:master not support now");
			}
			else if constexpr (Type == conn_type::general) {
				std::unique_lock<std::mutex> lock(mtx_);
				if (!pool_.empty()) {
					conn = std::move(pool_.front());
					pool_.pop();
				}
				lock.unlock();
			}
			else {
				static_assert(always_false_v<Type>, "unknown conn_type");
			}

			if (conn != nullptr) {
				if (conn->is_health()) {
					return connection_guard(std::move(conn), *this);
				}
			}
			
			//create new connection 
			if constexpr (Type == conn_type::general) {
				return connection_guard(create_connection(), *this);
			}
		}

		void return_back(std::unique_ptr<connection>&& p) {
			if constexpr (Model == model::single) {
				std::lock_guard<std::mutex> lock(mtx_);
				pool_.push(std::move(p));
			}
		}

	private:
		std::unique_ptr<connection> create_connection() {
			return std::make_unique<connection>(connection_options{ node_.ip, node_.port, user_, passwd_ }, drive_name_);
		}
	};
}