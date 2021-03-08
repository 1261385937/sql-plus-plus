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
#include "mysql_connection.hpp"
#include "mysql_sentinel.hpp"
#include "db_common.h"

namespace sqlcpp::mysql {
	template<model Model>
	class connection_pool {
	public:
		using master_pool = std::unordered_map<std::string, std::queue<std::unique_ptr<connection>>>; //usually one master, ip---conn
		using slave_pool = std::unordered_map<std::string, std::queue<std::unique_ptr<connection>>>; // ip---conn
		using general_pool = std::queue<std::unique_ptr<connection>>;
	private:
		std::unique_ptr<sentinel> sentine_;
		std::thread update_cluster_connections_thread_;
		std::atomic<bool> run_ = true;
		//cluster mode
		std::mutex master_mtx_;
		uint64_t master_fetch_times_ = 0;
		std::vector<node_info> masters_;
		uint8_t masters_count_ = 0;
		master_pool master_pool_;

		std::mutex  slave_mtx_;
		uint64_t slave_fetch_times_ = 0;
		std::vector<node_info> slaves_;
		uint8_t slaves_count_ = 0;
		slave_pool slave_pool_;

		//single mode
		std::mutex mtx_;
		node_info node_;
		general_pool pool_;
		std::string user_;
		std::string passwd_;
	public:
		connection_pool(const connection_pool&) = delete;
		connection_pool& operator=(const connection_pool&) = delete;

		connection_pool(connection_pool&&) = default;
		connection_pool& operator=(connection_pool&&) = default;

		connection_pool(std::vector<node_info> nodes, std::string global_user, std::string global_passwd)
			:sentine_(std::make_unique<sentinel>(std::move(nodes), std::move(global_user), std::move(global_passwd))) {
			update_cluster_connections_thread_ = std::thread(&connection_pool::update_cluster_connections, this);
		}

		connection_pool(node_info node, std::string user, std::string passwd)
			:node_(std::move(node)), user_(std::move(user)), passwd_(std::move(passwd))
		{}

		~connection_pool() {
			if constexpr (Model == model::cluster) {
				run_ = false;
				sentine_->wakeup();
				if (update_cluster_connections_thread_.joinable()) {
					update_cluster_connections_thread_.join();
				}
			}
		}
		
		template<conn_type Type>
		decltype(auto) get_connection() {
			std::unique_ptr<connection> conn;
			int index = 0;

			if constexpr (Type == conn_type::slave) {
				std::unique_lock<std::mutex> lock(slave_mtx_);
				slave_fetch_times_++;
				if (slaves_count_ == 0) {
					throw except::mysql_exception("mysql cluster no slave node found now");
				}

				index = slave_fetch_times_ % slaves_count_;
				auto iter = slave_pool_.find(slaves_[index].ip);
				auto& q = iter->second;
				if (!q.empty()) {
					conn = std::move(q.front());
					q.pop();
				}
				lock.unlock();
			}
			else if constexpr (Type == conn_type::master) {
				std::unique_lock<std::mutex> lock(master_mtx_);
				master_fetch_times_++;
				if (masters_count_ == 0) {
					throw except::mysql_exception("mysql cluster no master node found now");
				}

				index = master_fetch_times_ % masters_count_;
				auto iter = master_pool_.find(masters_[index].ip);
				auto& q = iter->second;
				if (!q.empty()) {
					conn = std::move(q.front());
					q.pop();
				}
				lock.unlock();
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
				printf("general ");
				return connection_guard(create_connection(), *this);
			}
			else if constexpr (Type == conn_type::master) {
				printf("master ");
				return connection_guard(sentine_->create_connection(masters_[index]), *this);
			}
			else if constexpr (Type == conn_type::slave) {
				printf("slave ");
				return connection_guard(sentine_->create_connection(slaves_[index]), *this);
			}
		}
		
		void return_back(std::unique_ptr<connection>&& p) {
			if constexpr (Model == model::cluster) {
				{
					std::lock_guard<std::mutex> lock_slave(slave_mtx_);
					if (auto iter = slave_pool_.find(p->get_ip()); iter != slave_pool_.end()) {
						iter->second.emplace(std::move(p));
						return;
					}
				}
				{
					std::lock_guard<std::mutex> master_slave(master_mtx_);
					if (auto iter = master_pool_.find(p->get_ip()); iter != master_pool_.end()) {
						iter->second.emplace(std::move(p));
					}
				}
			}
			else if constexpr (Model == model::single) {
				std::lock_guard<std::mutex> lock(mtx_);
				pool_.emplace(std::move(p));
			}
			else {
				static_assert(always_false_v<Model>, "unknown launch_model");
			}
		}

	private:
		void update_cluster_connections() {
			while (run_) {
				auto changed_cluster = sentine_->wait_for_cluster_change();

				std::scoped_lock lock(master_mtx_, slave_mtx_);
				decltype(master_pool_) master_pool;
				decltype(slave_pool_) slave_pool;
				masters_.clear(); masters_count_ = 0;
				slaves_.clear(); slaves_count_ = 0;

				for (auto& node : changed_cluster) {
					if (node.role == "PRIMARY") { //master
						//remain the old conns
						if (auto iter = master_pool_.find(node.ip); iter != master_pool_.end()) {
							master_pool.emplace(iter->first, std::move(iter->second));
						}
						else if (auto iter1 = slave_pool_.find(node.ip); iter1 != slave_pool_.end()) {
							master_pool.emplace(iter1->first, std::move(iter1->second));
						}
						else {
							master_pool[node.ip]; //new master appeared, Lazy create
						}
						masters_.emplace_back(std::move(node));
					}
					else { //slave
						//remain the old conns
						if (auto iter = slave_pool_.find(node.ip); iter != slave_pool_.end()) {
							slave_pool.emplace(iter->first, std::move(iter->second));
						}
						else if (auto iter1 = master_pool_.find(node.ip); iter1 != master_pool_.end()) {
							slave_pool.emplace(iter1->first, std::move(iter1->second));
						}
						else {
							slave_pool[node.ip]; //new slave appeared, Lazy create
						}
						slaves_.emplace_back(std::move(node));
					}
				}
				//replace old pool
				master_pool_ = std::move(master_pool); masters_count_ = (uint8_t)master_pool_.size();
				slave_pool_ = std::move(slave_pool); slaves_count_ = (uint8_t)slave_pool_.size();
			}
		}

		std::unique_ptr<connection> create_connection() {
			return std::make_unique<connection>(connection_options{ node_.ip, node_.port, user_, passwd_ });
		}
	};
}