#pragma once
#include <vector>
#include <string>
#include <unordered_map>
#include <memory>
#include <thread>
#include <atomic>
#include <type_traits>
#include <mutex>
#include <algorithm>
#include <condition_variable>
#include "db_meta.hpp"
#include "db_common.h"

namespace sqlcpp::mysql {
	class connection;
	enum class fetch_type :uint8_t {
		single_master,
		single_slave,
		masters,
		slaves,
		all_members
	};

	class sentinel {
	private:
		std::string global_user_; //cluster members use same user name
		std::string global_passwd_; //cluster members use same password
		std::vector<node_info> seed_nodes_;
		std::vector<node_info> online_nodes_;
		std::unique_ptr<connection> conn_;
		//std::unordered_map<std::string, std::shared_ptr<connection>> alived_conns_;
		std::thread monitor_thread_;
		std::mutex mtx_;
		std::condition_variable cond_;
		std::atomic<bool> run_ = true;
	public:
		sentinel(const sentinel&) = delete;
		sentinel& operator=(const sentinel&) = delete;

		sentinel(std::vector<node_info> nodes, std::string global_user, std::string global_passwd)
			:global_user_(std::move(global_user))
			, global_passwd_(std::move(global_passwd))
			, online_nodes_(std::move(nodes))
		{
			std::sort(online_nodes_.begin(), online_nodes_.end()); //for compare
			seed_nodes_ = online_nodes_;

			monitor_thread_ = std::thread([this]() {
				while (run_) {
					for (const auto& node : seed_nodes_) {
						try {
							if (conn_ == nullptr) {
								conn_ = std::make_unique<connection>(connection_options{ node.ip,node.port,global_user_, global_passwd_ });
							}
							auto nodes = get_node<fetch_type::all_members>();
							if (nodes.empty()) {
								conn_.reset(); //the connection is not good
								continue;
							}

							std::sort(nodes.begin(), nodes.end()); //for compare
							if (nodes != online_nodes_) {
								decltype(seed_nodes_) seed_nodes;
								std::set_union(nodes.begin(), nodes.end(), seed_nodes_.begin(), seed_nodes_.end(), std::back_inserter(seed_nodes));
								seed_nodes_ = std::move(seed_nodes);
								online_nodes_ = std::move(nodes);
								cond_.notify_one();
							}
							break; // go to sleep
						}
						catch (const std::exception& e) {
							printf("make monitor connection error: %s\n", e.what());
							conn_.reset();
							std::this_thread::sleep_for(std::chrono::milliseconds(3000));//maybe network is bad
							continue; //ingnore error
						}
					}
					std::this_thread::sleep_for(std::chrono::milliseconds(3000)); //monitor frequency
				}
			});
		}

		~sentinel() {
			cond_.notify_one();
			run_ = false;
			if (monitor_thread_.joinable()) {
				monitor_thread_.join();
			}
		}

		std::vector<node_info> wait_for_cluster_change() {
			std::unique_lock lock(mtx_);
			cond_.wait(lock);
			return online_nodes_;
		}

		std::unique_ptr<connection> create_connection(const node_info& node) {
			return std::make_unique<connection>(connection_options{ node.ip, node.port, global_user_, global_passwd_ });
		}

		auto query_cluster_members(const std::unique_ptr<connection>& conn, std::string_view statement_sql) {
			return conn->query<std::tuple<std::string, std::string, std::string>>(statement_sql);
		}

		void wakeup() {
			cond_.notify_one();
		}

	private:
		template<fetch_type FetchType>
		return_if_t< FetchType == fetch_type::single_master || FetchType == fetch_type::single_slave, node_info, std::vector<node_info>>
			get_node() {
			std::string_view statement_sql;
			if constexpr (FetchType == fetch_type::single_master || FetchType == fetch_type::masters) {
				statement_sql = "select member_host, member_port, member_role from performance_schema.replication_group_members "
					"where member_state = 'ONLINE' and member_role = 'PRIMARY'";
			}
			else if constexpr (FetchType == fetch_type::single_slave || FetchType == fetch_type::slaves) {
				statement_sql = "select member_host, member_port, member_role from performance_schema.replication_group_members "
					"where member_state = 'ONLINE' and member_role = 'SECONDARY'";
			}
			else {
				statement_sql = "select member_host, member_port, member_role from performance_schema.replication_group_members "
					"where member_state = 'ONLINE'";
			}
			auto r = query_cluster_members(conn_, statement_sql);
			if (r.empty()) {
				return{};
			}

			if constexpr (FetchType == fetch_type::single_master || FetchType == fetch_type::single_slave) {
				return { std::move(std::get<0>(r[0])),std::move(std::get<1>(r[0])),std::move(std::get<2>(r[0])) };
			}
			else {
				std::vector<node_info> nodes;
				for (auto& tup : r) {
					nodes.emplace_back(node_info{ std::move(std::get<0>(tup)),std::move(std::get<1>(tup)),std::move(std::get<2>(tup)) });
				}
				return nodes;
			}
		}
	};
}