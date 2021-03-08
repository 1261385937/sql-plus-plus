#pragma once
#include <memory>
#include <vector>
#include "db_common.h"
#include "exception.hpp"
#include "db_meta.hpp"

namespace sqlcpp {
	template<model Model, template<model M> typename ConnectionPool>
	class db {
	private:
		std::unique_ptr<ConnectionPool<Model>> pool_;
	public:
		db(std::vector<node_info> nodes, std::string user, std::string passwd) {
			if constexpr (Model == model::single) {
				pool_ = std::make_unique<ConnectionPool<Model>>(std::move(nodes[0]), std::move(user), std::move(passwd));
			}
			else if constexpr (Model == model::cluster) {
				pool_ = std::make_unique<ConnectionPool<Model>>(std::move(nodes), std::move(user), std::move(passwd));
			}
			else {
				static_assert(always_false_v<ConnectionPool<Model>>, "mode error");
			}
		}

		db(std::vector<node_info> nodes, std::string user, std::string passwd, std::string odbc_driver_name) {
			if constexpr (Model == model::single) {
				pool_ = std::make_unique<ConnectionPool<Model>>(std::move(nodes[0]), std::move(user), std::move(passwd), std::move(odbc_driver_name));
			}
			else if constexpr (Model == model::cluster) {
				pool_ = std::make_unique<ConnectionPool<Model>>(std::move(nodes), std::move(user), std::move(passwd), std::move(odbc_driver_name));
			}
			else {
				static_assert(always_false_v<ConnectionPool<Model>>, "mode error");
			}
		}


		template<conn_type Type>
		decltype(auto) get_conn() {
			return pool_->template get_connection<Type>();
		}
	};
}