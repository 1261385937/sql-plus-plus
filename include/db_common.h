#pragma once
#include <string>
#include <memory>

namespace sqlcpp {
	struct connection_options {
		std::string ip;
		std::string port;
		std::string user;
		std::string passwd;
	};

	struct node_info {
		std::string ip;
		std::string port{};
		//std::string user;
		//std::string passwd;
		std::string role{};

		bool operator==(const node_info& n) const {
			return role == n.role && ip == n.ip/* && port == port*/;
		}
		bool operator<(const node_info& n)  const {
			return ip < n.ip;
		}
	};

	enum class model {
		single,
		cluster
	};

	enum class conn_type {
		slave,
		master,
		general
	};

	template <typename Conn, typename ConnPool>
	class connection_guard {
	public:
		connection_guard(connection_guard&&) = default;
		connection_guard& operator=(connection_guard&& cg) noexcept {
			if (this->conn_) {
				this->connection_pool_.return_back(std::move(this->conn_));
			}

			if (cg.conn_) {
				this->conn_ = std::move(cg.conn_);
			}
			return*this;
		}

		connection_guard(const connection_guard&) = delete;
		connection_guard& operator=(const connection_guard&) = delete;

		connection_guard(std::unique_ptr<Conn>&& p, ConnPool& conn_pool)
			:conn_(std::move(p)), connection_pool_(conn_pool)
		{}

		~connection_guard() {
			if (conn_) {
				connection_pool_.return_back(std::move(conn_));
			}
		}

		std::unique_ptr<Conn>& operator->() {
			return conn_;
		}

		bool operator!() {
			return !conn_;
		}

		explicit operator bool() {
			return conn_ != nullptr;
		}
		
		/*void free_conn() {
			conn_.reset();
		}*/
	private:
		std::unique_ptr<Conn> conn_;
		ConnPool& connection_pool_;
	};


	template <typename Fun>
	class scope_guard {
	private:
		Fun releaser_;
	public:
		scope_guard() = default;

		scope_guard(Fun&& f) : releaser_(std::move(f))
		{}

		void set_releaser(Fun&& f) {
			releaser_ = std::move(f);
		}

		~scope_guard() {
			releaser_();
		}

	private:
		scope_guard(const scope_guard&) = delete;
		scope_guard& operator=(const scope_guard&) = delete;
	};


}