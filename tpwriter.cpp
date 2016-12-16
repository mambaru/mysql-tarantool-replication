#include <iostream>
#include <sstream>
#include <sys/time.h>

#include <tarantool/tarantool.h>
#include <tarantool/tnt_net.h>
#include <tarantool/tnt_opt.h>
#include <msgpuck.h>

#include <boost/any.hpp>

#include "tpwriter.h"
#include "serializable.h"

namespace replicator {

const std::string TPWriter::empty_call("");

TPWriter::TPWriter(
	const std::string &host,
	const std::string &user,
	const std::string &password,
	uint32_t binlog_key_space,
	uint32_t binlog_key,
	unsigned connect_retry,
	unsigned sync_retry
) :
	host(host),
	user(user),
	password(password),
	binlog_key_space(binlog_key_space),
	binlog_key(binlog_key),
	binlog_name(""),
	binlog_pos(0),
	connect_retry(connect_retry),
	sync_retry(sync_retry),
	next_connect_attempt(0),
	next_sync_attempt(0),
	next_ping_attempt(0)
{
	::tnt_net(&sess);

	::tnt_set(&sess, ::TNT_OPT_URI, host.c_str());
	::tnt_set(&sess, ::TNT_OPT_SEND_BUF, 0);
	::tnt_set(&sess, ::TNT_OPT_RECV_BUF, 16 * 1024 * 1024);

	::timeval tmout;
	auto make_timeout = [&tmout] (const unsigned t) -> ::timeval* {
		tmout.tv_sec  =  t / 1000;
		tmout.tv_usec = (t % 1000) * 1000;
		return &tmout;
	};

	::tnt_set(&sess, ::TNT_OPT_TMOUT_RECV, make_timeout(100));
	::tnt_set(&sess, ::TNT_OPT_TMOUT_SEND, make_timeout(10000));
}

bool TPWriter::Connect()
{
	// connect to tarantool
	if (::time(NULL) < next_connect_attempt) {
		::sleep(next_connect_attempt - ::time(NULL));
	}
	if (::tnt_connect(&sess) < 0) {
		std::cerr << "Could not connect to Tarantool: " << ::tnt_strerror(&sess) << std::endl;
		::tnt_close(&sess);
		next_connect_attempt = ::time(NULL) + connect_retry;
		return false;
	}

	std::cout << "Connected to Tarantool at " << host << std::endl;
	next_sync_attempt = 0;

	return true;
}

TPWriter::~TPWriter()
{
	::tnt_stream_free(&sess);
}

void TPWriter::ReadBinlogPos(std::string &binlog_name, unsigned long &binlog_pos)
{
	// read initial binlog pos
	int64_t sync;
	{
		__tnt_object key;
		::tnt_object_add_array(&key, 1);
		::tnt_object_add_uint(&key, binlog_key);
		::tnt_object_container_close(&key);

		__tnt_request req;
		::tnt_request_select(&req);
		::tnt_request_set_space(&req, binlog_key_space);
		::tnt_request_set_limit(&req, 1);
		::tnt_request_set_key(&req, &key);

		sync = Send(&req);
	}

	__tnt_reply re;
	while (!Recv(&re));

	if ((&re)->sync != sync) {
		throw std::runtime_error("ReadBinlogPos error: not requested reply");
	}
	do {
		this->binlog_name = binlog_name = "";
		this->binlog_pos = binlog_pos = 0;

		const char *data = (&re)->data;
		// result rows
		if (mp_unlikely(mp_typeof(*data) != MP_ARRAY)) break;
		if (mp_unlikely(mp_decode_array(&data) == 0)) {
			// no binlog created yet
			return;
		}

		// row
		if (mp_unlikely(mp_typeof(*data) != MP_ARRAY)) break;
		if (mp_unlikely(mp_decode_array(&data) != 3)) break;

		// binlog_key
		if (mp_unlikely(mp_typeof(*data) != MP_UINT)) break;
		if (mp_unlikely(mp_decode_uint(&data) != binlog_key)) break;;

		if (mp_unlikely(mp_typeof(*data) != MP_STR)) break;
		uint32_t _binlog_name_len;
		const char *_binlog_name = mp_decode_str(&data, &_binlog_name_len);

		if (mp_unlikely(mp_typeof(*data) != MP_UINT)) break;
		uint64_t _binlog_pos = mp_decode_uint(&data);

		this->binlog_name = binlog_name = std::string(_binlog_name, _binlog_name_len);
		this->binlog_pos = binlog_pos = _binlog_pos;

		next_ping_attempt = Milliseconds() + TPWriter::PING_TIMEOUT;

		return;

	} while (0);

	std::cerr << "ReadBinlogPos error: bad state record format" << std::endl;
}

void TPWriter::Disconnect()
{
	::tnt_close(&sess);
}

void TPWriter::AddTable(
	const std::string& db,
	const std::string& table,
	const unsigned space,
	const Tuple &keys,
	const std::string& insert_call,
	const std::string& update_call,
	const std::string& delete_call
) {
	TableSpace& s = dbs[db][table];
	s.space = space;
	s.keys = keys;
	s.insert_call = insert_call;
	s.update_call = update_call;
	s.delete_call = delete_call;
}

void TPWriter::BinlogEventCallback(const SerializableBinlogEvent& ev)
{
	// spacial case event "IGNORE", which only updates binlog position
	// but doesn't modify any table data
	if (ev.event == "IGNORE") {
		return;
	}

	const TableSpace& ts = dbs.at(ev.database).at(ev.table);

	// check if doing same key as last time
	static std::string prev_key;
	std::string curnt_key;

	for (const auto i : ts.keys) {
		curnt_key += ev.row.at(i).to_string();
	}
	if (prev_key == curnt_key) {
		// sync first
		RecvAll();
	} else {
		prev_key = curnt_key;
	}

	const std::map<unsigned, SerializableValue>* replace_null_;
	const auto irn = replace_null.find(ts.space);

	if (irn != replace_null.end()) {
		replace_null_ = &irn->second;
	} else {
		replace_null_ = nullptr;
	}

	const auto add_nil_with_replace = [&] (struct ::tnt_stream *o, const unsigned index) -> void {
		do {
			if (replace_null_ == nullptr) break;
			const auto irnv = replace_null_->find(index);
			if (irnv == irn->second.end()) break;

			const SerializableValue& v = irnv->second;
			if (v.is<std::string>()) {
				const std::string s = v.as<std::string>();
				::tnt_object_add_str(o, s.c_str(), s.length());
			} else if (v.is<long long>()) {
				::tnt_object_add_int(o, v.as<long long>());
			} else if (v.is<unsigned long long>()) {
				::tnt_object_add_uint(o, v.as<unsigned long long>());
			} else {
				std::ostringstream s;
				s << "Typecasting error for non-null value for column: " << index;
				throw std::range_error(s.str());
			}
			return;

		} while (false);

		::tnt_object_add_nil(o);
	};

	const auto add_value = [&] (struct ::tnt_stream *o, const unsigned index, const SerializableValue &v) -> void {
		try {
			if (v.is<std::string>()) {
				const std::string s = v.as<std::string>();
				::tnt_object_add_str(o, s.c_str(), s.length());
			} else if (v.is<char>()) {
				::tnt_object_add_int(o, v.as<char>());
			} else if (v.is<unsigned char>()) {
				::tnt_object_add_uint(o, v.as<unsigned char>());
			} else if (v.is<short>()) {
				::tnt_object_add_int(o, v.as<short>());
			} else if (v.is<unsigned short>()) {
				::tnt_object_add_uint(o, v.as<unsigned short>());
			} else if (v.is<int>()) {
				::tnt_object_add_int(o, v.as<int>());
			} else if (v.is<unsigned int>()) {
				::tnt_object_add_uint(o, v.as<unsigned int>());
			} else if (v.is<long>()) {
				::tnt_object_add_int(o, v.as<long>());
			} else if (v.is<unsigned long>()) {
				::tnt_object_add_uint(o, v.as<unsigned long>());
			} else if (v.is<long long>()) {
				::tnt_object_add_int(o, v.as<long long>());
			} else if (v.is<unsigned long long>()) {
				::tnt_object_add_uint(o, v.as<unsigned long long>());
			} else if (v.is<float>()) {
				::tnt_object_add_float(o, v.as<float>());
			} else if (v.is<double>()) {
				::tnt_object_add_double(o, v.as<double>());
			} else {
				add_nil_with_replace(o, index);
			}
		}
		catch (boost::bad_any_cast &ex) {
			std::ostringstream s;
			s << "Typecasting error for column: " << ex.what();
			throw std::range_error(s.str());
		}
	};

	const auto add_key = [&] (struct ::tnt_stream *o) -> void {
		::tnt_object_add_array(o, ts.keys.size());
		for (const auto i : ts.keys) {
			add_value(o, i, ev.row.at(i));
		}
		::tnt_object_container_close(o);
	};

	const auto add_tuple = [&] (struct ::tnt_stream *o) -> void {
		::tnt_object_add_array(o, space_last_id[ts.space] + 1);
		unsigned i_nil = 0;
		// ev.row may have gaps, since it's not an array but a map
		// so fill the gaps to match columns count
		for (auto it = ev.row.begin(), end = ev.row.end(); it != end; ++it) {
			// fill gaps
			for (; i_nil < it->first; ++i_nil) add_nil_with_replace(o, i_nil);

			add_value(o, it->first, it->second);
			i_nil = it->first + 1;
		}
		// fill gaps
		for (; i_nil <= space_last_id[ts.space]; ++i_nil) add_nil_with_replace(o, i_nil);

		::tnt_object_container_close(o);
	};

	const auto add_ops = [&] (struct ::tnt_stream *o) -> void {
		::tnt_update_container_reset(o);
		for (auto it = ev.row.begin(), end = ev.row.end(); it != end; ++it) {
			__tnt_object sval;
			add_value(&sval, it->first, it->second);
			::tnt_update_assign(o, it->first, &sval);
		}
		::tnt_update_container_close(o);
	};

	const auto make_call = [&] (const std::string& func_name) -> void {
		__tnt_object args;
		::tnt_object_add_array(&args, 1);
		::tnt_object_add_map(&args, ev.row.size());

		for (auto it = ev.row.begin(), end = ev.row.end(); it != end; ++it) {
			::tnt_object_add_uint(&args, it->first);
			add_value(&args, it->first, it->second);
		}
		::tnt_object_container_close(&args);
		::tnt_object_container_close(&args);

		__tnt_request req;
		::tnt_request_call(&req);
		::tnt_request_set_tuple(&req, &args);
		::tnt_request_set_func(&req, func_name.c_str(), func_name.length());

		Send(&req);
	};

	// add Tarantool request
	if (ev.event == "DELETE") {
		if (ts.delete_call.empty()) {
			__tnt_object key;
			add_key(&key);

			__tnt_request req;
			::tnt_request_delete(&req);
			::tnt_request_set_space(&req, ts.space);
			::tnt_request_set_key(&req, &key);

			Send(&req);
		} else {
			make_call(ts.delete_call);
		}
	} else if (ev.event == "INSERT") {
		if (ts.insert_call.empty()) {
			// __tnt_object tuple;
			// add_tuple(&tuple);

			// __tnt_request req;
			// ::tnt_request_replace(&req);
			// ::tnt_request_set_space(&req, ts.space);
			// ::tnt_request_set_tuple(&req, &tuple);

			__tnt_object tuple;
			add_tuple(&tuple);

			__tnt_object ops;
			add_ops(&ops);

			__tnt_request req;
			::tnt_request_upsert(&req);
			::tnt_request_set_space(&req, ts.space);
			::tnt_request_set_tuple(&req, &tuple);
			::tnt_request_set_ops(&req, &ops);

			Send(&req);
		} else {
			make_call(ts.insert_call);
		}
	} else if (ev.event == "UPDATE") {
		if (ts.update_call.empty()) {
			__tnt_object key;
			add_key(&key);

			__tnt_object ops;
			add_ops(&ops);

			__tnt_request req;
			::tnt_request_update(&req);
			::tnt_request_set_space(&req, ts.space);
			::tnt_request_set_key(&req, &key);
			::tnt_request_set_ops(&req, &ops);

			Send(&req);
		} else {
			make_call(ts.update_call);
		}
	} else {
		std::ostringstream s;
		s << "Uknown binlog event: " << ev.event;
		throw std::range_error(s.str());
	}

	if (ev.binlog_name != "") {
		binlog_name = ev.binlog_name;
		binlog_pos = ev.binlog_pos;
	}
}

// blocking send
int64_t TPWriter::Send(struct ::tnt_request *req)
{
	struct ::tnt_stream sbuf;
	::tnt_buf(&sbuf);
	const int64_t sync = ::tnt_request_compile(&sbuf, req);

	size_t len = TNT_SBUF_SIZE(&sbuf);
	char *buf = TNT_SBUF_DATA(&sbuf);

	while (len > 0) {
		const ssize_t sent = sess.write(&sess, buf, len);
		if (sent < 0) {
			const int _errno = ::tnt_errno(&sess);
			if (_errno == EWOULDBLOCK || _errno == EAGAIN) {
				continue;
			}
			::tnt_stream_free(&sbuf);
			std::ostringstream s;
			s << "Send failed: " << ::tnt_strerror(&sess);
			throw std::runtime_error(s.str());
		}
		len -= sent;
		buf += sent;
	}

	::tnt_stream_free(&sbuf);
	return sync;
}

void TPWriter::Sync()
{
	bool force = false;

	if (Milliseconds() > next_ping_attempt) {
		force = true;

		__tnt_request req;
		::tnt_request_ping(&req);
		Send(&req);

		next_ping_attempt = Milliseconds() + TPWriter::PING_TIMEOUT;
	}
	if (force || Milliseconds() >= next_sync_attempt) {
		__tnt_object tuple;
		::tnt_object_add_array(&tuple, 3);
		::tnt_object_add_uint(&tuple, binlog_key);
		::tnt_object_add_str(&tuple, binlog_name.c_str(), binlog_name.length());
		::tnt_object_add_uint(&tuple, binlog_pos);
		::tnt_object_container_close(&tuple);

		__tnt_request req;
		::tnt_request_replace(&req);
		::tnt_request_set_space(&req, binlog_key_space);
		::tnt_request_set_tuple(&req, &tuple);

		Send(&req);

		next_sync_attempt = Milliseconds() + sync_retry;
	}
}

// non-blocking receive
bool TPWriter::Recv(struct ::tnt_reply *re)
{
	const int result = sess.read_reply(&sess, re);

	if (result == 0) {
		if (re->code) {
			std::ostringstream s;
			s << "Tarantool error: " << std::string(re->error, re->error_end - re->error) << " (code: " << re->code << ")";
			throw std::range_error(s.str());
		}
		return true;
	}
	else if (result < 0) {
		const int _errno = ::tnt_errno(&sess);
		if (_errno == EWOULDBLOCK || _errno == EAGAIN) {
			return false;
		}
		std::ostringstream s;
		s << "Recv failed: " << ::tnt_strerror(&sess);
		throw std::runtime_error(s.str());
	}
	// no complete replies in buffer
	return false;
}

void TPWriter::RecvAll()
{
	bool result;
	do {
		__tnt_reply re;
		result = Recv(&re);
	}
	while (result);
}

}
