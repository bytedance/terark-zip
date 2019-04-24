#ifndef __terark_logged_tab_hpp__
#define __terark_logged_tab_hpp__

#include "io/DataIO.hpp"
#include "io/FileStream.hpp"
#include "io/MemStream.hpp"
#include "io/StreamBuffer.hpp"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace terark {

struct OpCode {
	enum Op {
		add = 1,
		set,
		del,
		list_push,
		list_pop,
	};
};

template<class Tab, class Key, class Val>
class Logged {
	Tab* tab;
	std::string fmain;
	std::string flog;
	int  log_fd;

public:
	// modifications on tab must be in a Transaction
   	class Transaction; friend class Transaction;

	Logged(Tab* tab, const std::string& dir) {
		assert(NULL != tab);
		this->tab = tab;
		fmain = dir + "/main";
		flog  = dir + "/log";
		FileStream file;
		if (file.xopen(fmain.c_str(), "rb")) {
			NativeDataInput<InputBuffer> dio; dio.attach(&file);
			dio >> *tab; // load the check point
			file.close();
		}
		if (file.xopen(flog.c_str(), "rb")) {
			InputBuffer buf(&file);
			replay(buf); // load the write-ahead-log
			file.close();
		}
		log_fd = -1;
		open_write_ahead(flog.c_str());
	}
	~Logged() {
		if (log_fd > 0) {
			::close(log_fd);
		}
	}

	off_t check_point_size() const {
		struct stat st;
		if (::stat(fmain.c_str(), &st) < 0)
			return 0;
		else
			return st.st_size;
	}
	off_t log_size() const {
		struct stat st;
		if (::fstat(log_fd, &st) < 0)
			throw std::runtime_error("::fstat");
		return st.st_size;
	}

	/// Must trigger by the caller
	void make_check_point() {{
		std::string ftmp = fmain + ".tmp";
		{
			FileStream file(ftmp.c_str(), "w");
			NativeDataOutput<OutputBuffer> dio; dio.attach(&file);
			dio << *tab;
		}
		if (::remove(fmain.c_str()) < 0 && ENOENT != errno) {
			fprintf(stderr, "::remove(\"%s\") = %s\n", fmain.c_str(), strerror(errno));
			throw std::runtime_error("::remove");
		}
		if (::rename(ftmp.c_str(), fmain.c_str()) < 0) {
			fprintf(stderr, "::rename(\"%s\", \"%s\") = %s\n", ftmp.c_str(), fmain.c_str(), strerror(errno));
			throw std::runtime_error("::rename");
		}
		if (log_fd > 0) {
			::close(log_fd);
		}
		if (::remove(flog.c_str()) < 0) {
			fprintf(stderr, "::remove(\"%s\") = %s\n", flog.c_str(), strerror(errno));
			throw std::runtime_error("::remove");
		}}
		open_write_ahead(flog.c_str()); // create new log
	}

private:
	void open_write_ahead(const char* fpath) {
		if (log_fd > 0)
			::close(log_fd);
		log_fd = ::open(fpath, O_CREAT|O_RDWR, 0644);
		if (log_fd < 0) {
			fprintf(stderr, "::open(\"%s\",	O_CREAT|O_RDWR) = %s\n", fpath, strerror(errno));
			throw std::runtime_error("::open");
		}
		off_t fsize = ::lseek(log_fd, 0, SEEK_END);
		if (fsize < 0) {
			fprintf(stderr, "::lseek(\"%s\", 0, SEEK_END) = %s\n", fpath, strerror(errno));
			throw std::runtime_error("::lseek");
		}
	//	printf("::lseek(\"%s\", 0, SEEK_END) = %ld\n", fpath, (long)fsize);
	}
	template<class Buf>
	void replay(Buf& buf) {
		NativeDataInput<Buf>& dio = static_cast<NativeDataInput<Buf>&>(buf);
		Key key;
		Val val;
		while (!dio.eof()) {
			unsigned char op;
			dio >> op;
			switch (op) {
			default:
				throw std::runtime_error("");
				break;
			case OpCode::add:
				dio >> key;
				dio >> val;
				// ignore when existed
				tab->insert(std::make_pair(key, val));
				break;
			case OpCode::set:
				dio >> key;
				dio >> val;
			   	{
					std::pair<typename Tab::iterator, bool> ib
						= tab->insert(std::make_pair(key, val));
					if (!ib.second) // exists
						ib.first->second = val;
				}
				break;
			case OpCode::del:
				dio >> key;
				tab->erase(key);
				break;
			case OpCode::list_push: // TODO:
				assert(0);
				break;
			case OpCode::list_pop: // TODO:
				assert(0);
				break;
			}
		}
	}
};

template<class Tab, class Key, class Val>
class Logged<Tab, Key, Val>::Transaction {
	NativeDataOutput<AutoGrownMemIO> buf;
	Logged* log;
public:
	explicit Transaction(Logged* log, size_t initial_bufsize = 1024) {
		assert(NULL != log);
		this->log = log;
		buf.resize(initial_bufsize);
		assert(buf.tell() == 0);
	}
	~Transaction() {
		if (buf.tell() != 0) {
			fprintf(stderr, "Transaction was not committed nor abortted\n");
		}
	}
	void commit() {
		assert(buf.tell() != 0);
		ssize_t n_write = buf.tell();
		ssize_t written = ::write(log->log_fd, buf.begin(), n_write);
		if (n_write != written) {
			fprintf(stderr, "::write(\"%s\", %zd, %zd) = %s\n"
				, log->flog.c_str(), n_write, written, strerror(errno));
			if (written > 0) {
				off_t old_pos = ::lseek(log->log_fd, -written, SEEK_END);
				if (old_pos < 0) {
					throw std::runtime_error("::lseek");
				}
				::ftruncate(log->log_fd, old_pos - written);
			}
			throw std::runtime_error("::write");
		}
		MemIO mem = buf.head();
		log->replay(mem);
		assert(mem.eof());
		buf.rewind();
	}
	void abort() {
		buf.rewind();
	}
	void add(const Key& key, const Val& val) {
		buf << (unsigned char)(OpCode::add);
		buf << key;
		buf << val;
	}
	void set(const Key& key, const Val& val) {
		buf << (unsigned char)(OpCode::set);
		buf << key;
		buf << val;
	}
	void del(const Key& key) {
		buf << (unsigned char)(OpCode::del);
		buf << key;
	}
};

} // namespace terark

#endif // __terark_logged_tab_hpp__

