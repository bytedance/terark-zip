//
// Created by leipeng on 2019-07-04.
//

// Terark pipeline join
#include <terark/util/sortable_strvec.hpp>
#include <terark/util/fstrvec.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/circular_queue.hpp>
#include <fcntl.h>
#include <getopt.h>

#include <sys/select.h>

/* According to earlier standards */
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

void usage(const char* prog) {
    fprintf(stderr, R"EOS(usage: %s

-j field1,field2,...:[QuoteBeg]:[QuoteEnd]:[OFdelim]:command
   join key(field1,field2,...) to command, send "QuoteBeg$key$QuoteEnd" to command and read
   the command output, append the command output on input fields.

   the command output fields delim is OFdelim.

   for example: for redis, QuoteBeg is 'GET ', QuoteEnd is '\r\n'
   default QuoteBeg is empty
   default QuoteEnd is '\r\n'

-o field1,field2,...
   output such fields, default output all input fields and joined field
   joined fields are appended on input fields vector

)EOS", prog);
}

using namespace terark;

struct OneRecord : fstrvec {
    OneRecord() : fstrvec(valvec_no_init()) {}

    valvec<valvec<byte_t> > jresp;
    bool is_completed = false;
};

struct OneJoin {
    valvec<size_t> keyfields; // key is multiple fields of input record/line
    valvec<byte_t> keybuf;
    valvec<byte_t> resp;
    std::string cmd;
    int pipe;
    bool is_eof = false; // for pipe read
    intptr_t rqpos;
    intptr_t sendqpos;

    void decode_key(const OneRecord& record, unsigned char delim) {
        keybuf.erase_all();
        for (size_t kf : keyfields) {
            if (kf >= record.size()) {
                fprintf(stderr, "ERROR: input fields=%zd is less than keyfield=%zd\n", record.size(), kf);
                exit(255);
            }
            keybuf.append(record[kf]);
            keybuf.append(delim);
        }
        keybuf.back() = '\0';
        keybuf.pop_back();
    }

    void send_req(const OneRecord& record, unsigned char delim) {
        decode_key(record, delim);
        ::write(pipe, keybuf.data(), keybuf.size());
    }

    void read_fully() {
        const size_t MAX_RESPONSE_BUF = 64 * 1024 * 1024;
        while (!is_eof && resp.size() < MAX_RESPONSE_BUF) {
            size_t len1 = resp.free_mem_size();
            if (len1 < 4096) {
                resp.grow_capacity(4096);
                len1 = resp.free_mem_size();
            }
            byte_t* buf1 = resp.end();
            intptr_t len2 = read(pipe, buf1, len1);
            if (len2 < 0) { // error
                int err = errno;
                fprintf(stderr, "ERROR: read cmd(%s) = %s\n", cmd.c_str(), strerror(err));
                exit(err);
            } else if (0 == len2) {
                if (EAGAIN == errno) {
                    break;
                }
                is_eof = true;
            } else {
                resp.risk_set_size(resp.size() + len2);
            }
        }
    }

    int recv_vhead(circular_queue<OneRecord>& queue, size_t jidx) {
        size_t  oldsize = resp.size();
        read_fully();
        byte_t* endp = resp.end();
        byte_t* line = resp.data();
        byte_t* scan = resp.data() + oldsize;
        while (NULL != (scan = (byte_t*)memchr(scan, '\n', endp - scan))) {
            size_t vi = queue.virtual_index(rqpos);
            if (vi < queue.size()) {
                auto& record = queue[vi];
                record.jresp[jidx].assign(line, scan);
                rqpos = queue.real_index(vi + 1);
                line = scan = scan + 1;
            } else { // response lines is more than request
                fprintf(stderr, "ERROR: cmd(%s) response lines is more than request\n", cmd.c_str());
                exit(255);
            }
        }
        resp.erase_i(0, line - resp.data());
        return queue.virtual_index(rqpos);
    }
};

struct Main {

unsigned char delim = '\t';
valvec<OneJoin> joins;
valvec<size_t> outputFields;
circular_queue<OneRecord> queue;
fd_set rfdset, wfdset, efdset;
int fdnum;
bool is_input_eof = false;
size_t input_fields = 0;

void read_one_line() {
    LineBuf line;
    if (line.getline(stdin) >= 0) {
        line.chomp();
        queue.push_back(OneRecord());
        auto& record = queue.back();
        record.offsets.reserve(input_fields + 1);
        line.split_f(delim, [&](char* col, char* endc) {
            record.offsets.push_back(col - line.p);
        });
        record.offsets.push_back(line.size());
        line.risk_swap_valvec(record.strpool);
    } else {
        is_input_eof = true;
    }
}

void send_req() {
    for (size_t i = 0; i < joins.size(); i++) {
        auto& j = joins[i];
        if (FD_ISSET(j.pipe, &wfdset)) {
            size_t vi = queue.virtual_index(j.sendqpos);
            if (vi < queue.size()) {
                auto& record = queue[vi];
                j.send_req(record, delim);
                j.sendqpos = queue.real_index(vi + 1);
            } else {
                fprintf(stderr, "ERROR: ith_joinkey = %zd, sendqpos = %zd reaches queue.size()\n", i, j.sendqpos);
            }
        }
    }
}

void read_response_and_write() {
    intptr_t min_qvhead = queue.size();
    for (size_t i = 0; i < joins.size(); ++i) {
        auto& j = joins[i];
        int fd = j.pipe;
        if (FD_ISSET(fd, &rfdset)) {
            intptr_t cur_qvhead = j.recv_vhead(queue, i);
            min_qvhead = std::min(min_qvhead, cur_qvhead);
        }
    }
    for (intptr_t i = 0; i < min_qvhead; ++i) {
        write_row(queue.front());
        queue.pop_front();
    }
}

void write_row(const OneRecord& row) {
    intptr_t len1 = row.strpool.size();
    intptr_t len2 = ::write(STDOUT_FILENO, row.strpool.data(), len1);
    if (len2 != len1) {
        int err = errno;
        fprintf(stderr, "ERROR: write(stdout, %zd) = %zd: %s\n",
                len1, len2, strerror(err));
        exit(err);
    }
}

int my_select_rw() {
    FD_ZERO(&rfdset);
    FD_ZERO(&wfdset);
    FD_ZERO(&efdset);
    for (auto& j : joins) {
        FD_SET(j.pipe, &rfdset);
        FD_SET(j.pipe, &wfdset);
        FD_SET(j.pipe, &efdset);
    }
    timeval timeout = {0, 10000}; // 10ms
    int err = select(fdnum, &rfdset, NULL, &efdset, &timeout);
    if (err < 0) { // error
        err = errno;
        fprintf(stderr, "ERROR: select(readwrite fdset) = %s\n", strerror(err));
        exit(err);
    }
    return err;
}

int main(int argc, char* argv[]) {
    for (;;) {
        int opt = getopt(argc, argv, "hd:j:o:");
        switch (opt) {
            case -1:
                goto GetoptDone;
            case 'd':

                delim = optarg[0];
                break;
            case 'j':
                break;
            case 'o':
                break;
            case 'v':
                //	verbose = true;
                break;
            case '?':
            case 'h':
            default:
                usage(argv[0]);
        }
    }
GetoptDone:
    fdnum = 0;
    for (size_t i = 0; i < joins.size(); ++i) {
        fdnum = std::max(fdnum, joins[i].pipe);
    }
    fdnum += 1;
    while (!(is_input_eof && queue.empty())) {
        if (!queue.full() && !is_input_eof) {
            read_one_line();
        }
        while (my_select_rw() == 0) {
            if (queue.full()) {
                fprintf(stderr, "WARN: queue.size() = %zd is full, waiting response...\n", queue.size());
            } else if (!is_input_eof) {
                read_one_line();
            }
        }
        send_req();
        read_response_and_write();
    }
    return 0;
}

};

int main(int argc, char* argv[]) {
    return Main().main(argc, argv);
}
