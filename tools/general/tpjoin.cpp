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
#include <sys/wait.h>
#include <unistd.h>

void usage(const char* prog) {
    fprintf(stderr, R"EOS(usage: %s
-d input-delim
   field delim of stdin

-D output-delim

-j field1,field2,...:[QuoteBeg]:[QuoteEnd]:[KFdelim]:[OFdelim]:command
   join key(field1,field2,...) to command, send "QuoteBeg$key$QuoteEnd" to command and read
   the command output, append the command output on input fields.

   KFdelim is key field delim.
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

    valvec<fstrvec> jresp;
};

struct OneJoin {
    valvec<size_t> keyfields; // key is multiple fields of input record/line
    valvec<byte_t> keybuf;
    valvec<byte_t> resp;
    const char* cmd;
    fstring quote_beg;
    fstring quote_end;
    unsigned char odelim;
    unsigned char kdelim;
    pid_t childpid;
    int rfd;
    int wfd;
    bool is_eof = false; // for pipe read
    intptr_t rqpos = 0;
    intptr_t sendqpos = 0;

    ~OneJoin() {
        if (rfd > 0) ::close(rfd);
        if (wfd > 0) ::close(wfd);
    }

    void extract_key(const OneRecord& record) {
        keybuf.erase_all();
        keybuf.append(quote_beg);
        for (size_t kf : keyfields) {
            if (kf-1 >= record.size()) {
                fprintf(stderr, "ERROR: input fields=%zd is less than keyfield=%zd\n", record.size(), kf);
                exit(255);
            }
            keybuf.append(record[kf-1]);
            keybuf.back() = kdelim;
        }
        keybuf.pop_back();
        keybuf.append(quote_end);
    }

    void send_req(const OneRecord& record) {
        assert(wfd > 0);
        extract_key(record);
        intptr_t wlen = ::write(wfd, keybuf.data(), keybuf.size());
        if (intptr_t(keybuf.size()) != wlen) {
            int err = errno;
            fprintf(stderr, "ERROR: write(%zd) = %zd, cmd(%s) = %s\n",
                    keybuf.size(), wlen, cmd, strerror(err));
            exit(err);
        }
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
            intptr_t len2 = read(rfd, buf1, len1);
            if (len2 < 0) { // error
                int err = errno;
                fprintf(stderr, "ERROR: read cmd(%s) = %s\n", cmd, strerror(err));
                exit(err);
            } else if (len2 > 0) {
                resp.risk_set_size(resp.size() + len2);
                if (len2 < (intptr_t)len1) {
                    break; // have read'ed fully
                }
                assert(len2 == (intptr_t)len1); // continue read more...
            } else { // 0 == len2
                if (EAGAIN == errno) {
                    break;
                }
                if (resp.size() && '\n' != resp.back()) {
                    resp.push_back('\n'); // add missing trailing '\n'
                }
                is_eof = true;
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
            if (queue.tail_real_index() != rqpos) {
                size_t vi = queue.virtual_index(rqpos);
                auto& record = queue[vi];
                auto& jr = record.jresp.ensure_get(jidx);
                jr.strpool.assign(line, scan+1); // include '\n'
                jr.offsets.erase_all();
                fstring(jr.strpool).split_f2(odelim,
                        [&](const char* col, const char*) {
                    jr.offsets.push_back(col - jr.strpool.data());
                });
                jr.offsets.push_back(jr.strpool.size());
                //fprintf(stderr, "DEBUG: join_id=%zd: line=%zd:%.*s, jr.size()=%zd\n", jidx+1, scan-line, int(scan-line), line, jr.size());
                rqpos = queue.real_index(vi + 1);
                line = scan = scan + 1;
            } else { // response lines is more than request
                fprintf(stderr, "ERROR: cmd(%s) response lines is more than request\n", cmd);
                exit(255);
            }
        }
        //fprintf(stderr, "DEBUG: join_id=%zd: line_pos=%zd: resp.size()=%zd\n", jidx+1, line-resp.data(), resp.size());
        resp.erase_i(0, line - resp.data());
        return queue.virtual_index(rqpos);
    }
};

struct Main {

unsigned char delim = '\t';
unsigned char odelim = '\t';
valvec<OneJoin> joins;
valvec<std::pair<int, int> > fofields;
circular_queue<OneRecord> queue;
fd_set rfdset, wfdset, efdset;
int fdnum;
bool is_input_eof = false;
size_t input_fields = 0;

void read_one_line() {
    LineBuf line;
    if (line.getline(stdin) >= 0) {
        line.chomp();
        line.push_back('\n');
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
        if (j.wfd > 0 && FD_ISSET(j.wfd, &wfdset)) {
            size_t vi = queue.virtual_index(j.sendqpos);
            if (vi < queue.size()) {
                auto& record = queue[vi];
                j.send_req(record);
                j.sendqpos = queue.real_index(vi + 1);
                if (is_input_eof && queue.tail_real_index() == j.sendqpos) {
                    ::close(j.wfd); j.wfd = -1;
                }
            } else {
                // fprintf(stderr, "ERROR: ith_joinkey = %zd, sendqpos = %zd reaches queue.size()\n", i, j.sendqpos);
            }
        }
    }
}

void read_response_and_write() {
    intptr_t min_qvhead = INT_MAX;
    for (size_t i = 0; i < joins.size(); ++i) {
        auto& j = joins[i];
        int fd = j.rfd;
        if (FD_ISSET(fd, &rfdset)) {
            intptr_t cur_qvhead = j.recv_vhead(queue, i);
            min_qvhead = std::min(min_qvhead, cur_qvhead);
        }
    }
    if (INT_MAX != min_qvhead) {
        assert(min_qvhead <= (intptr_t)queue.size());
        for (intptr_t i = 0; i < min_qvhead; ++i) {
            write_row(queue.front());
            queue.pop_front();
        }
    }
}

valvec<byte_t> rowbuf;
void write_row(const OneRecord& row) {
    rowbuf.erase_all();
    for (auto fj: fofields) {
        if (0 == fj.first) { // ref 'row'
            if (0 == fj.second) { // ref all fields
                for (size_t i = 0; i < row.size(); ++i) {
                    rowbuf.append(row[i]);
                    rowbuf.back() = odelim;
                }
            }
            else {
                rowbuf.append(row[fj.second-1]);
                rowbuf.back() = odelim;
            }
        }
        else { // ref joins[]
            auto& jr = row.jresp[fj.first-1];
            if (jr.strpool.size() <= 1) {
                fprintf(stderr, "WARN: empty respond: join_id = %d, jresp[fields = %zd, bytes=%zd]\n", fj.first, jr.size(), jr.strpool.size());
            }
            if (0 == fj.second) {
                for (size_t i = 0; i < jr.size(); ++i) {
                    rowbuf.append(jr[i]);
                    rowbuf.back() = odelim;
                }
            }
            else {
                rowbuf.append(jr[fj.second]);
                rowbuf.back() = odelim;
            }
        }
    }
    rowbuf.back() = '\n';
    intptr_t len1 = rowbuf.size();
    intptr_t len2 = ::write(STDOUT_FILENO, rowbuf.data(), len1);
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
        FD_SET(j.rfd, &rfdset);  FD_SET(j.rfd, &efdset);
        if (j.wfd > 0) {
            FD_SET(j.wfd, &wfdset);
            FD_SET(j.wfd, &efdset);
        }
    }
    timeval timeout = {0, 10000}; // 10ms
    int err = select(fdnum, &rfdset, &wfdset, &efdset, &timeout);
    if (err < 0) { // error
        err = errno;
        fprintf(stderr, "ERROR: select(readwrite fdset) = %s\n", strerror(err));
        exit(err);
    }
    return err;
}

// :[content]:
// return content, if content is empty, use szDefault
fstring parse_bracket(const char*& cur, const char* szDefault) {
    if (':' != cur[0]) {
        fprintf(stderr, "ERROR: bad bracket 1 = %s\n", cur);
        exit(EINVAL);
    }
    if ('[' != cur[1]) {
        fprintf(stderr, "ERROR: bad bracket 2 = %s\n", cur);
        exit(EINVAL);
    }
    const char* closep = strchr(cur+2, ']');
    if (NULL == closep) {
        fprintf(stderr, "ERROR: bad bracket 3 = %s\n", cur);
        exit(EINVAL);
    }
    if (':' != closep[1]) {
        fprintf(stderr, "ERROR: bad bracket 4 = %s\n", cur);
        exit(EINVAL);
    }
    const char* content = cur + 2;
    cur = closep + 1; // cur point to tail ':'
    return content == closep ? szDefault : fstring(content, closep);
}

void add_join(const char* js) {
    joins.emplace_back(); OneJoin& j = joins.back();
    const char* cur = js;
    while (true) {
        char* endp = NULL;
        long fidx = strtol(cur, &endp, 10);
        if (fidx < 0) {
            fprintf(stderr, "ERROR: bad key fields = %s\n", js);
            exit(EINVAL);
        }
        if (',' == *endp) {
            j.keyfields.push_back(fidx);
        }
        else if (':' == *endp) {
            j.keyfields.push_back(fidx);
            cur = endp;
            break; // :[QuoteBeg]:[QuoteEnd]:[Odelim]:
        }
        else {
            fprintf(stderr, "ERROR: bad key fields = %s\n", js);
            exit(EINVAL);
        }
        cur = endp + 1;
    }

    // parse :[QuoteBeg]:[QuoteEnd]:[Odelim]:
    j.quote_beg = parse_bracket(cur, "");
    j.quote_end = parse_bracket(cur, "\n");
    j.kdelim = parse_bracket(cur, "\t")[0];
    j.odelim = parse_bracket(cur, "\t")[0];

    const char* cmd = cur + 1;

    int rfds[2], wfds[2];
    int err = pipe(rfds);
    if (err < 0) {
        fprintf(stderr, "ERROR: pipe() = %s\n", strerror(err));
        exit(err);
    }
    err = pipe(wfds);
    if (err < 0) {
        fprintf(stderr, "ERROR: pipe() = %s\n", strerror(err));
        exit(err);
    }
    pid_t pid = fork();
    if (pid < 0) {
        err = errno;
        fprintf(stderr, "ERROR: fork() = %s\n", strerror(err));
        exit(err);
    }
    if (0 == pid) { // child
        err = dup2(wfds[0], 0); // parent write -> child read
        if (err < 0) {
            err = errno;
            fprintf(stderr, "ERROR: dup2(%d, %d) = %s\n", rfds[0], 0, strerror(err));
            exit(err);
        }
        err = dup2(rfds[1], 1); // child write -> parent read
        if (err < 0) {
            err = errno;
            fprintf(stderr, "ERROR: dup2(%d, %d) = %s\n", wfds[1], 1, strerror(err));
            exit(err);
        }
        ::close(wfds[0]); ::close(wfds[1]);
        ::close(rfds[0]); ::close(rfds[1]);
        err = execl("/bin/sh", "/bin/sh", "-c", cmd, NULL);
        if (err) {
            err = errno;
            fprintf(stderr, "ERROR: execl(%s) = %s\n", cmd, strerror(err));
            exit(err);
        }
        return; // should not goes here...
    }
    // parent
    ::close(rfds[1]); j.rfd = rfds[0];
    ::close(wfds[0]); j.wfd = wfds[1];

    if (fcntl(j.rfd, F_SETFL, fcntl(j.rfd, F_GETFL) | O_NONBLOCK) < 0) {
        err = errno;
        fprintf(stderr, "ERROR: fcntl(%d, F_SETFD, O_NONBLOCK) = %s\n", j.rfd, strerror(err));
        exit(err);
    }
    // do not set wfd as O_NONBLOCK

    j.childpid = pid;
}

void parse_output_fileds(const char* outputFieldsSpec) {
    if (NULL == outputFieldsSpec) {
        fofields.push_back({0,0});
        for (size_t i = 0; i < joins.size(); ++i) {
            fofields.push_back({int(i+1), 0});
        }
        return;
    }
    const char* cur = outputFieldsSpec;
    while (true) {
        char* endp = NULL;
        int fidx = (int)strtol(cur, &endp, 10);
        if (fidx < 0) {
            fprintf(stderr, "ERROR: bad outputFieldsSpec = %s\n", outputFieldsSpec);
            exit(EINVAL);
        }
        if ('.' == *endp) {
            // now fidx indicate i'th join cmd
            if ('*' == endp[1]) {
                endp += 1;
                fofields.push_back({fidx, 0});
            }
            else {
                long jfidx = strtol(endp+1, &endp, 10);
                fofields.push_back({fidx, jfidx});
            }
        }
        if (',' == *endp) {
            fofields.push_back({0, fidx});
        }
        else if ('\0' == *endp) {
            break;
        }
        else {
            fprintf(stderr, "ERROR: bad outputFieldsSpec = %s\n", outputFieldsSpec);
            exit(EINVAL);
        }
        cur = endp + 1;
    }
}

int main(int argc, char* argv[]) {
    const char* outputFieldsSpec = NULL;
    for (;;) {
        int opt = getopt(argc, argv, "hd:D:j:o:q:v");
        switch (opt) {
            case -1:
                goto GetoptDone;
            case 'd':
                delim = optarg[0];
                break;
            case 'D':
                odelim = optarg[0];
                break;
            case 'j':
                add_join(optarg);
                break;
            case 'o':
                outputFieldsSpec = optarg;
                break;
            case 'q':
            {
                long cap = strtol(optarg, NULL, 10);
                if (cap < 0) {
                    fprintf(stderr, "ERROR: bad argument -q %s\n", optarg);
                    exit(EINVAL);
                }
                queue.init(cap);
                break;
            }
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
    if (queue.capacity() == 0) {
        queue.init(256);
    }
    parse_output_fileds(outputFieldsSpec);
    fdnum = 0;
    for (size_t i = 0; i < joins.size(); ++i) {
        maximize(fdnum, joins[i].rfd);
        maximize(fdnum, joins[i].wfd);
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
    auto find_child = [&](pid_t childpid) {
        for (size_t i = 0; i < joins.size(); ++i) {
            if (joins[i].childpid == childpid)
                return i;
        }
        return size_t(-1);
    };
    while (true) {
        int status = 0;
	//fprintf(stderr, "DEBUG: wait(&status)...\n");
        pid_t childpid = waitpid(-1, &status, WNOHANG);
        if (childpid < 0) {
            int err = errno;
            if (ECHILD == err) {
                break;
            }
        }
        else if (childpid > 0) {
            size_t jidx = find_child(childpid);
            if (size_t(-1) == jidx) {
                fprintf(stderr, "ERROR: unexpected waited childpid = %zd\n", size_t(childpid));
                return 255;
            }
            auto& j = joins[jidx];
            if (WEXITSTATUS(status) == 0) {
                fprintf(stderr, "INFO: %s completed successful\n", j.cmd);
            } else {
                fprintf(stderr, "ERROR: %s exit = %d : %s\n", j.cmd, status, strerror(status));
            }
        }
	else {
		break;
	}
    }
    return 0;
}

};

int main(int argc, char* argv[]) {
    return Main().main(argc, argv);
}
