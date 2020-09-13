public:
template<class OP>
void for_each_move(state_id_t root, OP* op) const {
	this->template for_each_move<OP&>(root, *op);
}

template<class OP>
void for_each_dest(state_id_t root, OP* op) const {
	this->template for_each_dest<OP&>(root, *op);
}
template<class OP>
void for_each_dest_rev(state_id_t root, OP* op) const {
	this->template for_each_dest_rev<OP&>(root, *op);
}

void v_for_each_move(size_t parent, const OnMove& op) const override {
	this->for_each_move(parent, &op);
}
void v_for_each_dest(size_t parent, const OnDest& op) const override {
	this->for_each_dest(parent, &op);
}
void v_for_each_dest_rev(size_t parent, const OnDest& op) const override {
	this->for_each_dest_rev(parent, &op);
}

/////////////////////////////////////////////////////////////////
// virtuals:

public:
bool v_is_pzip(size_t s) const override final { return this->is_pzip(s); }
bool v_is_term(size_t s) const override final { return this->is_term(s); }
size_t  v_total_states() const override final { return this->total_states(); }

fstring v_get_zpath_data(size_t s, MatchContext* ctx) const override final {
	return this->get_zpath_data(s, ctx);
}
size_t v_nil_state() const override final { return nil_state; }
size_t v_max_state() const override final { return max_state; }

size_t v_state_move(size_t curr, auchar_t ch) const override final
{ return state_move(curr, ch); }

using BaseDFA::get_all_move;
size_t get_all_move(size_t s, CharTarget<size_t>* moves)
const override final {
	assert(NULL != moves);
	size_t idx = 0;
	this->for_each_move(s,
		[moves,&idx](size_t t, auchar_t c) {
			moves[idx++] = {c, t};
		});
	return idx;
}
using BaseDFA::get_all_dest;
size_t get_all_dest(size_t s, size_t* dests)
const override final {
	assert(NULL != dests);
	size_t idx = 0;
	this->for_each_dest(s,
		[dests,&idx](size_t t) {
			dests[idx++] = t;
		});
	return idx;
}

void check_is_dag(const char* func) const {
	if (!this->m_is_dag) {
		std::string msg;
		msg += func;
		msg += " : DFA is not a DAG";
		throw std::logic_error(msg);
	}
}


