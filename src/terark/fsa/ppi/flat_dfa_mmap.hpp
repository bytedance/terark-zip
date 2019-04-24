
void finish_load_mmap(const DFA_MmapHeader* base) override {
	assert(sizeof(State) == base->state_size);
	byte_t* bbase = (byte_t*)base;
	if (base->total_states >= size_t(-1)) {
		THROW_STD(out_of_range, "total_states=%lld", (long long)base->total_states);
	}
	states.clear();
	states.risk_set_data((State*)(bbase + base->blocks[0].offset));
	states.risk_set_size(size_t(base->total_states));
	states.risk_set_capacity(size_t(base->total_states));
	m_gnode_states = size_t(base->gnode_states);
	m_zpath_states = size_t(base->zpath_states);
	this->set_trans_num(size_t(base->transition_num));
}

long prepare_save_mmap(DFA_MmapHeader* base, const void** dataPtrs)
const override {
	base->state_size   = sizeof(State);
	base->transition_num = total_transitions();
	base->num_blocks   = 1;
	base->blocks[0].offset = sizeof(DFA_MmapHeader);
	base->blocks[0].length = sizeof(State)*states.size();
	dataPtrs[0] = states.data();
	return 0;
}
