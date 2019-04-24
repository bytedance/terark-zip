void finish_load_mmap(const DFA_MmapHeader* base) override {
	super::finish_load_mmap(base);
	this->is_compiled = true;
	this->n_words = size_t(base->dawg_num_words);
	this->m_is_dag = true;
}

long prepare_save_mmap(DFA_MmapHeader* base, const void** dataPtrs)
const override {
	super::prepare_save_mmap(base, dataPtrs);
	base->dawg_num_words = this->n_words;
	base->is_dag = true;
	return 0;
}

